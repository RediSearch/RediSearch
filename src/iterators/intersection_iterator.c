/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "intersection_iterator.h"
#include "iterators_rs.h"

/**
 * Reduce the intersection iterator by applying these rules:
 * 1. Remove all wildcard iterators since they would not contribute to the intersection
 *    (return the last one if all are wildcards).
 * 2. If any child is NULL or EMPTY_ITERATOR, free the rest and return it.
 * 3. If there is only one left child iterator, return it.
 * 4. Otherwise, return NULL and update *num — caller should build a real intersection.
 *
 * When non-NULL is returned, `its` has already been freed.
 * When NULL is returned, `its` is still valid and owned by the caller.
 */
QueryIterator *IntersectionIteratorReducer(QueryIterator **its, size_t *num) {
  QueryIterator *ret = NULL;

  // Remove all wildcard iterators from the array
  size_t current_size = *num;
  size_t write_idx = 0;
  bool all_wildcards = true;
  for (size_t read_idx = 0; read_idx < current_size; read_idx++) {
    if (IsWildcardIterator(its[read_idx])) {
      if (!all_wildcards || all_wildcards && read_idx != current_size - 1) {
        // remove all the wildcards in case there are other non-wildcard iterators
        // avoid removing it in case it's the last one and all are wildcards
        its[read_idx]->Free(its[read_idx]);
      }
    } else {
      all_wildcards = false;
      its[write_idx++] = its[read_idx];
    }
  }
  *num = write_idx;

  // Check for empty iterators
  for (size_t ii = 0; ii < write_idx; ++ii) {
    if (!its[ii] || its[ii]->type == EMPTY_ITERATOR) {
      ret = its[ii] ? its[ii] : NewEmptyIterator();
      its[ii] = NULL; // Mark as taken
      break;
    }
  }

  if (ret) {
    // Free all non-NULL iterators
    for (size_t ii = 0; ii < write_idx; ++ii) {
      if (its[ii]) {
        its[ii]->Free(its[ii]);
      }
    }
  } else {
    // Handle edge cases after wildcard removal
    if (current_size == 0) {
      // No iterators were provided, return an empty iterator
      ret = NewEmptyIterator();
    } else if (write_idx == 0) {
      // All iterators were wildcards, return the last one which was not Freed
      ret = its[current_size - 1];
    } else if (write_idx == 1) {
      // Only one iterator left, return it directly
      ret = its[0];
    }
  }

  if (ret != NULL) {
    rm_free(its);
  }

  return ret;
}
