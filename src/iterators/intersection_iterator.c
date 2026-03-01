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
#include "union_iterator.h"
#include "index_result.h"
#include "wildcard_iterator.h"

/**************************** Read + SkipTo Helpers ****************************/

static inline bool II_currentIsRelevant(IntersectionIterator *it) {
  return IndexResult_IsWithinRange(it->base.current, it->max_slop, it->in_order);
}

static inline IteratorStatus II_ReadFromFirstChild(IntersectionIterator *it, t_docId *out) {
  // Read from the first child iterator, which is guaranteed to be non-NULL
  RS_ASSERT(it->num_its > 0 && it->its[0] != NULL);
  QueryIterator *child = it->its[0];
  IteratorStatus rc = child->Read(child);
  if (rc == ITERATOR_OK) {
    *out = child->lastDocId; // If we read successfully, we return the docId
  } else if (rc == ITERATOR_EOF) {
    it->base.atEOF = true; // If the first child is at EOF, the intersection is also at EOF
  }
  return rc;
}

/**
 * Check if all iterators agree on the current docId `curTarget` holds.
 * If they do, aggregate their results into `current` and return ITERATOR_OK.
 * If any of the iterators is at EOF, set `atEOF` to true and return ITERATOR_EOF.
 * If any of the iterators is not at the requested docId, advance it to the requested `docId` and
 * return ITERATOR_NOTFOUND. The caller may retry calling this function in that case.
 * In case of an error, return the error code.
 */
static IteratorStatus II_AgreeOnDocId(IntersectionIterator *it, t_docId *curTarget) {
  const t_docId docId = *curTarget;

  for (uint32_t i = 0; i < it->num_its; i++) {
    RS_ASSERT(it->its[i]->lastDocId <= docId);
    if (it->its[i]->lastDocId < docId) {
      // Advance the iterator to the requested docId
      IteratorStatus rc = it->its[i]->SkipTo(it->its[i], docId);
      if (rc != ITERATOR_OK) { // Not OK, we break out of the loop
        if (rc == ITERATOR_EOF) {
          // Some child iterator reached EOF, so the intersection is also at EOF
          it->base.atEOF = true;
        } else if (rc == ITERATOR_NOTFOUND) {
          // The child iterator did not find the requested docId, so we need to advance the lastDocId
          // to the next possible value (the result that the child iterator yielded)
          *curTarget = it->its[i]->lastDocId;
        }
        return rc;
      }
    }
  }
  // All iterators agree on the docId, so we can set the current result
  IndexResult_ResetAggregate(it->base.current);
  for (uint32_t i = 0; i < it->num_its; i++) {
    RS_ASSERT(docId == it->its[i]->current->docId);
    AggregateResult_AddChild(it->base.current, it->its[i]->current);
  }
  return ITERATOR_OK;
}

static inline IteratorStatus II_Find_Consensus_WithRelevancyCheck(IntersectionIterator *it, t_docId docId) {
  IteratorStatus rc;
  do { // retry until we agree on the docId
    rc = II_AgreeOnDocId(it, &docId);
    if (rc != ITERATOR_OK) {
      continue;
    }
    if (!II_currentIsRelevant(it)) {
      rc = II_ReadFromFirstChild(it, &docId);
      continue;
    }
    // Hit!
    it->base.lastDocId = it->base.current->docId;
    return ITERATOR_OK;

  } while (rc == ITERATOR_OK || rc == ITERATOR_NOTFOUND);
  return rc;
}

static inline IteratorStatus II_Find_Consensus(IntersectionIterator *it, t_docId docId) {
  IteratorStatus rc;
  do { // retry until we agree on the docId
    rc = II_AgreeOnDocId(it, &docId);
  } while (rc == ITERATOR_NOTFOUND);
  if (rc == ITERATOR_OK) {
    it->base.lastDocId = it->base.current->docId;
  }
  return rc;
}

/*********************** Intersection Iterator API Implementation ***********************/

static IteratorStatus II_Read_CheckRelevancy(QueryIterator *base) {
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  t_docId target;
  IteratorStatus rc = II_ReadFromFirstChild(it, &target);
  if (rc == ITERATOR_OK) {
    rc = II_Find_Consensus_WithRelevancyCheck(it, target);
  }
  return rc;
}

static IteratorStatus II_Read(QueryIterator *base) {
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  t_docId target;
  IteratorStatus rc = II_ReadFromFirstChild(it, &target);
  if (rc == ITERATOR_OK) {
    rc = II_Find_Consensus(it, target);
  }
  return rc;
}

static IteratorStatus II_SkipTo_CheckRelevancy(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }

  IteratorStatus rc = II_AgreeOnDocId(it, &docId);
  if (ITERATOR_OK == rc) {
    if (II_currentIsRelevant(it)) {
      it->base.lastDocId = it->base.current->docId;
      return ITERATOR_OK;
    }
    // Agreed on docId, but not relevant - need to read the next valid result.
    rc = II_ReadFromFirstChild(it, &docId);
    if (rc != ITERATOR_OK) return rc; // If we failed to read, bubble up the error
  } else if (ITERATOR_NOTFOUND != rc) {
    return rc; // Unexpected - bubble up
  }

  // Not found - but we need to read the next valid result.
  // `docId` is set to the next valid docId (by `II_AgreeOnDocId` or `II_ReadFromFirstChild`).
  rc = II_Find_Consensus_WithRelevancyCheck(it, docId);
  // Return rc, switching OK to NOTFOUND
  if (rc == ITERATOR_OK) {
    return ITERATOR_NOTFOUND;
  }
  return rc; // Unexpected - bubble up
}

static IteratorStatus II_SkipTo(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }

  IteratorStatus rc = II_AgreeOnDocId(it, &docId);
  if (ITERATOR_OK == rc) {
    it->base.lastDocId = it->base.current->docId;
  } else if (ITERATOR_NOTFOUND == rc) {
    // Not found - but we need to read the next valid result
    // `docId` was set to the next valid docId by `II_AgreeOnDocId`
    IteratorStatus read_rc = II_Find_Consensus(it, docId);
    if (read_rc != ITERATOR_OK) return read_rc; // If we failed to read, bubble up the error
  }
  return rc;
}

static size_t II_NumEstimated(const QueryIterator *base) {
  const IntersectionIterator *it = (const IntersectionIterator *)base;
  return it->num_expected;
}

static void II_Rewind(QueryIterator *base) {
  IntersectionIterator *ii = (IntersectionIterator *)base;

  base->atEOF = false;
  base->lastDocId = 0;
  IndexResult_ResetAggregate(base->current);

  // rewind all child iterators
  for (uint32_t i = 0; i < ii->num_its; i++) {
    ii->its[i]->Rewind(ii->its[i]);
  }
}

static void II_Free(QueryIterator *base) {
  IntersectionIterator *ii = (IntersectionIterator *)base;

  for (uint32_t i = 0; i < ii->num_its; i++) {
    // This function may be called with NULL iterators, so we check for NULLs
    if (ii->its[i]) ii->its[i]->Free(ii->its[i]);
  }

  rm_free(ii->its);
  IndexResult_Free(base->current);
  rm_free(base);
}

/*********************** Constructor helpers for the intersection iterator ***********************/

static inline double iteratorFactor(const QueryIterator *it) {
  double factor = 1.0;
  if (it->type == INTERSECT_ITERATOR) {
    // on INTERSECT iterator, we divide the estimate by the number of children
    // since we skip as soon as a number is not in all iterators
    factor = 1.0 / ((const IntersectionIterator *)it)->num_its;
  } else if (it->type == UNION_ITERATOR && RSGlobalConfig.prioritizeIntersectUnionChildren) {
    factor = ((const UnionIterator *)it)->num;
  }
  return factor;
}

typedef int (*CompareFunc)(const void *a, const void *b);
static int cmpIter(const QueryIterator *const *it1, const QueryIterator *const *it2) {
  double factor1 = iteratorFactor(*it1);
  double factor2 = iteratorFactor(*it2);
  double est1 = (*it1)->NumEstimated(*it1) * factor1;
  double est2 = (*it2)->NumEstimated(*it2) * factor2;
  return (int)(est1 - est2);
}

// Set estimation for number of results.
static void II_SetEstimation(IntersectionIterator *it) {
  // Set the expected number of results to the minimum of all iterators.
  // If any of the iterators is NULL, we set the expected number to 0
  RS_ASSERT(it->num_its); // Ensure there is at least one iterator, so we can set num_expected to SIZE_MAX temporarily
  it->num_expected = SIZE_MAX;
  for (uint32_t i = 0; i < it->num_its; ++i) {
    const QueryIterator *cur = it->its[i];
    size_t amount = cur->NumEstimated(cur);
    if (amount < it->num_expected) {
      it->num_expected = amount;
    }
  }
}

/**
 * Reduce the intersection iterator by applying these rules:
 * 1. If any of the iterators is an empty iterator, return the empty iterator and update the number of children
 * 2. Remove all wildcard iterators since they would not contribute to the intersection (Return one of them if all are wildcards)
 * 3. If there is only one left child iterator, return it
 * 4. Otherwise, return NULL and let the caller create the intersection iterator
*/
static QueryIterator *IntersectionIteratorReducer(QueryIterator **its, size_t *num) {
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

static ValidateStatus II_Revalidate(QueryIterator *base) {
  IntersectionIterator *ii = (IntersectionIterator *)base;
  bool any_child_moved = false, movedToEOF = false;
  t_docId max_child_docId = 0;

  // Step 1: Revalidate all children and track status
  for (uint32_t i = 0; i < ii->num_its; i++) {
    QueryIterator *child = ii->its[i];
    ValidateStatus child_status = child->Revalidate(child);

    if (child_status == VALIDATE_ABORTED) {
      return VALIDATE_ABORTED; // Intersection fails if any child fails
    }

    if (child_status == VALIDATE_MOVED) {
      any_child_moved = true;
      // Track the maximum docId among moved children
      if (child->atEOF) {
        movedToEOF = true; // If any child moved and now at EOF, the intersection is also at EOF now
      } else if (child->lastDocId > max_child_docId) {
        max_child_docId = child->lastDocId;
      }
    }
  }

  // Step 2: Handle the result based on child status
  if (!any_child_moved) {
    // All children returned OK - simply return OK
    return VALIDATE_OK;
  } else if (base->atEOF) {
    // If the intersection was already at EOF, we return OK
    return VALIDATE_OK;
  } else if (movedToEOF) {
    // If any child is at EOF, the intersection is also moved to EOF
    base->atEOF = true;
    return VALIDATE_MOVED;
  }

  // Step 3: At least one child moved - need to find new intersection position
  // Skip to the maximal docId among moved children
  base->SkipTo(base, max_child_docId);
  return VALIDATE_MOVED;
}

QueryIterator *NewIntersectionIterator(QueryIterator **its, size_t num, int max_slop, bool in_order, double weight) {
  QueryIterator *ret = IntersectionIteratorReducer(its, &num);
  if (ret != NULL) {
    return ret;
  }
  RS_ASSERT(its && num > 1);
  IntersectionIterator *it = rm_calloc(1, sizeof(*it));
  it->its = its;
  it->num_its = num;
  // If max_slop is negative, we set it to INT_MAX in case `in_order` is true
  it->max_slop = max_slop < 0 ? INT_MAX : max_slop;
  it->in_order = in_order;

  II_SetEstimation(it);

  // Sort children iterators from low count to high count which reduces the number of iterations.
  if (!in_order) {
    qsort(its, num, sizeof(*its), (CompareFunc)cmpIter);
  }

  // bind the iterator calls
  ret = &it->base;
  ret->type = INTERSECT_ITERATOR;
  ret->atEOF = false;
  ret->lastDocId = 0;
  ret->current = NewIntersectResult(num, weight);
  ret->NumEstimated = II_NumEstimated;
  if (max_slop < 0 && !in_order) {
    // No slop and no order means every result is relevant, so we can use the fast path
    ret->Read = II_Read;
    ret->SkipTo = II_SkipTo;
  } else {
    // Otherwise, we need to check relevancy
    ret->Read = II_Read_CheckRelevancy;
    ret->SkipTo = II_SkipTo_CheckRelevancy;
  }
  ret->Free = II_Free;
  ret->Rewind = II_Rewind;
  ret->Revalidate = II_Revalidate;

  return ret;
}
