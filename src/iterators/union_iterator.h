/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "iterator_api.h"
#include "util/heap.h"
#include "query_node.h"
#include "config.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  QueryIterator base;
  heap_t *heap_min_id;
  /**
   * We maintain two iterator arrays. One is the original iterator list, and
   * the other is the list of currently active iterators. When an iterator
   * reaches EOF, it is removed from the `its` list, but is still retained in
   * the `its_orig` list, for the purpose of supporting things like Rewind() and
   * Free()
   */
  QueryIterator **its;      // child iterator array, might change/shuffle throughout the query execution
  QueryIterator **its_orig; // "const" copy of child iterator array, used to rewind and free the iterator
  uint32_t num;             // number of non-depleted child iterators
  uint32_t num_orig;        // the length of `its_orig`

  // type of query node UNION,GEO,NUMERIC...
  QueryNodeType type;
  // original string for fuzzy or prefix unions
  const char *q_str;
} UnionIterator;

/**
 * @param its + num - iterators to get the union of
 * @param quickExit - when returning a result (read/skip), whether to collect all the children with the matching ID, or return after the first match
 * @param weight - the weight of the node (assigned to the returned result)
 * @param type - node type - used by profile iterator to generate node's name
 * @param q_str - slice of the query that yielded this union node - used by profile iterator
 * @param config - pointer to a valid configuration struct for construction decisions
 */
QueryIterator *NewUnionIterator(QueryIterator **its, int num, bool quickExit, double weight,
                                QueryNodeType type, const char *q_str, IteratorsConfig *config);

// Sync state according to `its_orig` and `num_orig` (exposed for profile iterator injection)
void UI_SyncIterList(UnionIterator *ui);

#ifdef __cplusplus
}
#endif
