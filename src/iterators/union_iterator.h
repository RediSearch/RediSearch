/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "iterator_api.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  QueryIterator base;
  heap_t *heap_min_id;
  size_t num_results_estimated;
  /**
   * We maintain two iterator arrays. One is the original iterator list, and
   * the other is the list of currently active iterators. When an iterator
   * reaches EOF, it is removed from the `its` list, but is still retained in
   * the `its_orig` list, for the purpose of supporting things like Rewind() and
   * Free()
   */
  QueryIterator **its;
  QueryIterator **its_orig;
  uint32_t num;
  uint32_t num_orig;

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
QueryIterator *IT_V2(NewUnionIterator)(QueryIterator **its, int num, bool quickExit, double weight,
                                QueryNodeType type, const char *q_str, IteratorsConfig *config);

#ifdef __cplusplus
}
#endif
