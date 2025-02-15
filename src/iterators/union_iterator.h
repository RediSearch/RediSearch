/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "iterator_api.h"

typedef struct {
  QueryIterator base;
  heap_t *heapMinId;
  size_t nExpected;
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
  QueryNodeType origType;
  // original string for fuzzy or prefix unions
  const char *q_str;
} UnionIterator;

QueryIterator *NewUnionIterator(QueryIterator **its, int num, bool quickExit, double weight,
                                QueryNodeType type, const char *q_str, IteratorsConfig *config);
