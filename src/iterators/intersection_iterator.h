/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "iterator_api.h"

typedef struct IntersectionIterator {
  QueryIterator base;

  // The iterators to intersect
  QueryIterator **its;
  uint32_t num_its;

  uint32_t max_slop : 31;
  bool in_order : 1;

  size_t num_expected;
} IntersectionIterator;

QueryIterator *NewIntersectionIterator(QueryIterator **its, size_t num, int maxSlop, bool inOrder, double weight);

#ifdef __cplusplus
}
#endif
