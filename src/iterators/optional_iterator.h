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

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  QueryIterator base;     // base index iterator
  QueryIterator *child;   // child index iterator
  RSIndexResult *virt;
  t_docId maxDocId;
  double weight;
} OptionalIterator;

QueryIterator *IT_V2(NewOptionalIterator_NonOptimized)(QueryIterator *it, t_docId maxDocId, size_t numDocs, double weight);

#ifdef __cplusplus
}
#endif
