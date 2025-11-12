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
#include "query_ctx.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  QueryIterator base;     // base index iterator
  QueryIterator *child;   // child index iterator
  QueryIterator *wcii;    // wildcard child iterator, used for optimization
  RSIndexResult *virt;
  t_docId maxDocId;
  double weight;
} OptionalIterator;

QueryIterator *NewOptionalIterator(QueryIterator *it, QueryEvalCtx *q, double weight);

#ifdef __cplusplus
}
#endif
