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
#include "util/timeout.h"
#include "query_ctx.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  QueryIterator base;         // base index iterator
  QueryIterator *wcii;        // wildcard index iterator
  QueryIterator *child;       // child index iterator
  t_docId maxDocId;
  TimeoutCtx timeoutCtx;
} NotIterator;

/**
* @param it - The iterator to negate
* @param maxDocId - the maximum docId
* @param weight - the weight of the node (assigned to the returned result)
* @param timeout - the timeout for the iterator
* @param q - the query context
*/
QueryIterator *NewNotIterator(QueryIterator *it, t_docId maxDocId, double weight, struct timespec timeout, QueryEvalCtx *q);

// Constructor used for benchmarking (easy to inject MockIterators)
QueryIterator *_New_NotIterator_With_WildCardIterator(QueryIterator *child, QueryIterator *wcii, t_docId maxDocId, double weight, struct timespec timeout);

#ifdef __cplusplus
}
#endif
