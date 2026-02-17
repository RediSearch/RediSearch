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

/**
* @param it - The iterator to negate
* @param maxDocId - the maximum docId
* @param weight - the weight of the node (assigned to the returned result)
* @param timeout - the timeout for the iterator
* @param q - the query context
*/
QueryIterator *NewNotIterator(QueryIterator *it, t_docId maxDocId, double weight, struct timespec timeout, QueryEvalCtx *q);

// Constructor used for benchmarking (easy to inject MockIterators)
// timeoutCounter: initial counter value (use REDISEARCH_UNINITIALIZED to skip timeout checks)
QueryIterator *_New_NotIterator_With_WildCardIterator(QueryIterator *child, QueryIterator *wcii, t_docId maxDocId, double weight, struct timespec timeout, uint32_t timeoutCounter);

QueryIterator const *GetNotIteratorChild(QueryIterator *const it);
void SetNotIteratorChild(QueryIterator *it, QueryIterator* child);
QueryIterator *TakeNotIteratorChild(QueryIterator *it);

void _SetOptimizedNotIteratorWildcard(QueryIterator *it, QueryIterator* wcii);
QueryIterator const *_GetOptimizedNotIteratorWildcard(QueryIterator *it);

#ifdef __cplusplus
}
#endif
