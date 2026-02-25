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

QueryIterator *NewOptionalIterator(QueryIterator *it, QueryEvalCtx *q, double weight);

QueryIterator const *GetOptionalIteratorChild(const QueryIterator *const it);
void SetOptionalIteratorChild(QueryIterator *it, QueryIterator* child);
QueryIterator *TakeOptionalIteratorChild(QueryIterator *it);

QueryIterator const *GetOptionalOptimizedIteratorWildcard(QueryIterator *const it);
void SetOptionalOptimizedIteratorWildcard(QueryIterator *it, QueryIterator* newWcii);

#ifdef __cplusplus
}
#endif
