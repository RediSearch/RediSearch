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
 * Create a new optimized wildcard iterator.
 * This iterator can only be used when the index is configured to index all documents.
 * @param sctx - The search context
 */
QueryIterator *NewWildcardIterator_Optimized(const RedisSearchCtx *sctx, double weight);

/**
 * Create a new wildcard iterator.
 * If possible, it will use the optimized wildcard iterator,
 * otherwise it will fall back to the non-optimized version.
 * @param q - The query evaluation context
 * @param weight - The weight of the iterator
 */
QueryIterator *NewWildcardIterator(const QueryEvalCtx *q, double weight);

bool IsWildcardIterator(const QueryIterator *it);

#ifdef __cplusplus
}
#endif
