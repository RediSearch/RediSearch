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
  QueryIterator base;
  t_docId topId;
  t_docId currentId;
  t_docId numDocs;
} WildcardIterator;

/**
 * @param maxId - The maxID to return
 * @param numDocs - the number of docs to return
 */
QueryIterator *IT_V2(NewWildcardIterator_NonOptimized)(t_docId maxId, size_t numDocs);

/**
 * Create a new wildcard iterator. This API should be preferred over the non-optimized version.
 * If possible, it will use the optimized wildcard iterator,
 * otherwise it will fall back to the non-optimized version.
 * @param q - The query evaluation context
 */
QueryIterator *IT_V2(NewWildcardIterator)(QueryEvalCtx *q);

#ifdef __cplusplus
}
#endif
