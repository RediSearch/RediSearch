/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __REDIS_INDEX__
#define __REDIS_INDEX__

#include "document.h"
#include "inverted_index.h"
#include "search_ctx.h"
#include "concurrent_ctx.h"
#include "spec.h"
#include "iterators/iterator_api.h"

/* Open an inverted index reader on a redis DMA string, for a specific term.
 * If singleWordMode is set to 1, we do not load the skip index, only the score index
 */
QueryIterator *Redis_OpenReader(const RedisSearchCtx *ctx, RSToken *tok, int tok_id, DocTable *dt,
                                 t_fieldMask fieldMask, double weight);

InvertedIndex *Redis_OpenInvertedIndex(const RedisSearchCtx *ctx, const char *term, size_t len,
                                        bool write, bool *outIsNew);

#define DONT_CREATE_INDEX false
#define CREATE_INDEX true

int Redis_LegacyDeleteKey(RedisModuleCtx *ctx, RedisModuleString *s);
int Redis_DeleteKeyC(RedisModuleCtx *ctx, char *cstr);

/* Drop all the index's internal keys using this scan handler */
int Redis_LegacyDropScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque);

/**
 * Format redis key for a term.
 */
RedisModuleString *Legacy_fmtRedisTermKey(const RedisSearchCtx *ctx, const char *term, size_t len);

#endif
