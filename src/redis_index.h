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
QueryIterator *Redis_OpenReader(const RedisSearchCtx *ctx, RSQueryTerm *term, DocTable *dt,
                                 t_fieldMask fieldMask, double weight);

InvertedIndex *Redis_OpenInvertedIndex(const RedisSearchCtx *ctx, const char *term, size_t len,
                                         int write, bool *outIsNew);

/*
 * Select a random term from the index that matches the index prefix and inveted key format.
 * It tries RANDOMKEY 10 times and returns NULL if it can't find anything.
 */
const char *Redis_SelectRandomTerm(const RedisSearchCtx *ctx, size_t *tlen);

#define TERM_KEY_FORMAT "ft:%s/%.*s"
#define TERM_KEY_PREFIX "ft:"
#define SKIPINDEX_KEY_FORMAT "si:%s/%.*s"
#define SCOREINDEX_KEY_FORMAT "ss:%s/%.*s"

#define DONT_CREATE_INDEX false
#define CREATE_INDEX true

typedef int (*ScanFunc)(RedisModuleCtx *ctx, RedisModuleString *keyName, void *opaque);

/* Optimize the buffers of a speicif term hit */
int Redis_OptimizeScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque);

int Redis_DeleteKey(RedisModuleCtx *ctx, RedisModuleString *s);
int Redis_DeleteKeyC(RedisModuleCtx *ctx, char *cstr);

/* Drop all the index's internal keys using this scan handler */
int Redis_DropScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque);

/* Collect memory stas on the index */
int Redis_StatsScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque);
/**
 * Format redis key for a term.
 * TODO: Add index name to it
 */
RedisModuleString *fmtRedisTermKey(const RedisSearchCtx *ctx, const char *term, size_t len);
RedisModuleString *fmtRedisSkipIndexKey(const RedisSearchCtx *ctx, const char *term, size_t len);
RedisModuleString *fmtRedisNumericIndexKey(const RedisSearchCtx *ctx, const HiddenString *field);

#endif
