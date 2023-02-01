/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __SEARCH_CTX_H
#define __SEARCH_CTX_H

#include <sched.h>

#include "redismodule.h"
#include "spec.h"
#include "concurrent_ctx.h"
#include "trie/trie_type.h"
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__FreeBSD__)
#define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

typedef enum {
  RS_CTX_UNSET,
  RS_CTX_READONLY,
  RS_CTX_READWRITE
} RSContextFlags;


/** Context passed to all redis related search handling functions. */
typedef struct RedisSearchCtx {
  RedisModuleCtx *redisCtx;
  RedisModuleKey *key_;
  IndexSpec *spec;
  uint64_t specId;  // Unique id of the spec; used when refreshing
  struct timespec timeout;
  unsigned int apiVersion; // API Version to allow for backward compatibility / alternative functionality
  RSContextFlags flags;
} RedisSearchCtx;

#define SEARCH_CTX_SORTABLES(ctx) ((ctx && ctx->spec) ? ctx->spec->sortables : NULL)
// Create a string context on the heap
// Returned context includes a strong reference to the spec
RedisSearchCtx *NewSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL);

// Same as above, only from c string (null terminated)
RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);

static inline RedisSearchCtx SEARCH_CTX_STATIC(RedisModuleCtx *ctx, IndexSpec *sp) {
  RedisSearchCtx sctx = {
                          .redisCtx = ctx,
                          .key_ = NULL,
                          .spec = sp,
                          .timeout = { 0, 0 },
                          .flags = RS_CTX_UNSET, };
  return sctx;
}

void SearchCtx_CleanUp(RedisSearchCtx * sctx);

void SearchCtx_Free(RedisSearchCtx *sctx);

void RedisSearchCtx_LockSpecRead(RedisSearchCtx *sctx);

void RedisSearchCtx_LockSpecWrite(RedisSearchCtx *sctx);

void RedisSearchCtx_UnlockSpec(RedisSearchCtx *sctx);

#ifdef __cplusplus
}
#endif
#endif
