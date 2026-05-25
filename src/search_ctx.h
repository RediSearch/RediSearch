/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __SEARCH_CTX_H
#define __SEARCH_CTX_H

#include <sched.h>

#include "redismodule.h"
#include "spec.h"
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__FreeBSD__)
#define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

#define APIVERSION_RETURN_MULTI_CMP_FIRST 3

typedef enum {
  RS_CTX_UNSET,
  RS_CTX_READONLY,
  RS_CTX_READWRITE
} RSContextFlags;

typedef struct SearchTime {
  // current execution start time - real clock
  struct timespec current;
  // when the query should timeout - monotonic raw clock, unrelated to real clock
  struct timespec timeout;
  // Flag to skip timeout checks (used in background thread mode with FAIL policy)
  bool skipTimeoutChecks;
  // Borrowed RS_Atomic(bool) timed-out flag, wired in AREQ_ApplyContext.
  // Read via SearchTime_IsTimedOut. NULL on contexts without an owning AREQ.
  // TODO: move to QueryProcessingCtx.
  const void *timedOutFlag;
} SearchTime;

// Returns true iff the SearchTime (passed as `void *` so the function doubles
// as a SyncPoint stop predicate) has a wired timed-out flag and that flag has
// been set by the main-thread timeout callback. NULL arg or unwired flag both
// return false. Reads with memory_order_relaxed: callers only need to observe
// the flag — there is no surrounding state that requires synchronization.
bool SearchTime_IsTimedOut(void *arg);

/** Context passed to all redis related search handling functions. */
typedef struct RedisSearchCtx {
  RedisModuleCtx *redisCtx;
  RedisModuleKey *key_;
  IndexSpec *spec;
  SearchTime time;
  unsigned int apiVersion; // API Version to allow for backward compatibility / alternative functionality
  unsigned int expanded; // Reply format
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
                          .time = {.current = { 0, 0 }, .timeout = { 0, 0 }, .skipTimeoutChecks = false, .timedOutFlag = NULL},
                          .flags = RS_CTX_UNSET,};
  return sctx;
}

void SearchCtx_UpdateTime(RedisSearchCtx *sctx, int32_t durationNS);

void SearchCtx_CleanUp(RedisSearchCtx * sctx);

void SearchCtx_Free(RedisSearchCtx *sctx);

void RedisSearchCtx_LockSpecRead(RedisSearchCtx *sctx);

int RedisSearchCtx_TryLockSpecRead(RedisSearchCtx *sctx);

void RedisSearchCtx_LockSpecWrite(RedisSearchCtx *sctx);

void RedisSearchCtx_UnlockSpec(RedisSearchCtx *sctx);

#ifdef __cplusplus
}
#endif
#endif
