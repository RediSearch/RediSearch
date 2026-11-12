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
#include "rmutil/rm_assert.h"
#include "search_disk_api.h"
#include "spec.h"
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

struct QueryRequestLimits;

#if defined(__FreeBSD__)
#define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

#define APIVERSION_RETURN_MULTI_CMP_FIRST 3

/** Context passed to all redis related search handling functions. */
typedef struct RedisSearchCtx {
  // Borrowed, never owned; valid only within the execution cycle that lent it
  // (command handler, worker cycle, or per-read install for cursor reads).
  RedisModuleCtx *redisCtx;
  IndexSpec *spec;
  // Real-clock snapshot shared by document, field, and disk TTL checks so one execution cycle
  // evaluates every expiration against the same instant.
  struct timespec currentTime;
  // Borrowed request timeout, wired when the request adopts this search context.
  // NULL when there is no owning request.
  struct QueryRequestLimits *limits;
  uint8_t apiVersion; // API Version to allow for backward compatibility / alternative functionality
  // Per-query disk read view (NULL until opened, or when the backing index has no disk
  // component). Every disk iterator and disk metadata read built from this context passes
  // it, so they agree on which documents the query can see. Owned by the query setup that
  // opened it; iterators borrow it.
  RedisSearchDiskSnapshot *diskSnapshot;
} RedisSearchCtx;

#define SEARCH_CTX_SORTABLES(ctx) ((ctx && ctx->spec) ? ctx->spec->sortables : NULL)
// Create a string context on the heap
// Returned context includes a strong reference to the spec
RedisSearchCtx *NewSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL);

// Same as above, only from c string (null terminated)
RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);

// Same as NewSearchCtxC, with explicit index-load accounting options.
RedisSearchCtx *NewSearchCtxCEx(RedisModuleCtx *ctx, const char *indexName, bool resetTTL,
                                IndexLoadOptionsFlags flags);

static inline RedisSearchCtx SEARCH_CTX_STATIC(RedisModuleCtx *ctx, IndexSpec *sp) {
  RedisSearchCtx sctx = {
                          .redisCtx = ctx,
                          .spec = sp,
                          .currentTime = { 0, 0 },
                          .limits = NULL,
                          .diskSnapshot = NULL,};
  return sctx;
}

// Refreshes the real-clock snapshot used for document, field, and disk TTL checks.
void SearchCtx_UpdateCurrentTime(RedisSearchCtx *sctx);

typedef struct QueryError QueryError;

// Open the per-query disk read view on `sctx`, so every iterator built from `sctx` (and
// every disk metadata read on the same sctx) agrees on which documents the query can see.
// Must be called exactly once per sctx, while holding the spec read lock, so the view is
// opened against the same index state as the in-memory trie/stats consulted by query
// planning. Calling this on an sctx that already has a view asserts.
//
// Returns REDISMODULE_OK in two cases:
//   - the index has no disk component (no view needed), or
//   - the disk read view was successfully opened.
// Returns REDISMODULE_ERR and sets `status` if the index is disk-backed but
// the underlying `SearchDisk_CreateSnapshot` returned NULL. Callers must abort
// the query in that case rather than read the disk index without a view.
int SearchCtx_TakeDiskSnapshot(RedisSearchCtx *sctx, QueryError *status);

// Context destruction never releases a spec lock: a cursor sweep can destroy
// an idle request while the same thread holds a different request's lock.
void SearchCtx_CleanUp(RedisSearchCtx * sctx);

void SearchCtx_Free(RedisSearchCtx *sctx);

#ifdef __cplusplus
}
#endif
#endif
