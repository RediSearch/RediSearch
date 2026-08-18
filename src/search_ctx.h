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

struct QueryRequestTimeout;

#if defined(__FreeBSD__)
#define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

#define APIVERSION_RETURN_MULTI_CMP_FIRST 3

typedef enum {
  SPEC_LOCK_UNSET,
  SPEC_LOCK_READ,
  SPEC_LOCK_WRITE
} SpecLockState;

/** Context passed to all redis related search handling functions. */
typedef struct RedisSearchCtx {
  RedisModuleCtx *redisCtx;
  RedisModuleKey *key_;
  IndexSpec *spec;
  // Real-clock snapshot shared by document, field, and disk TTL checks so one execution cycle
  // evaluates every expiration against the same instant.
  struct timespec currentTime;
  // Borrowed request timeout, wired when the request adopts this search context.
  // NULL when there is no owning request.
  struct QueryRequestTimeout *timeout;
  unsigned int apiVersion; // API Version to allow for backward compatibility / alternative functionality
  unsigned int expanded; // Reply format
  SpecLockState lock_state;
  // Per-query disk snapshot (optional, NULL when no snapshot has been taken or when the
  // backing index has no disk component). Used by the disk-iterator construction paths
  // so all iterators created during one query observe a consistent on-disk view.
  // Owned by the query setup that took the snapshot; iterators borrow it.
  RedisSearchDiskSnapshot *diskSnapshot;
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
                          .currentTime = { 0, 0 },
                          .timeout = NULL,
                          .lock_state = SPEC_LOCK_UNSET,
                          .diskSnapshot = NULL,};
  return sctx;
}

// Refreshes the real-clock snapshot used for document, field, and disk TTL checks.
void SearchCtx_UpdateCurrentTime(RedisSearchCtx *sctx);

typedef struct QueryError QueryError;

// Open a disk snapshot on `sctx` for the duration of one query, so every iterator
// built from `sctx` (and any snapshot-aware disk read on the same sctx) observes the
// same point-in-time view. Must be called exactly once per sctx, while holding the
// spec read lock so the in-memory trie/stats consulted by query planning are coherent
// with the snapshot. Calling this on an sctx that already has a snapshot asserts.
//
// Returns REDISMODULE_OK in two cases:
//   - the index has no disk component (no snapshot needed), or
//   - the disk snapshot was successfully created.
// Returns REDISMODULE_ERR and sets `status` if the index is disk-backed but
// the underlying `SearchDisk_CreateSnapshot` returned NULL. Callers must abort
// the query in that case rather than fall back to live disk reads.
int SearchCtx_TakeDiskSnapshot(RedisSearchCtx *sctx, QueryError *status);

void SearchCtx_CleanUp(RedisSearchCtx * sctx);

void SearchCtx_Free(RedisSearchCtx *sctx);

void RedisSearchCtx_LockSpecRead(RedisSearchCtx *sctx);

int RedisSearchCtx_TryLockSpecRead(RedisSearchCtx *sctx);

void RedisSearchCtx_LockSpecWrite(RedisSearchCtx *sctx);

void RedisSearchCtx_UnlockSpec(RedisSearchCtx *sctx);

/* Debug-only (ENABLE_ASSERT) check that the spec lock is not held. Used at
 * background request-cycle boundaries: the lock must be taken and released
 * within a single cycle, on the same worker thread — a later release (request
 * free / client unblock on the main thread) would unlock the pthread_rwlock
 * from a thread that does not own it, which is undefined behavior. */
#define RedisSearchCtx_AssertLockNotHeld(sctx) \
  RS_LOG_ASSERT(!(sctx) || (sctx)->lock_state == SPEC_LOCK_UNSET, "spec lock must not be held")

#ifdef __cplusplus
}
#endif
#endif
