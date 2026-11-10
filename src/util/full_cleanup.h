/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef FULL_CLEANUP_H__
#define FULL_CLEANUP_H__

#include <stdbool.h>

#include "redismodule.h"
#include "thpool/thpool.h"

#ifdef __cplusplus
extern "C" {
#endif

// Full cleanup (RediSearch_CleanupModule, run from the shutdown event under RS_GLOBAL_DTORS)
// frees every module resource. A pool teardown normally releases the GIL around its join, so a
// job blocked on the GIL can finish. SHUTDOWN fires the event from inside the command, where Redis
// does not let the main thread release the GIL, so then a pool is joined only once it is idle.
// TODO(MOD-21057): cancel the remaining jobs instead, and join without the GIL in every case.

// Records SHUTDOWN commands, so FullCleanup_HoldsGIL can tell them from a signal. Call once at
// load, and only under RS_GLOBAL_DTORS: the filter it installs sees every command.
void FullCleanup_WatchShutdownCommand(RedisModuleCtx *ctx);

// Marks full cleanup as started. Main thread only; there is no way back.
void FullCleanup_Begin(void);
// True once FullCleanup_Begin was called. Main thread only.
bool FullCleanup_InProgress(void);
// True if full cleanup runs inside a SHUTDOWN command, so it must not release the GIL.
bool FullCleanup_HoldsGIL(void);

// Polls `idle(arg)`, holding the GIL, until it returns true or the full-cleanup deadline passes.
// Returns false, having logged `what`, if it never did: what is still running waits for the GIL,
// which the main thread keeps until the process exits, so the caller must leave it in place and
// free nothing it may reach.
bool FullCleanup_WaitUntilIdle(bool (*idle)(void *arg), void *arg, const char *what);

// FullCleanup_WaitUntilIdle on `pool` having no queued and no running jobs.
bool FullCleanup_WaitForPool(redisearch_thpool_t *pool, const char *what);

// Joins and frees `pool`, releasing the GIL around the join unless FullCleanup_HoldsGIL, in which
// case it first waits for the pool as FullCleanup_WaitForPool does and returns false, leaving the
// pool untouched, if it never went idle. Also valid outside full cleanup.
bool FullCleanup_DestroyPool(redisearch_thpool_t *pool, const char *what);

#ifdef __cplusplus
}
#endif

#endif  // FULL_CLEANUP_H__
