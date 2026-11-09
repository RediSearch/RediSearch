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

#include "thpool/thpool.h"

#ifdef __cplusplus
extern "C" {
#endif

// Full cleanup (RediSearch_CleanupModule, run from the shutdown event under RS_GLOBAL_DTORS)
// frees every module resource. SHUTDOWN fires that event from inside the command, where Redis
// does not let the main thread release the GIL, so during it a background thread is joined only
// once it no longer needs the GIL.
// TODO(MOD-21057): cancel the remaining jobs instead of leaving them running.

// Marks full cleanup as started. Main thread only; there is no way back.
void FullCleanup_Begin(void);
// True once FullCleanup_Begin was called. Main thread only.
bool FullCleanup_InProgress(void);

// Polls `idle(arg)`, holding the GIL, until it returns true or the full-cleanup deadline passes.
// Returns false, having logged `what`, if it never did: what is still running waits for the GIL,
// which the main thread keeps until the process exits, so the caller must leave it in place and
// free nothing it may reach.
bool FullCleanup_WaitUntilIdle(bool (*idle)(void *arg), void *arg, const char *what);

// FullCleanup_WaitUntilIdle on `pool` having no queued and no running jobs.
bool FullCleanup_WaitForPool(redisearch_thpool_t *pool, const char *what);

#ifdef __cplusplus
}
#endif

#endif  // FULL_CLEANUP_H__
