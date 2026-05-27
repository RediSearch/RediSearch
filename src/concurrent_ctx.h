/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_CONCERRNT_CTX_
#define RS_CONCERRNT_CTX_

#include "redisearch.h"
#include "redismodule.h"
#include "thpool/thpool.h"
#include "util/references.h"
#include "rs_wall_clock.h"
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Concurrent Search Execution Context.
 */

typedef void (*ConcurrentReopenCallback)(void *ctx);

/* Destroys all thread pools created with `ConcurrentSearch_CreatePool` */
void ConcurrentSearch_ThreadPoolDestroy(void);

/* Create a new thread pool, and return its identifying id */
int ConcurrentSearch_CreatePool(int numThreads);

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type);

/* return number of currently working threads */
size_t ConcurrentSearchPool_WorkingThreadCount();

/* return number of pending high priority jobs */
size_t ConcurrentSearchPool_HighPriorityPendingJobsCount();

typedef struct ConcurrentSearchHandlerCtx {
  rs_wall_clock_ns_t coordStartTime;  // Time when command was received on coordinator
  rs_wall_clock_ns_t coordQueueTime;  // Time spent waiting in coordinator thread pool queue
  WeakRef spec_ref;                   // Weak reference to the index spec
  bool isProfile;                     // Whether this is an FT.PROFILE command
  size_t numShards;                   // Number of shards in the cluster (captured from main thread)
} ConcurrentSearchHandlerCtx;

// Initialize a ConcurrentSearchHandlerCtx to zero
static inline void ConcurrentSearchHandlerCtx_Init(ConcurrentSearchHandlerCtx *ctx) {
  memset(ctx, 0, sizeof(*ctx));
}

/********************************************* for debugging **********************************/

int ConcurrentSearch_isPaused();

int ConcurrentSearch_pause();

int ConcurrentSearch_resume();

thpool_stats ConcurrentSearch_getStats();

#ifdef __cplusplus
}
#endif
#endif // RS_CONCERRNT_CTX_
