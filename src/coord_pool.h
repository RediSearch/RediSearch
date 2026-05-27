/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_COORD_POOL_H_
#define RS_COORD_POOL_H_

#include "redisearch.h"
#include "redismodule.h"
#include "thpool/thpool.h"
#include "util/references.h"
#include "rs_wall_clock.h"
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Coordinator thread-pool helpers. */

typedef void (*CoordReopenCallback)(void *ctx);

/* Destroy the coordinator thread pool created with `CoordPool_CreatePool` */
void CoordPool_ThreadPoolDestroy(void);

/* Create the coordinator thread pool. */
void CoordPool_CreatePool(int numThreads);

/* Run a function on the coordinator thread pool */
void CoordPool_ThreadPoolRun(void (*func)(void *), void *arg);

/* return number of currently working threads */
size_t CoordPool_WorkingThreadCount();

/* return number of pending high priority jobs */
size_t CoordPool_HighPriorityPendingJobsCount();

typedef struct CoordPoolHandlerCtx {
  rs_wall_clock_ns_t coordStartTime;  // Time when command was received on coordinator
  rs_wall_clock_ns_t coordQueueTime;  // Time spent waiting in coordinator thread pool queue
  WeakRef spec_ref;                   // Weak reference to the index spec
  bool isProfile;                     // Whether this is an FT.PROFILE command
  size_t numShards;                   // Number of shards in the cluster (captured from main thread)
} CoordPoolHandlerCtx;

// Initialize a CoordPoolHandlerCtx to zero
static inline void CoordPoolHandlerCtx_Init(CoordPoolHandlerCtx *ctx) {
  memset(ctx, 0, sizeof(*ctx));
}

/********************************************* for debugging **********************************/

int CoordPool_isPaused();

int CoordPool_pause();

int CoordPool_resume();

thpool_stats CoordPool_getStats();

#ifdef __cplusplus
}
#endif
#endif // RS_COORD_POOL_H_
