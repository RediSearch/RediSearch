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

struct ConcurrentCmdCtx;
typedef void (*ConcurrentCmdHandler)(RedisModuleCtx *, RedisModuleString **, int,
                                     struct ConcurrentCmdCtx *);

// Context for concurrent search handler
// Contains additional parameters passed to ConcurrentSearch_HandleRedisCommandEx
struct CoordRequestCtx;  // Forward declaration

// Context for blocking client
typedef struct ConcurrentSearchBlockClientCtx {
  RedisModuleCmdFunc callback;            // Callback for timeout
  rs_wall_clock_ms_t timeoutMS;           // Timeout value in milliseconds (0 if no timeout)
  void *privdata;                         // Private data for the blocked client
  void (*free_privdata)(RedisModuleCtx*, void*);           // Callback to free private data
} ConcurrentSearchBlockClientCtx;

typedef struct ConcurrentSearchHandlerCtx {
  rs_wall_clock_ns_t coordStartTime;  // Time when command was received on coordinator
  rs_wall_clock_ns_t coordQueueTime;  // Time spent waiting in coordinator thread pool queue
  WeakRef spec_ref;                   // Weak reference to the index spec
  bool isProfile;                     // Whether this is an FT.PROFILE command
  size_t numShards;                   // Number of shards in the cluster (captured from main thread)
  ConcurrentSearchBlockClientCtx bcCtx; // Context for blocking client
} ConcurrentSearchHandlerCtx;

// Initialize a ConcurrentSearchHandlerCtx to zero
static inline void ConcurrentSearchHandlerCtx_Init(ConcurrentSearchHandlerCtx *ctx) {
  memset(ctx, 0, sizeof(*ctx));
}

#define CMDCTX_KEEP_RCTX 0x01

/**
 * Take ownership of the underlying Redis command context. Once ownership is
 * claimed, the context needs to be freed (at some point in the future) via
 * RM_FreeThreadSafeContext()
 *
 * TODO/FIXME:
 * The context is tied to a BlockedCLient, but it shouldn't actually utilize it.
 * Need to add an API to Redis to better manage a thread safe context, or to
 * otherwise 'detach' it from the Client so that trying to perform I/O on it
 * would result in an error rather than simply using a dangling pointer.
 */
void ConcurrentCmdCtx_KeepRedisCtx(struct ConcurrentCmdCtx *ctx);

// Returns the WeakRef held in the context.
WeakRef ConcurrentCmdCtx_GetWeakRef(struct ConcurrentCmdCtx *cctx);

// Returns the coordinator start time held in the context.
rs_wall_clock_ns_t ConcurrentCmdCtx_GetCoordStartTime(struct ConcurrentCmdCtx *cctx);

// Returns the number of shards captured from the main thread.
size_t ConcurrentCmdCtx_GetNumShards(const struct ConcurrentCmdCtx *cctx);

// Returns the blocked client held in the context.
RedisModuleBlockedClient *ConcurrentCmdCtx_GetBlockedClient(struct ConcurrentCmdCtx *cctx);

/* Same as handleRedis command, but set flags for the concurrent context */
int ConcurrentSearch_HandleRedisCommandEx(int poolType, ConcurrentCmdHandler handler,
                                          RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                          ConcurrentSearchHandlerCtx *handlerCtx);

/********************************************* for debugging **********************************/

int ConcurrentSearch_isPaused();

int ConcurrentSearch_pause();

int ConcurrentSearch_resume();

thpool_stats ConcurrentSearch_getStats();

#ifdef __cplusplus
}
#endif
#endif // RS_CONCERRNT_CTX_
