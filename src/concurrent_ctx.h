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
#include "config.h"
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

/* Return the underlying thread pool for direct submission. */
redisearch_thpool_t *ConcurrentSearch_GetPool(int type);

/* return number of currently working threads */
size_t ConcurrentSearchPool_WorkingThreadCount();

/* return number of pending high priority jobs */
size_t ConcurrentSearchPool_HighPriorityPendingJobsCount();

struct ConcurrentCmdCtx;
typedef void (*ConcurrentCmdHandler)(RedisModuleCtx *, RedisModuleString **, int,
                                     struct ConcurrentCmdCtx *);

// Context for concurrent search handler
// Contains additional parameters passed to ConcurrentSearch_HandleRedisCommandEx
struct BlockedRequestCtx;  // Forward declaration
struct Cursor;             // Forward declaration

// Context for blocking client
typedef struct ConcurrentSearchBlockClientCtx {
  RedisModuleCmdFunc reply_callback;      // Callback when UnblockClient is called (FAIL policy)
  RedisModuleCmdFunc timeout_callback;    // Callback when timeout fires (FAIL policy)
  rs_wall_clock_ms_t timeoutMS;           // Timeout value in milliseconds (0 if no timeout)
  // Wrapper owning the request executed by this command. Allocated on the main
  // thread before blocking; becomes the blocked client's privdata (freed via
  // BlockedRequestCtx_OnFree). ConcurrentSearch_HandleRedisCommandEx runs
  // BlockedRequestCtx_BeginCycle on it right after blocking the client.
  struct BlockedRequestCtx *brc;
  // Timeout policy captured on the main thread at dispatch (avoids a TOCTOU
  // against a concurrent FT.CONFIG SET); recorded on the cycle by BeginCycle.
  RSTimeoutPolicy timeoutPolicy;
} ConcurrentSearchBlockClientCtx;

typedef struct ConcurrentSearchHandlerCtx {
  rs_wall_clock_ns_t coordStartTime;  // Time when command was received on coordinator
  rs_wall_clock_ns_t coordQueueTime;  // Time spent waiting in coordinator thread pool queue
  WeakRef spec_ref;                   // Weak reference to the index spec
  bool isProfile;                     // Whether this is an FT.PROFILE command
  size_t numShards;                   // Number of shards in the cluster (captured from main thread)
  // Cursor taken for execution on the main thread (coord FT.CURSOR READ with a
  // blocking policy); NULL otherwise. The BG handler reads it back via
  // ConcurrentCmdCtx_GetCursor instead of re-taking by id.
  // TRANSITIONAL(MOD-16691): folds into the wrapper's per-cycle cursor state
  // once the cursor-ownership step lands.
  struct Cursor *cursor;
  ConcurrentSearchBlockClientCtx bcCtx; // Context for blocking client
} ConcurrentSearchHandlerCtx;

// Initialize a ConcurrentSearchHandlerCtx to zero
static inline void ConcurrentSearchHandlerCtx_Init(ConcurrentSearchHandlerCtx *ctx) {
  memset(ctx, 0, sizeof(*ctx));
}

#define CMDCTX_KEEP_RCTX 0x01
#define CMDCTX_KEEP_BC   0x02

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

/**
 * Take ownership of the BlockedClient. After calling this, the handler is
 * responsible for calling RedisModule_BlockedClientMeasureTimeEnd and
 * RedisModule_UnblockClient itself (typically from a continuation). The
 * default behavior (without this call) is for threadHandleCommand to unblock
 * the client immediately after the handler returns.
 */
void ConcurrentCmdCtx_KeepBlockedClient(struct ConcurrentCmdCtx *ctx);

// Returns the WeakRef held in the context.
WeakRef ConcurrentCmdCtx_GetWeakRef(struct ConcurrentCmdCtx *cctx);

WeakRef ConcurrentCmdCtx_TakeWeakRef(struct ConcurrentCmdCtx *cctx);

// Returns the coordinator start time held in the context.
rs_wall_clock_ns_t ConcurrentCmdCtx_GetCoordStartTime(struct ConcurrentCmdCtx *cctx);

// Returns the number of shards captured from the main thread.
size_t ConcurrentCmdCtx_GetNumShards(const struct ConcurrentCmdCtx *cctx);

// Returns the blocked client held in the context.
RedisModuleBlockedClient *ConcurrentCmdCtx_GetBlockedClient(struct ConcurrentCmdCtx *cctx);

// Returns the thread pool ID the command was dispatched on.
int ConcurrentCmdCtx_GetPoolId(const struct ConcurrentCmdCtx *cctx);

/* Cursor taken for execution on the main thread at dispatch (coord FT.CURSOR
 * READ with a blocking policy); NULL otherwise. */
struct Cursor *ConcurrentCmdCtx_GetCursor(const struct ConcurrentCmdCtx *cctx);

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
