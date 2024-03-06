/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "concurrent_ctx.h"
#include "thpool/thpool.h"
#include <unistd.h>
#include <util/arr.h>
#include "rmutil/rm_assert.h"
#include "util/logging.h"

static arrayof(redisearch_threadpool) threadpools_g = NULL;

int CONCURRENT_POOL_INDEX = -1;
int CONCURRENT_POOL_SEARCH = -1;

int ConcurrentSearch_CreatePool(int numThreads) {
  if (!threadpools_g) {
    threadpools_g = array_new(redisearch_threadpool, 4);
  }
  int poolId = array_len(threadpools_g);
  threadpools_g = array_append(threadpools_g, redisearch_thpool_create(numThreads, DEFAULT_PRIVILEGED_THREADS_NUM));
  redisearch_thpool_init(threadpools_g[poolId], LogCallback);

  return poolId;
}

/** Start the concurrent search thread pool. Should be called when initializing the module */
void ConcurrentSearch_ThreadPoolStart() {

  if (CONCURRENT_POOL_SEARCH == -1) {
    CONCURRENT_POOL_SEARCH = ConcurrentSearch_CreatePool(RSGlobalConfig.searchPoolSize);
    long numProcs = 0;

    if (!RSGlobalConfig.poolSizeNoAuto) {
      numProcs = sysconf(_SC_NPROCESSORS_ONLN);
    }

    if (numProcs < 1) {
      numProcs = RSGlobalConfig.indexPoolSize;
    }
    CONCURRENT_POOL_INDEX = ConcurrentSearch_CreatePool(numProcs);
  }
}

/** Stop all the concurrent threads */
void ConcurrentSearch_ThreadPoolDestroy(void) {
  if (!threadpools_g) {
    return;
  }
  for (size_t ii = 0; ii < array_len(threadpools_g); ++ii) {
    redisearch_thpool_destroy(threadpools_g[ii]);
  }
  array_free(threadpools_g);
  threadpools_g = NULL;
}

typedef struct ConcurrentCmdCtx {
  RedisModuleBlockedClient *bc;
  RedisModuleCtx *ctx;
  ConcurrentCmdHandler handler;
  RedisModuleString **argv;
  int argc;
  int options;
} ConcurrentCmdCtx;

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type) {
  redisearch_threadpool p = threadpools_g[type];
  redisearch_thpool_add_work(p, func, arg, THPOOL_PRIORITY_HIGH);
}

static void threadHandleCommand(void *p) {
  ConcurrentCmdCtx *ctx = p;
  // Lock GIL if needed
  if (!(ctx->options & CMDCTX_NO_GIL)) {
    RedisModule_ThreadSafeContextLock(ctx->ctx);
  }

  ctx->handler(ctx->ctx, ctx->argv, ctx->argc, ctx);

  // Unlock GIL if needed
  if (!(ctx->options & CMDCTX_NO_GIL)) {
    RedisModule_ThreadSafeContextUnlock(ctx->ctx);
  }

  if (!(ctx->options & CMDCTX_KEEP_RCTX)) {
    RedisModule_FreeThreadSafeContext(ctx->ctx);
  }

  RS_CHECK_FUNC(RedisModule_BlockedClientMeasureTimeEnd, ctx->bc);

  RedisModule_UnblockClient(ctx->bc, NULL);
  rm_free(ctx->argv);
  rm_free(p);
}

void ConcurrentCmdCtx_KeepRedisCtx(ConcurrentCmdCtx *cctx) {
  cctx->options |= CMDCTX_KEEP_RCTX;
}

// Used by RSCordinator
int ConcurrentSearch_HandleRedisCommandEx(int poolType, int options, ConcurrentCmdHandler handler,
                                          RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  ConcurrentCmdCtx *cmdCtx = rm_malloc(sizeof(*cmdCtx));
  cmdCtx->bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
  cmdCtx->argc = argc;
  cmdCtx->ctx = RedisModule_GetThreadSafeContext(cmdCtx->bc);
  RS_AutoMemory(cmdCtx->ctx);
  cmdCtx->handler = handler;
  cmdCtx->options = options;
  // Copy command arguments so they can be released by the calling thread
  cmdCtx->argv = rm_calloc(argc, sizeof(RedisModuleString *));
  for (int i = 0; i < argc; i++) {
    cmdCtx->argv[i] = RedisModule_CreateStringFromString(cmdCtx->ctx, argv[i]);
  }

  RS_CHECK_FUNC(RedisModule_BlockedClientMeasureTimeStart, cmdCtx->bc);

  ConcurrentSearch_ThreadPoolRun(threadHandleCommand, cmdCtx, poolType);
  return REDISMODULE_OK;
}

int ConcurrentSearch_HandleRedisCommand(int poolType, ConcurrentCmdHandler handler,
                                        RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return ConcurrentSearch_HandleRedisCommandEx(poolType, 0, handler, ctx, argv, argc);
}

void ConcurrentSearchCtx_ReopenKeys(ConcurrentSearchCtx *ctx) {
  size_t sz = ctx->numOpenKeys;
  for (size_t i = 0; i < sz; i++) {
    ConcurrentKeyCtx *kx = &ctx->openKeys[i];
    // if the key is marked as shared, make sure it isn't now
    kx->cb(kx->privdata);
  }
}

/** Check the elapsed timer, and release the lock if enough time has passed */
int ConcurrentSearch_CheckTimer(ConcurrentSearchCtx *ctx) {
  static struct timespec now;
  clock_gettime(CLOCK_MONOTONIC_RAW, &now);

  long long durationNS = (long long)1000000000 * (now.tv_sec - ctx->lastTime.tv_sec) +
                         (now.tv_nsec - ctx->lastTime.tv_nsec);
  // Timeout - release the thread safe context lock and let other threads run as well
  if (durationNS > CONCURRENT_TIMEOUT_NS) {
    ConcurrentSearchCtx_Unlock(ctx);

    // Right after releasing, we try to acquire the lock again.
    // If other threads are waiting on it, the kernel will decide which one
    // will get the chance to run again. Calling sched_yield is not necessary here.
    // See http://blog.firetree.net/2005/06/22/thread-yield-after-mutex-unlock/
    ConcurrentSearchCtx_Lock(ctx);
    // Right after re-acquiring the lock, we sample the current time.
    // This will be used to calculate the elapsed running time
    ConcurrentSearchCtx_ResetClock(ctx);
    return 1;
  }
  return 0;
}

void ConcurrentSearchCtx_ResetClock(ConcurrentSearchCtx *ctx) {
  clock_gettime(CLOCK_MONOTONIC_RAW, &ctx->lastTime);
  ctx->ticker = 0;
}

/** Initialize a concurrent context */
void ConcurrentSearchCtx_Init(RedisModuleCtx *rctx, ConcurrentSearchCtx *ctx) {
  ctx->ctx = rctx;
  ctx->isLocked = 0;
  ctx->numOpenKeys = 0;
  ctx->openKeys = NULL;
  ConcurrentSearchCtx_ResetClock(ctx);
}

void ConcurrentSearchCtx_InitSingle(ConcurrentSearchCtx *ctx, RedisModuleCtx *rctx, ConcurrentReopenCallback cb) {
  ctx->ctx = rctx;
  ctx->isLocked = 0;
  ctx->numOpenKeys = 1;
  ctx->openKeys = rm_calloc(1, sizeof(*ctx->openKeys));
  ctx->openKeys->cb = cb;
}

void ConcurrentSearchCtx_Free(ConcurrentSearchCtx *ctx) {
  // Release the monitored open keys
  for (size_t i = 0; i < ctx->numOpenKeys; i++) {
    ConcurrentKeyCtx *cctx = ctx->openKeys + i;

    // free the private data if needed
    if (cctx->freePrivData) {
      cctx->freePrivData(cctx->privdata);
    }
  }

  rm_free(ctx->openKeys);
  ctx->numOpenKeys = 0;
}

/* Add a "monitored" key to the context. When keys are open during concurrent execution, they need
 * to be closed before we yield execution and release the GIL, and reopened when we get back the
 * execution context.
 * To simplify this, each place in the program that holds a reference to a redis key
 * based data, registers itself and the key to be automatically reopened.
 *
 * After reopening, a callback
 * is being called to notify the key holder that it has been reopened, and handle the consequences.
 * This is used by index iterators to avoid holding reference to deleted keys or changed data.
 *
 * We register the key, the flags to reopen it, a string holding its name for reopening, a callback
 * for notification, and private callback data. if freePrivDataCallback is provided, we will call it
 * when the context is freed to release the private data. If NULL is passed, we do nothing */
void ConcurrentSearch_AddKey(ConcurrentSearchCtx *ctx, ConcurrentReopenCallback cb,
                             void *privdata, void (*freePrivDataCallback)(void *)) {
  ctx->numOpenKeys++;
  ctx->openKeys = rm_realloc(ctx->openKeys, ctx->numOpenKeys * sizeof(ConcurrentKeyCtx));
  ctx->openKeys[ctx->numOpenKeys - 1] = (ConcurrentKeyCtx){.cb = cb,
                                                           .privdata = privdata,
                                                           .freePrivData = freePrivDataCallback};
}

void ConcurrentSearchCtx_Lock(ConcurrentSearchCtx *ctx) {
  RS_LOG_ASSERT(!ctx->isLocked, "Redis GIL shouldn't be locked");
  RedisModule_ThreadSafeContextLock(ctx->ctx);
  ctx->isLocked = 1;
  ConcurrentSearchCtx_ReopenKeys(ctx);
}

void ConcurrentSearchCtx_Unlock(ConcurrentSearchCtx *ctx) {
  RedisModule_ThreadSafeContextUnlock(ctx->ctx);
  ctx->isLocked = 0;
}
