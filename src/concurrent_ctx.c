/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "concurrent_ctx.h"
#include "thpool/thpool.h"
#include <util/arr.h>
#include "rmutil/rm_assert.h"
#include "module.h"
#include "util/logging.h"

static arrayof(redisearch_thpool_t *) threadpools_g = NULL;

int ConcurrentSearch_CreatePool(int numThreads) {
  if (!threadpools_g) {
    threadpools_g = array_new(redisearch_thpool_t *, 1); // Only used by the coordinator, so 1 is enough
  }
  int poolId = array_len(threadpools_g);
  array_append(threadpools_g, redisearch_thpool_create(numThreads, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD,
                                                                       LogCallback, "coord"));

  return poolId;
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
  WeakRef spec_ref;
} ConcurrentCmdCtx;

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type) {
  redisearch_thpool_t *p = threadpools_g[type];
  redisearch_thpool_add_work(p, func, arg, THPOOL_PRIORITY_HIGH);
}

/* return number of currently working threads */
size_t ConcurrentSearchPool_WorkingThreadCount() {
  RS_ASSERT(threadpools_g);
  // Assert we only have 1 pool
  RS_LOG_ASSERT(array_len(threadpools_g) == 1, "assuming 1 ConcurrentSearch pool");
  return redisearch_thpool_num_jobs_in_progress(threadpools_g[0]);
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

  RedisModule_BlockedClientMeasureTimeEnd(ctx->bc);

  RedisModule_UnblockClient(ctx->bc, NULL);
  rm_free(ctx->argv);
  rm_free(p);
}

void ConcurrentCmdCtx_KeepRedisCtx(ConcurrentCmdCtx *cctx) {
  cctx->options |= CMDCTX_KEEP_RCTX;
}

WeakRef ConcurrentCmdCtx_GetWeakRef(ConcurrentCmdCtx *cctx) {
  return cctx->spec_ref;
}

int ConcurrentSearch_HandleRedisCommandEx(int poolType, int options, ConcurrentCmdHandler handler,
                                          RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                          WeakRef spec_ref) {
  ConcurrentCmdCtx *cmdCtx = rm_malloc(sizeof(*cmdCtx));

  cmdCtx->bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
  cmdCtx->argc = argc;
  cmdCtx->spec_ref = spec_ref;
  cmdCtx->ctx = RedisModule_GetThreadSafeContext(cmdCtx->bc);
  RS_AutoMemory(cmdCtx->ctx);
  cmdCtx->handler = handler;
  cmdCtx->options = options;
  // Copy command arguments so they can be released by the calling thread
  cmdCtx->argv = rm_calloc(argc, sizeof(RedisModuleString *));
  for (int i = 0; i < argc; i++) {
    cmdCtx->argv[i] = RedisModule_CreateStringFromString(cmdCtx->ctx, argv[i]);
  }

  RedisModule_BlockedClientMeasureTimeStart(cmdCtx->bc);

  ConcurrentSearch_ThreadPoolRun(threadHandleCommand, cmdCtx, poolType);
  return REDISMODULE_OK;
}

int ConcurrentSearch_HandleRedisCommand(int poolType, ConcurrentCmdHandler handler,
                                        RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return ConcurrentSearch_HandleRedisCommandEx(poolType, 0, handler, ctx, argv, argc, (WeakRef){0});
}

void ConcurrentSearchCtx_ReopenKeys(ConcurrentSearchCtx *ctx) {
  size_t sz = ctx->numOpenKeys;
  for (size_t i = 0; i < sz; i++) {
    ConcurrentKeyCtx *kx = &ctx->openKeys[i];
    // if the key is marked as shared, make sure it isn't now
    kx->cb(kx->privdata);
  }
}

/** Initialize a concurrent context */
void ConcurrentSearchCtx_Init(RedisModuleCtx *rctx, ConcurrentSearchCtx *ctx) {
  ctx->ctx = rctx;
  ctx->numOpenKeys = 0;
  ctx->openKeys = NULL;
}

void ConcurrentSearchCtx_InitSingle(ConcurrentSearchCtx *ctx, RedisModuleCtx *rctx, ConcurrentReopenCallback cb) {
  ctx->ctx = rctx;
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
