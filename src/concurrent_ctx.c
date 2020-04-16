#include "concurrent_ctx.h"
#include "dep/thpool/thpool.h"
#include <unistd.h>
#include <util/arr.h>
#include "rmutil/rm_assert.h"

static threadpool *threadpools_g = NULL;

int CONCURRENT_POOL_INDEX = -1;
int CONCURRENT_POOL_SEARCH = -1;

int ConcurrentSearch_CreatePool(int numThreads) {
  if (!threadpools_g) {
    threadpools_g = array_new(threadpool, 4);
  }
  int poolId = array_len(threadpools_g);
  threadpools_g = array_append(threadpools_g, thpool_init(numThreads));
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
    thpool_destroy(threadpools_g[ii]);
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
  threadpool p = threadpools_g[type];
  thpool_add_work(p, func, arg);
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
  RedisModule_AutoMemory(cmdCtx->ctx);
  cmdCtx->handler = handler;
  cmdCtx->options = options;
  // Copy command arguments so they can be released by the calling thread
  cmdCtx->argv = rm_calloc(argc, sizeof(RedisModuleString *));
  for (int i = 0; i < argc; i++) {
    cmdCtx->argv[i] = RedisModule_CreateStringFromString(cmdCtx->ctx, argv[i]);
  }

  ConcurrentSearch_ThreadPoolRun(threadHandleCommand, cmdCtx, poolType);
  return REDISMODULE_OK;
}

int ConcurrentSearch_HandleRedisCommand(int poolType, ConcurrentCmdHandler handler,
                                        RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return ConcurrentSearch_HandleRedisCommandEx(poolType, 0, handler, ctx, argv, argc);
}
