#include "concurrent_ctx.h"
#include "dep/thpool/thpool.h"
#include <unistd.h>

static threadpool ConcurrentSearchThreadPool = NULL;
static threadpool ConcurrentIndexThreadPool = NULL;

/** Start the concurrent search thread pool. Should be called when initializing the module */
void ConcurrentSearch_ThreadPoolStart() {
  if (ConcurrentSearchThreadPool == NULL) {
    ConcurrentSearchThreadPool = thpool_init(CONCURRENT_SEARCH_POOL_SIZE);
    long numProcs = sysconf(_SC_NPROCESSORS_ONLN);
    if (numProcs < 1) {
      numProcs = CONCURRENT_INDEX_POOL_SIZE;
    }
    ConcurrentIndexThreadPool = thpool_init(numProcs);
  }
}

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type) {
  threadpool p =
      type == CONCURRENT_POOL_INDEX ? ConcurrentIndexThreadPool : ConcurrentSearchThreadPool;
  thpool_add_work(p, func, arg);
}

static void ConcurrentSearch_CloseKeys(ConcurrentSearchCtx *ctx) {
  size_t sz = ctx->numOpenKeys;
  for (size_t i = 0; i < sz; i++) {
    if (ctx->openKeys[i].key) {
      RedisModule_CloseKey(ctx->openKeys[i].key);
    }
  }
}

static void ConcurrentSearch_ReopenKeys(ConcurrentSearchCtx *ctx) {
  size_t sz = ctx->numOpenKeys;
  for (size_t i = 0; i < sz; i++) {
    ConcurrentKeyCtx *kx = &ctx->openKeys[i];
    kx->key = RedisModule_OpenKey(ctx->ctx, kx->keyName, kx->keyFlags);
    // if the key is marked as shared, make sure it isn't now
    kx->opts &= ~ConcurrentKey_SharedKey;
    kx->cb(kx->key, kx->privdata);
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
  if (!rctx) {
    ctx->ctx = NULL;
  }
  ctx->ctx = rctx;
  ctx->isLocked = 0;
  ctx->numOpenKeys = 0;
  ctx->openKeys = NULL;
  ConcurrentSearchCtx_ResetClock(ctx);
}

void ConcurrentSearchCtx_InitEx(ConcurrentSearchCtx *ctx, int mode, ConcurrentReopenCallback cb) {
  ctx->ctx = NULL;
  ctx->isLocked = 0;
  ctx->numOpenKeys = 1;
  ctx->openKeys = calloc(1, sizeof(*ctx->openKeys));
  ctx->openKeys->cb = cb;
  ctx->openKeys->keyFlags = mode;
}

void ConcurrentSearchCtx_Free(ConcurrentSearchCtx *ctx) {
  // Release the monitored open keys
  for (size_t i = 0; i < ctx->numOpenKeys; i++) {

    if (ctx->isLocked && ctx->openKeys[i].key &&
        // if this is a shared key, don't do anything
        !(ctx->openKeys[i].opts & ConcurrentKey_SharedKey)) {
      RedisModule_CloseKey(ctx->openKeys[i].key);
    }
    // If the key name is a shared string, don't do anything
    if (!(ctx->openKeys[i].opts & ConcurrentKey_SharedKeyString)) {
      RedisModule_FreeString(ctx->ctx, ctx->openKeys[i].keyName);
    }

    // free the private data if needed
    if (ctx->openKeys[i].freePrivData) {
      ctx->openKeys[i].freePrivData(ctx->openKeys[i].privdata);
    }
  }
  free(ctx->openKeys);
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
void ConcurrentSearch_AddKey(ConcurrentSearchCtx *ctx, RedisModuleKey *key, int openFlags,
                             RedisModuleString *keyName, ConcurrentReopenCallback cb,
                             void *privdata, void (*freePrivDataCallback)(void *),
                             ConcurrentKeyOptions opts) {
  ctx->numOpenKeys++;
  ctx->openKeys = realloc(ctx->openKeys, ctx->numOpenKeys * sizeof(ConcurrentKeyCtx));
  ctx->openKeys[ctx->numOpenKeys - 1] = (ConcurrentKeyCtx){.key = key,
                                                           .keyName = keyName,
                                                           .keyFlags = openFlags,
                                                           .cb = cb,
                                                           .privdata = privdata,
                                                           .freePrivData = freePrivDataCallback,
                                                           .opts = opts};
}

void ConcurrentSearchCtx_Lock(ConcurrentSearchCtx *ctx) {
  RedisModule_ThreadSafeContextLock(ctx->ctx);
  ctx->isLocked = 1;
  ConcurrentSearch_ReopenKeys(ctx);
}

void ConcurrentSearchCtx_Unlock(ConcurrentSearchCtx *ctx) {
  ConcurrentSearch_CloseKeys(ctx);
  RedisModule_ThreadSafeContextUnlock(ctx->ctx);
  ctx->isLocked = 0;
}