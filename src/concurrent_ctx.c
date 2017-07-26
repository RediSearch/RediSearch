#include "concurrent_ctx.h"
#include "dep/thpool/thpool.h"

threadpool ConcurrentSearchThreadPool = NULL;

/** Start the concurrent search thread pool. Should be called when initializing the module */
void ConcurrentSearch_ThreadPoolStart() {
  if (ConcurrentSearchThreadPool == NULL) {
    ConcurrentSearchThreadPool = thpool_init(CONCURRENT_SEARCH_POOL_SIZE);
  }
}

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg) {
  thpool_add_work(ConcurrentSearchThreadPool, func, arg);
}

void ConcurrentSearch_CloseKeys(ConcurrentSearchCtx *ctx) {
  size_t sz = ctx->numOpenKeys;
  for (size_t i = 0; i < sz; i++) {
    RedisModule_CloseKey(ctx->openKeys[i].key);
  }
}

void ConcurrentSearch_ReopenKeys(ConcurrentSearchCtx *ctx) {
  size_t sz = ctx->numOpenKeys;
  for (size_t i = 0; i < sz; i++) {
    ConcurrentKeyCtx *kx = &ctx->openKeys[i];
    kx->key = RedisModule_OpenKey(ctx->ctx, kx->keyName, kx->keyFlags);
    kx->cb(kx->key, kx->ctx);
  }
}

/** Check the elapsed timer, and release the lock if enough time has passed */
inline void ConcurrentSearch_CheckTimer(ConcurrentSearchCtx *ctx) {
  static struct timespec now;
  clock_gettime(CLOCK_MONOTONIC_RAW, &now);

  long long durationNS = (long long)1000000000 * (now.tv_sec - ctx->lastTime.tv_sec) +
                         (now.tv_nsec - ctx->lastTime.tv_nsec);

  // Timeout - release the thread safe context lock and let other threads run as well
  if (durationNS > CONCURRENT_TIMEOUT_NS) {
    ConcurrentSearch_CloseKeys(ctx);
    RedisModule_ThreadSafeContextUnlock(ctx->ctx);

    // Right after releasing, we try to acquire the lock again.
    // If other threads are waiting on it, the kernel will decide which one
    // will get the chance to run again. Calling sched_yield is not necessary here.
    // See http://blog.firetree.net/2005/06/22/thread-yield-after-mutex-unlock/
    RedisModule_ThreadSafeContextLock(ctx->ctx);
    ConcurrentSearch_ReopenKeys(ctx);
    // Right after re-acquiring the lock, we sample the current time.
    // This will be used to calculate the elapsed running time
    clock_gettime(CLOCK_MONOTONIC_RAW, &ctx->lastTime);
    ctx->ticker = 0;
  }
}

/** Initialize a concurrent context */
void ConcurrentSearchCtx_Init(RedisModuleCtx *rctx, ConcurrentSearchCtx *ctx) {
  if (!rctx) {
    ctx->ctx = NULL;
  }
  ctx->ctx = rctx;
  ctx->ticker = 0;
  ctx->numOpenKeys = 0;
  ctx->openKeys = NULL;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ctx->lastTime);
}

void ConcurrentSearchCtx_Free(ConcurrentSearchCtx *ctx) {
  for (size_t i = 0; i < ctx->numOpenKeys; i++) {
    RedisModule_CloseKey(ctx->openKeys[i].key);
    RedisModule_FreeString(ctx->ctx, ctx->openKeys[i].keyName);
  }
  free(ctx->openKeys);
}

void ConcurrentSearch_AddKey(ConcurrentSearchCtx *ctx, RedisModuleKey *key, int openFlags,
                             RedisModuleString *keyName, ConcurrentReopenCallback cb,
                             void *privdata) {
  ctx->numOpenKeys++;
  ctx->openKeys = realloc(ctx->openKeys, ctx->numOpenKeys * sizeof(ConcurrentKeyCtx));
  ctx->openKeys[ctx->numOpenKeys - 1] = (ConcurrentKeyCtx){
      .key = key, .keyName = keyName, .keyFlags = openFlags, .cb = cb, .ctx = privdata};
}