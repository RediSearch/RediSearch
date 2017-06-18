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

/** Check the elapsed timer, and release the lock if enough time has passed */
inline void ConcurrentSearch_CheckTimer(ConcurrentSearchCtx *ctx) {
  static struct timespec now;
  clock_gettime(CLOCK_MONOTONIC_RAW, &now);

  long long durationNS = (long long)1000000000 * (now.tv_sec - ctx->lastTime.tv_sec) +
                         (now.tv_nsec - ctx->lastTime.tv_nsec);

  // Timeout - release the thread safe context lock and let other threads run as well
  if (durationNS > CONCURRENT_TIMEOUT_NS) {
    RedisModule_ThreadSafeContextUnlock(ctx->ctx);

    // Right after releasing, we try to acquire the lock again.
    // If other threads are waiting on it, the kernel will decide which one
    // will get the chance to run again. Calling sched_yield is not necessary here.
    // See http://blog.firetree.net/2005/06/22/thread-yield-after-mutex-unlock/
    RedisModule_ThreadSafeContextLock(ctx->ctx);

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
  clock_gettime(CLOCK_MONOTONIC_RAW, &ctx->lastTime);
}
