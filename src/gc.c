#define RS_GC_C_
#include "gc.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "spec.h"

// convert a frequency to timespec
struct timespec hzToTimeSpec(float hz) {
  struct timespec ret;
  ret.tv_sec = (time_t)floor(1.0 / hz);
  ret.tv_nsec = (long)floor(1000000000.0 / hz) % 1000000000L;
  return ret;
}

typedef struct GarbageCollectorCtx {

  // current frequency
  float hz;

  // inverted index key name for reopening the index
  const RedisModuleString *keyName;

  // periodic timer
  struct RMUtilTimer *timer;

  GCStats stats;

} GarbageCollectorCtx;

GarbageCollectorCtx *NewGarbageCollector(const RedisModuleString *k, float initialHZ) {
  GarbageCollectorCtx *gc = malloc(sizeof(*gc));

  *gc = (GarbageCollectorCtx){
      .timer = NULL, .hz = initialHZ, .keyName = k, .stats = {},
  };
  return gc;
}

void GC_PeriodicCallback(RedisModuleCtx *ctx, void *privdata) {

  GarbageCollectorCtx *gc = privdata;
  assert(gc);

  RedisSearchCtx *sctx = NewSearchCtx(ctx, gc->keyName);
  if (!sctx) {
    RedisModule_Log(ctx, "notice", "No index spec for GC");
  }

  const char *term = IndexSpec_GetRandomTerm(sctx->spec);
  if (!term) {
    RedisModule_Log(ctx, "notice", "No term for GC to inspect");
  }
  InvertedIndex *idx = Redis_OpenInvertedIndexEx(ctx, term, 1, NULL);

  if (idx) {
    int blockNum = 0;
    int num = 10;
    size_t bytesCollected = 0;
    do {
      blockNum = InvertedIndex_Repair(idx, &sctx->spec->docs, blockNum, -1, &bytesCollected);
      if (!blockNum) break;
      // yield execution
      RedisModule_ThreadSafeCtxUnlock(ctx);
      RedisModule_ThreadSafeCtxLock(ctx);
      // TODO: Close key
      idx = Redis_OpenInvertedIndexEx(ctx, term, 1, NULL);
    } while (idx != NULL);
  }
}

// Start the collector thread
int GC_Start(GarbageCollectorCtx *ctx) {
  assert(ctx->timer == NULL);
  ctx->timer = RMUtil_NewPeriodicTimer(GC_PeriodicCallback, ctx, hzToTimeSpec(ctx->hz));
}

int GC_Stop(GarbageCollectorCtx *ctx) {
  if (ctx->timer) {
    RMUtilTimer_Stop(ctx->timer);
    RMUtilTimer_Free(ctx->timer);
    ctx->timer = NULL;
  }
}

void GC_Free(GarbageCollectorCtx *ctx) {
  if (ctx->timer) {
    GC_Stop(ctx);
  }
  free(ctx);
}

// get the current stats from the collector
GCStats *GC_GetStats(GarbageCollectorCtx *ctx) {
  return &ctx->stats;
}

// called externally when the user deletes a document to hint at increasing the HZ
void GC_OnDelete(GarbageCollectorCtx *ctx) {
  ctx->hz = MIN(ctx->hz + 1, GC_MAX_HZ);
}