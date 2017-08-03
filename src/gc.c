#define RS_GC_C_

#include <math.h>
#include <assert.h>
#include <sys/param.h>
#include "inverted_index.h"
#include "redis_index.h"
#include "spec.h"
#include "redismodule.h"

#include "gc.h"

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

  RedisSearchCtx *sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName);
  if (!sctx) {
    printf("No index spec for GC %s", RedisModule_StringPtrLen(gc->keyName, NULL));
    return;
  }
  char *term = IndexSpec_GetRandomTerm(sctx->spec, 10);
  if (!term) {
    // printf("No term for GC to inspect\n");
    return;
  }
  RedisModule_Log(ctx, "info", "Garbage collecting for term '%s'", term);

  InvertedIndex *idx = Redis_OpenInvertedIndex(sctx, term, strlen(term), 1);
  size_t totalRemoved = 0;
  size_t totalCollected = 0;
  if (idx) {
    // printf("Garbage collecting for term %s\n", term);
    int blockNum = 0;
    int num = 10;
    size_t bytesCollected = 0;
    size_t recordsRemoved = 0;

    do {
      blockNum = InvertedIndex_Repair(idx, &sctx->spec->docs, blockNum, num, &bytesCollected,
                                      &recordsRemoved);
      totalRemoved += recordsRemoved;
      sctx->spec->stats.numRecords -= recordsRemoved;
      sctx->spec->stats.invertedSize -= bytesCollected;
      gc->stats.totalCollected += bytesCollected;
      totalCollected += bytesCollected;

      // 0 means error or we've finished
      if (!blockNum) break;
      // yield execution
      RedisModule_ThreadSafeContextUnlock(ctx);
      RedisModule_ThreadSafeContextLock(ctx);
      // TODO: Close key
      idx = Redis_OpenInvertedIndex(sctx, term, 1, 1);
    } while (idx != NULL);
  }

  if (totalRemoved) {
    RedisModule_Log(ctx, "notice", "Garbage collected %zd bytes in %zd records for term '%s'",
                    totalCollected, totalRemoved, term);
  }

  free(term);
  gc->stats.numCycles++;
  gc->stats.effectiveCycles += totalRemoved > 0 ? 1 : 0;

  // if we didn't remove anything - reduce the frequency a bit.
  // if we did  - increase the frequency a bit

  if (totalRemoved > 0) {
    gc->hz = MIN(gc->hz * 1.05, GC_MAX_HZ);
  } else {
    gc->hz = MAX(gc->hz * 0.99, GC_MIN_HZ);
  }

  RMUtilTimer_SetInterval(gc->timer, hzToTimeSpec(gc->hz));
  RedisModule_Log(ctx, "debug", "New HZ: %f\n", gc->hz);
}

// Start the collector thread
int GC_Start(GarbageCollectorCtx *ctx) {
  assert(ctx->timer == NULL);
  ctx->timer = RMUtil_NewPeriodicTimer(GC_PeriodicCallback, ctx, hzToTimeSpec(ctx->hz));
  return REDISMODULE_OK;
}

int GC_Stop(GarbageCollectorCtx *ctx) {
  if (ctx->timer) {
    RMUtilTimer_Stop(ctx->timer);
    RMUtilTimer_Free(ctx->timer);
    ctx->timer = NULL;
  }
  return REDISMODULE_OK;
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
  ctx->hz = MIN(ctx->hz * 1.5, GC_MAX_HZ);
}

void GC_RenderStats(RedisModuleCtx *ctx, GarbageCollectorCtx *gc) {
#define REPLY_KVNUM(n, k, v)                   \
  RedisModule_ReplyWithSimpleString(ctx, k);   \
  RedisModule_ReplyWithDouble(ctx, (double)v); \
  n += 2

  int n = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  REPLY_KVNUM(n, "current_hz", gc->hz);
  REPLY_KVNUM(n, "bytes_collected", gc->stats.totalCollected);
  REPLY_KVNUM(
      n, "effectiv_cycles_rate",
      (double)gc->stats.effectiveCycles / (double)(gc->stats.numCycles ? gc->stats.numCycles : 1));
  RedisModule_ReplySetArrayLength(ctx, n);
}