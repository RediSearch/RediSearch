#define RS_GC_C_

#include <math.h>
#include <assert.h>
#include <sys/param.h>
#include "inverted_index.h"
#include "redis_index.h"
#include "spec.h"
#include "redismodule.h"
#include "rmutil/util.h"
#include "gc.h"
#include "tests/time_sample.h"

// convert a frequency to timespec
struct timespec hzToTimeSpec(float hz) {
  struct timespec ret;
  ret.tv_sec = (time_t)floor(1.0 / hz);
  ret.tv_nsec = (long)floor(1000000000.0 / hz) % 1000000000L;
  return ret;
}

/* Internal definition of the garbage collector context (each index has one) */
typedef struct GarbageCollectorCtx {

  // current frequency
  float hz;

  // inverted index key name for reopening the index
  const RedisModuleString *keyName;

  // periodic timer
  struct RMUtilTimer *timer;

  // statistics for reporting
  GCStats stats;

  // flag for rdb loading. Set to 1 initially, but unce it's set to 0 we don't need to check anymore
  int rdbPossiblyLoading;

} GarbageCollectorCtx;

/* Create a new garbage collector, with a string for the index name, and initial frequency */
GarbageCollectorCtx *NewGarbageCollector(const RedisModuleString *k, float initialHZ) {
  GarbageCollectorCtx *gc = malloc(sizeof(*gc));

  *gc = (GarbageCollectorCtx){
      .timer = NULL, .hz = initialHZ, .keyName = k, .stats = {}, .rdbPossiblyLoading = 1,
  };
  return gc;
}

/* Check if Redis is currently loading from RDB. Our thread starts before RDB loading is finished */
int isRdbLoading(RedisModuleCtx *ctx) {
  long long isLoading = 0;
  RMUtilInfo *info = RMUtil_GetRedisInfo(ctx);
  if (!info) {
    return 0;
  }

  if (!RMUtilInfo_GetInt(info, "loading", &isLoading)) {
    isLoading = 0;
  }

  RMUtilRedisInfo_Free(info);
  return isLoading == 1;
}

/* The GC periodic callback, called in a separate thread. It selects a random term (using weighted
 * random) */
static void gc_periodicCallback(RedisModuleCtx *ctx, void *privdata) {

  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NULL;
  RedisModule_AutoMemory(ctx);
  RedisModule_ThreadSafeContextLock(ctx);
  GarbageCollectorCtx *gc = privdata;
  assert(gc);

  // Check if RDB is loading - not needed after the first time we find out that rdb is not reloading
  if (gc->rdbPossiblyLoading) {
    if (isRdbLoading(ctx)) {
      RedisModule_Log(ctx, "notice", "RDB Loading in progress, not performing GC");
      goto end;
    } else {
      // the RDB will not load again, so it's safe to ignore the info check in the next cycles
      gc->rdbPossiblyLoading = 0;
    }
  }

  sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName);
  if (!sctx) {
    RedisModule_Log(ctx, "warning", "No index spec for GC %s",
                    RedisModule_StringPtrLen(gc->keyName, NULL));
    goto end;
  }

  // Select a weighted random term
  TimeSample ts;
  char *term = IndexSpec_GetRandomTerm(sctx->spec, 20);

  // if the index is empty we won't get anything here
  if (!term) {
    goto end;
  }
  RedisModule_Log(ctx, "debug", "Garbage collecting for term '%s'", term);

  // Open the term's index
  InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);
  size_t totalRemoved = 0;
  size_t totalCollected = 0;
  if (idx) {
    int blockNum = 0;
    int num = 100;

    do {
      size_t bytesCollected = 0;
      size_t recordsRemoved = 0;

      TimeSampler_Start(&ts);
      // repair 100 blocks at once
      blockNum = InvertedIndex_Repair(idx, &sctx->spec->docs, blockNum, num, &bytesCollected,
                                      &recordsRemoved);
      TimeSampler_End(&ts);
      RedisModule_Log(ctx, "debug", "Repair took %lldns", TimeSampler_DurationNS(&ts));

      /// update the statistics with the the number of records deleted
      totalRemoved += recordsRemoved;
      sctx->spec->stats.numRecords -= recordsRemoved;
      sctx->spec->stats.invertedSize -= bytesCollected;
      gc->stats.totalCollected += bytesCollected;
      totalCollected += bytesCollected;

      // blockNum 0 means error or we've finished
      if (!blockNum) break;

      // After each iteration we yield execution
      // First we close the relevant keys we're touching
      RedisModule_CloseKey(sctx->key);
      RedisModule_CloseKey(idxKey);
      SearchCtx_Free(sctx);

      // now release the global lock
      RedisModule_ThreadSafeContextUnlock(ctx);

      // try to acquire it again...
      RedisModule_ThreadSafeContextLock(ctx);

      // reopen the context - it might have gone away!
      sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName);
      // sctx null --> means it was deleted and we need to stop right now
      if (!sctx) break;
      // reopen the inverted index - it might have gone away
      idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);
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
  if (gc->timer) {  // the timer is NULL if we've been cancelled
    if (totalRemoved > 0) {
      gc->hz = MIN(gc->hz * 1.2, GC_MAX_HZ);
    } else {
      gc->hz = MAX(gc->hz * 0.99, GC_MIN_HZ);
    }

    RMUtilTimer_SetInterval(gc->timer, hzToTimeSpec(gc->hz));
  }

  RedisModule_Log(ctx, "debug", "New HZ: %f\n", gc->hz);

end:
  if (sctx) {
    RedisModule_CloseKey(sctx->key);
    SearchCtx_Free(sctx);
  }
  if (idxKey) RedisModule_CloseKey(idxKey);
  RedisModule_ThreadSafeContextUnlock(ctx);
}

/* Termination callback for the GC. Called after we stop, and frees up all the resources. */
static void gc_onTerm(void *privdata) {
  GarbageCollectorCtx *gc = privdata;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModule_ThreadSafeContextLock(ctx);
  RedisModule_FreeString(ctx, (RedisModuleString *)gc->keyName);
  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_FreeThreadSafeContext(ctx);
  free(gc);
}

// Start the collector thread
int GC_Start(GarbageCollectorCtx *ctx) {
  assert(ctx->timer == NULL);
  ctx->timer = RMUtil_NewPeriodicTimer(gc_periodicCallback, gc_onTerm, ctx, hzToTimeSpec(ctx->hz));
  return REDISMODULE_OK;
}

/* Stop the garbage collector, and call its termination function asynchronously when its thread is
 * finished. This also frees the resources allocated for the GC context */
int GC_Stop(GarbageCollectorCtx *ctx) {
  if (ctx->timer) {
    RMUtilTimer_Terminate(ctx->timer);
    // set the timer to NULL so we won't call this twice
    ctx->timer = NULL;
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

// get the current stats from the collector
const GCStats *GC_GetStats(GarbageCollectorCtx *ctx) {
  if (!ctx) return NULL;
  return &ctx->stats;
}

// called externally when the user deletes a document to hint at increasing the HZ
void GC_OnDelete(GarbageCollectorCtx *ctx) {
  if (!ctx) return;
  ctx->hz = MIN(ctx->hz * 1.5, GC_MAX_HZ);
}

void GC_RenderStats(RedisModuleCtx *ctx, GarbageCollectorCtx *gc) {
#define REPLY_KVNUM(n, k, v)                   \
  RedisModule_ReplyWithSimpleString(ctx, k);   \
  RedisModule_ReplyWithDouble(ctx, (double)v); \
  n += 2

  int n = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  if (gc) {
    REPLY_KVNUM(n, "current_hz", gc->hz);
    REPLY_KVNUM(n, "bytes_collected", gc->stats.totalCollected);
    REPLY_KVNUM(n, "effectiv_cycles_rate",
                (double)gc->stats.effectiveCycles /
                    (double)(gc->stats.numCycles ? gc->stats.numCycles : 1));
  }
  RedisModule_ReplySetArrayLength(ctx, n);
}