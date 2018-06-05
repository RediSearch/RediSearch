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
#include "numeric_index.h"

// convert a frequency to timespec
struct timespec hzToTimeSpec(float hz) {
  struct timespec ret;
  ret.tv_sec = (time_t)floor(1.0 / hz);
  ret.tv_nsec = (long)floor(1000000000.0 / hz) % 1000000000L;
  return ret;
}

typedef struct NumericFieldGCCtx {
  NumericRangeTree *rt;
  uint32_t revisionId;
  NumericRangeTreeIterator *gcIterator;
} NumericFieldGCCtx;

#define NUMERIC_GC_INITIAL_SIZE 4

#define SPEC_STATUS_OK 1
#define SPEC_STATUS_INVALID 2

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

  NumericFieldGCCtx **numericGCCtx;

  uint64_t spec_unique_id;

} GarbageCollectorCtx;

/* Create a new garbage collector, with a string for the index name, and initial frequency */
GarbageCollectorCtx *NewGarbageCollector(const RedisModuleString *k, float initialHZ,
                                         uint64_t spec_unique_id) {
  GarbageCollectorCtx *gc = malloc(sizeof(*gc));

  *gc = (GarbageCollectorCtx){
      .timer = NULL,
      .hz = initialHZ,
      .keyName = k,
      .stats = {},
      .rdbPossiblyLoading = 1,
  };

  gc->numericGCCtx = array_new(NumericFieldGCCtx *, NUMERIC_GC_INITIAL_SIZE);
  gc->spec_unique_id = spec_unique_id;

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

void gc_updateStats(RedisSearchCtx *sctx, GarbageCollectorCtx *gc, size_t recordsRemoved,
                    size_t bytesCollected) {
  sctx->spec->stats.numRecords -= recordsRemoved;
  sctx->spec->stats.invertedSize -= bytesCollected;
  gc->stats.totalCollected += bytesCollected;
}

size_t gc_RandomTerm(RedisModuleCtx *ctx, GarbageCollectorCtx *gc, int *status) {
  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName);
  size_t totalRemoved = 0;
  size_t totalCollected = 0;
  if (!sctx || sctx->spec->unique_id != gc->spec_unique_id) {
    RedisModule_Log(ctx, "warning", "No index spec for GC %s",
                    RedisModule_StringPtrLen(gc->keyName, NULL));
    *status = SPEC_STATUS_INVALID;
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
      gc_updateStats(sctx, gc, recordsRemoved, bytesCollected);
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
      if (!sctx || sctx->spec->unique_id != gc->spec_unique_id) {
        *status = SPEC_STATUS_INVALID;
        break;
      }

      // reopen the inverted index - it might have gone away
      idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);
    } while (idx != NULL);
  }
  if (totalRemoved) {
    RedisModule_Log(ctx, "notice", "Garbage collected %zd bytes in %zd records for term '%s'",
                    totalCollected, totalRemoved, term);
  }
  free(term);
  RedisModule_Log(ctx, "debug", "New HZ: %f\n", gc->hz);
end:
  if (sctx) {
    RedisModule_CloseKey(sctx->key);
    SearchCtx_Free(sctx);
  }
  if (idxKey) RedisModule_CloseKey(idxKey);

  return totalRemoved;
}

static NumericRangeNode *NextGcNode(NumericFieldGCCtx *numericGcCtx) {
  bool runFromStart = false;
  NumericRangeNode *node = NULL;
  do {
    while ((node = NumericRangeTreeIterator_Next(numericGcCtx->gcIterator))) {
      if (node->range) {
        return node;
      }
    }
    assert(!runFromStart);
    NumericRangeTreeIterator_Free(numericGcCtx->gcIterator);
    numericGcCtx->gcIterator = NumericRangeTreeIterator_New(numericGcCtx->rt);
    runFromStart = true;
  } while (true);

  // will never reach here
  return NULL;
}

static NumericFieldGCCtx *gc_NewNumericGcCtx(NumericRangeTree *rt) {
  NumericFieldGCCtx *ctx = rm_malloc(sizeof(NumericFieldGCCtx));
  ctx->rt = rt;
  ctx->revisionId = rt->revisionId;
  ctx->gcIterator = NumericRangeTreeIterator_New(rt);
  return ctx;
}

static void gc_FreeNumericGcCtx(NumericFieldGCCtx *ctx) {
  NumericRangeTreeIterator_Free(ctx->gcIterator);
  rm_free(ctx);
}

static void gc_FreeNumericGcCtxArray(GarbageCollectorCtx *gc) {
  for (int i = 0; i < array_len(gc->numericGCCtx); ++i) {
    gc_FreeNumericGcCtx(gc->numericGCCtx[i]);
  }
  array_trimm_len(gc->numericGCCtx, 0);
}

size_t gc_NumericIndex(RedisModuleCtx *ctx, GarbageCollectorCtx *gc, int *status) {
#define NUMERIC_FIELDS_ARRAY_CAP 2
  size_t bytesCollected = 0;
  size_t recordsRemoved = 0;
  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName);
  if (!sctx || sctx->spec->unique_id != gc->spec_unique_id) {
    RedisModule_Log(ctx, "warning", "No index spec for GC %s",
                    RedisModule_StringPtrLen(gc->keyName, NULL));
    *status = SPEC_STATUS_INVALID;
    goto end;
  }
  IndexSpec *spec = sctx->spec;
  // find all the numeric fields
  FieldSpec **numericFields = array_new(FieldSpec *, NUMERIC_FIELDS_ARRAY_CAP);
  for (int i = 0; i < spec->numFields; ++i) {
    if (spec->fields[i].type == FIELD_NUMERIC) {
      numericFields = array_append(numericFields, &(spec->fields[i]));
    }
  }

  if (array_len(numericFields) == 0) {
    goto end;
  }

  if (array_len(numericFields) != array_len(gc->numericGCCtx)) {
    // add all numeric fields to our gc
    assert(array_len(numericFields) >
           array_len(gc->numericGCCtx));  // it is not possible to remove fields
    gc_FreeNumericGcCtxArray(gc);
    for (int i = 0; i < array_len(numericFields); ++i) {
      NumericRangeTree *rt = OpenNumericIndex(sctx, numericFields[i]->name, &idxKey);
      // if we could not open the numeric field we probably have a
      // corruption in our data, better to know it now.
      assert(rt);
      gc->numericGCCtx = array_append(gc->numericGCCtx, gc_NewNumericGcCtx(rt));
      if (idxKey) RedisModule_CloseKey(idxKey);
    }
  }

  array_free(numericFields);

  // choose random numeric gc ctx
  int randomIndex = rand() % array_len(gc->numericGCCtx);
  NumericFieldGCCtx *numericGcCtx = gc->numericGCCtx[randomIndex];
  if (numericGcCtx->revisionId != numericGcCtx->rt->revisionId) {
    // revision changed, recreating our numeric gc ctx
    assert(numericGcCtx->revisionId < numericGcCtx->rt->revisionId);
    gc->numericGCCtx[randomIndex] = gc_NewNumericGcCtx(numericGcCtx->rt);
    gc_FreeNumericGcCtx(numericGcCtx);
    numericGcCtx = gc->numericGCCtx[randomIndex];
  }

  NumericRangeNode *nextNode = NextGcNode(numericGcCtx);
  InvertedIndex_Repair(nextNode->range->entries, &sctx->spec->docs, 0,
                       nextNode->range->entries->size, &bytesCollected, &recordsRemoved);

  gc_updateStats(sctx, gc, recordsRemoved, bytesCollected);

end:
  if (sctx) {
    RedisModule_CloseKey(sctx->key);
    SearchCtx_Free(sctx);
  }

  return recordsRemoved;
}

/* The GC periodic callback, called in a separate thread. It selects a random term (using weighted
 * random) */
static int gc_periodicCallback(RedisModuleCtx *ctx, void *privdata) {
  int status = SPEC_STATUS_OK;
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

  size_t totalRemoved = 0;

  totalRemoved += gc_RandomTerm(ctx, gc, &status);

  totalRemoved += gc_NumericIndex(ctx, gc, &status);

  gc->stats.numCycles++;
  gc->stats.effectiveCycles += totalRemoved > 0 ? 1 : 0;

  // if we didn't remove anything - reduce the frequency a bit.
  // if we did  - increase the frequency a bit
  if (gc->timer) {
    // the timer is NULL if we've been cancelled
    if (totalRemoved > 0) {
      gc->hz = MIN(gc->hz * 1.2, GC_MAX_HZ);
    } else {
      gc->hz = MAX(gc->hz * 0.99, GC_MIN_HZ);
    }
    RMUtilTimer_SetInterval(gc->timer, hzToTimeSpec(gc->hz));
  }

end:

  RedisModule_ThreadSafeContextUnlock(ctx);

  return status == SPEC_STATUS_OK;
}

/* Termination callback for the GC. Called after we stop, and frees up all the resources. */
static void gc_onTerm(void *privdata) {
  GarbageCollectorCtx *gc = privdata;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModule_ThreadSafeContextLock(ctx);
  RedisModule_FreeString(ctx, (RedisModuleString *)gc->keyName);
  for (int i = 0; i < array_len(gc->numericGCCtx); ++i) {
    gc_FreeNumericGcCtx(gc->numericGCCtx[i]);
  }
  array_free(gc->numericGCCtx);
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
