#ifndef RS_GARBAGE_COLLECTOR_H_
#define RS_GARBAGE_COLLECTOR_H_

#include "redismodule.h"
#include "rmutil/periodic.h"
#include "gc.h"

// the maximum frequency we are allowed to run in
#define GC_MAX_HZ 100
#define GC_MIN_HZ 1
#define GC_DEFAULT_HZ 10

#define NUM_CYCLES_HISTORY 10

typedef struct {
  // total bytes collected by the GC
  size_t totalCollected;
  // number of cycle ran
  size_t numCycles;
  // the number of cycles that collected anything
  size_t effectiveCycles;

  // the collection result of the last N cycles.
  // this is a cyclical buffer
  size_t history[NUM_CYCLES_HISTORY];
  // the offset in the history cyclical buffer
  int historyOffset;
} GCStats;

typedef struct GarbageCollectorCtx GarbageCollectorCtx;

/* Create a new garbage collector, with a string for the index name, and initial frequency */
GarbageCollectorCtx* NewGarbageCollector(const RedisModuleString *k, float initial_hz, uint64_t spec_unique_id, GCCallbacks* callbacks);

// called externally when the user deletes a document to hint at increasing the HZ
void GC_OnDelete(void *ctx);

void GC_OnTerm(void *privdata);

/* Render the GC stats to a redis connection, used by FT.INFO */
void GC_RenderStats(RedisModuleCtx *ctx, void *gc);

int GC_PeriodicCallback(RedisModuleCtx *ctx, void *privdata);

struct timespec GC_GetInterval(void *ctx);

#endif
