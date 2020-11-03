
#pragma once

#include "gc.h"
#include "search_ctx.h"
#include "redismodule.h"
#include "rmutil/periodic.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// the maximum frequency we are allowed to run in
#define GC_MAX_HZ 100
#define GC_MIN_HZ 1
#define GC_DEFAULT_HZ 10

#define NUM_CYCLES_HISTORY 10

//---------------------------------------------------------------------------------------------

struct GCStats {
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
};

//---------------------------------------------------------------------------------------------

struct NumericRangeTree;

struct NumericFieldGC :public Object {
  NumericFieldGC(NumericRangeTree *rt);
  ~NumericFieldGC();

  struct NumericRangeTree *rt;
  uint32_t revisionId;
  struct NumericRangeTreeIterator *gcIterator;
};

//---------------------------------------------------------------------------------------------

// Internal definition of the garbage collector context (each index has one)

struct GarbageCollector : public Object, public GCAPI {
  // current frequency
  float hz;

  // inverted index key name for reopening the index
  const RedisModuleString *keyName;

  // statistics for reporting
  GCStats stats;

  // flag for rdb loading. Set to 1 initially, but unce it's set to 0 we don't need to check anymore
  int rdbPossiblyLoading;

  arrayof(NumericFieldGC*) numericGC;

  uint64_t specUniqueId;

  bool noLockMode;

  // Create a new garbage collector, with a string for the index name, and initial frequency
  GarbageCollector(const RedisModuleString *k, float initial_hz, uint64_t spec_unique_id);

  void updateStats(RedisSearchCtx *sctx, size_t recordsRemoved, size_t bytesCollected);

  size_t CollectRandomTerm(RedisModuleCtx *ctx, int *status);
  size_t CollectNumericIndex(RedisModuleCtx *ctx, int *status);
  size_t CollectTagIndex(RedisModuleCtx *ctx, int *status);

  void FreeNumericGCArray();

  //-------------------------------------------------------------------------------------------

  virtual int PeriodicCallback(RedisModuleCtx* ctx);
  virtual void RenderStats(RedisModuleCtx* ctx);
  virtual void OnDelete(); // called when the user deletes a document to hint at increasing the HZ
  virtual void OnTerm();
  virtual struct timespec GetInterval();
};

///////////////////////////////////////////////////////////////////////////////////////////////
