/*
 * fork_gc.h
 *
 *  Created on: Aug 16, 2018
 *      Author: meir
 */

#ifndef SRC_FORK_GC_H_
#define SRC_FORK_GC_H_

#include "redismodule.h"
#include "gc.h"

typedef struct {
  // total bytes collected by the GC
  size_t totalCollected;
  // number of cycle ran
  size_t numCycles;

  long long totalMSRun;
  long long lastRunTimeMs;

  uint64_t gcNumericNodesMissed;
  uint64_t gcBlocksDenied;
} ForkGCStats;

/* Internal definition of the garbage collector context (each index has one) */
typedef struct ForkGCCtx {

  // inverted index key name for reopening the index
  const RedisModuleString *keyName;

  uint64_t specUniqueId;

  // statistics for reporting
  ForkGCStats stats;

  // flag for rdb loading. Set to 1 initially, but unce it's set to 0 we don't need to check anymore
  int rdbPossiblyLoading;

  int pipefd[2];

} ForkGCCtx;

ForkGCCtx* NewForkGC(const RedisModuleString *k, uint64_t specUniqueId, GCCallbacks* callbacks);
void ForkGc_RenderStats(RedisModuleCtx *ctx, void *gcCtx);
void ForkGc_OnDelete(void *ctx);
void ForkGc_ForceInvoke(void *ctx, RedisModuleBlockedClient *bClient);
void ForkGc_OnTerm(void *privdata);
struct timespec ForkGc_GetInterval(void *ctx);

#endif /* SRC_FORK_GC_H_ */
