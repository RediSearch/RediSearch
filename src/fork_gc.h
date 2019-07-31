
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

typedef enum FGCType { FGC_TYPE_INKEYSPACE, FGC_TYPE_NOKEYSPACE, FGC_TYPE_FREED } FGCType;

/* Internal definition of the garbage collector context (each index has one) */
typedef struct ForkGC {

  // inverted index key name for reopening the index
  union {
    const RedisModuleString *keyName;
    IndexSpec *sp;
  };
  FGCType type;

  uint64_t specUniqueId;

  // statistics for reporting
  ForkGCStats stats;

  // flag for rdb loading. Set to 1 initially, but unce it's set to 0 we don't need to check anymore
  int rdbPossiblyLoading;

  int pipefd[2];

} ForkGC;

ForkGC *FGC_New(const RedisModuleString *k, uint64_t specUniqueId, GCCallbacks *callbacks);
ForkGC *FGC_NewFromSpec(IndexSpec *sp, uint64_t specUniqueId, GCCallbacks *callbacks);

#endif /* SRC_FORK_GC_H_ */
