/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef SRC_FORK_GC_H_
#define SRC_FORK_GC_H_

#include "redismodule.h"
#include "gc.h"
#include "VecSim/vec_sim.h"
#include <poll.h>

#ifdef __cplusplus
#include <atomic>
#define RS_Atomic(T) std::atomic<T>
extern "C" {
#else
#define RS_Atomic(T) _Atomic(T)
#include <stdatomic.h>
#endif

typedef struct {
  // total bytes collected by the GC
  // This is signed because block splitting (when deltas are too big) can cause more bytes to be
  // allocated by the GC than the number of bytes collected.
  ssize_t totalCollected;
  // number of cycle ran
  size_t numCycles;

  long long totalMSRun;
  long long lastRunTimeMs;

  uint64_t gcNumericNodesMissed;
  uint64_t gcBlocksDenied;
} ForkGCStats;

// Optional hook for tests. beforeApply is called in the parent after fork(),
// letting tests inject mutations invisible to the child before results are applied.
typedef struct {
  void (*beforeApply)(void *privdata);
  void *privdata;
} FGCHook;

/* Internal definition of the garbage collector context (each index has one) */
typedef struct ForkGC {

  // owner of the gc
  WeakRef index;

  RedisModuleCtx *ctx;

  // statistics for reporting
  ForkGCStats stats;

  int pipe_read_fd;
  int pipe_write_fd;
  struct pollfd pollfd_read[1]; // pollfd to poll the read pipe so that we don't block while read

  struct timespec retryInterval;
  RS_Atomic(size_t) deletedOrUpdatedDocsFromLastRun;

  // current value of RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes
  // This value is updated during the periodic callback execution.
  int cleanNumericEmptyNodes;
} ForkGC;

ForkGC *FGC_Create(StrongRef spec_ref, GCCallbacks *callbacks);

// Run one full GC cycle. In production, called via the GCCallbacks with hooks=NULL.
// Tests call this directly with hooks to inject mutations at specific lifecycle points.
bool FGC_RunCycle(ForkGC *gc, bool force, const FGCHook *hook);

#ifdef __cplusplus
}
#endif

#endif /* SRC_FORK_GC_H_ */
