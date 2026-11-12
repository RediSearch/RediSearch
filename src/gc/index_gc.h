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
} IndexGCStats;

// Optional hook for tests. beforeApply is called in the parent after fork(),
// letting tests inject mutations invisible to the child before results are applied.
typedef struct {
  void (*beforeApply)(void *privdata);
  void *privdata;
} IndexGCHook;

// How the last scheduled cycle left the schedule, which decides the next timer. Only scheduled
// cycles change it: a forced run's fork result is its caller's, not the schedule's. Written by
// a scheduled cycle while it holds RUN_PENDING and read when arming on the main thread, which
// never happens while that bit is held.
typedef enum {
  INDEX_GC_SCHED_OK = 0,       // wait the run interval
  INDEX_GC_SCHED_FORK_FAILED,  // the last fork failed; retry after the retry interval
} IndexGCSchedState;

/* Internal definition of the garbage collector context (each index has one) */
typedef struct IndexGC {

  // owner of the gc
  WeakRef index;

  RedisModuleCtx *ctx;

  // statistics for reporting
  IndexGCStats stats;

  int pipe_read_fd;
  int pipe_write_fd;
  struct pollfd pollfd_read[1]; // pollfd to poll the read pipe so that we don't block while read
  // Read-ahead buffer for the parent's pipe reads: set by FGC_parentHandleFromChild for as long
  // as it reads, NULL otherwise.
  void *readBuffer;

  IndexGCSchedState schedState;
  RS_Atomic(size_t) deletedOrUpdatedDocsFromLastRun;

  // current value of RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes
  // This value is updated during the periodic callback execution.
  int cleanNumericEmptyNodes;
} IndexGC;

IndexGC *IndexGC_Create(StrongRef spec_ref, GCCallbacks *callbacks);

// Run one full GC cycle. In production, called via the GCCallbacks with hooks=NULL.
// Tests call this directly with hooks to inject mutations at specific lifecycle points.
bool IndexGC_RunCycle(IndexGC *gc, GCForcedRun *forced, const IndexGCHook *hook);

#ifdef __cplusplus
}
#endif

#endif /* SRC_FORK_GC_H_ */
