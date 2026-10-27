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

// Atomic: an in-process (THREAD) cycle updates them on the GC thread while the main thread reads
// them for FT.INFO and INFO.
typedef struct {
  // total bytes collected by the GC
  // This is signed because block splitting (when deltas are too big) can cause more bytes to be
  // allocated by the GC than the number of bytes collected.
  RS_Atomic(ssize_t) totalCollected;
  // number of cycle ran
  RS_Atomic(size_t) numCycles;

  RS_Atomic(long long) totalMSRun;
  RS_Atomic(long long) lastRunTimeMs;

  RS_Atomic(uint64_t) gcNumericNodesMissed;
  RS_Atomic(uint64_t) gcBlocksDenied;
} IndexGCStats;

// Optional hook for tests. beforeApply is called in the parent after fork(),
// letting tests inject mutations invisible to the child before results are applied.
typedef struct {
  void (*beforeApply)(void *privdata);
  void *privdata;
} IndexGCHook;

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

  struct timespec retryInterval;
  RS_Atomic(size_t) deletedOrUpdatedDocsFromLastRun;
  // Set while a thread cycle runs its scanners. Their deltas address blocks by position, so two
  // cycles on one index must never interleave; the GC scheduler guarantees it, this checks it.
  bool threadCycleInFlight;

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
