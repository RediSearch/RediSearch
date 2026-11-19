/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef SRC_DISK_GC_H_
#define SRC_DISK_GC_H_

#include "redismodule.h"
#include "gc.h"
#include "util/references.h"

#ifdef __cplusplus
#include <atomic>
#define RS_Atomic(T) std::atomic<T>
extern "C" {
#else
#define RS_Atomic(T) _Atomic(T)
#include <stdatomic.h>
#endif

/* Internal definition of the disk GC context (each disk index has one). */
typedef struct DiskGC {
  WeakRef index;
  RS_Atomic(size_t) intervalSec;
  // Tracks only writes since last GC run (no updates)
  RS_Atomic(size_t) writesFromLastRun;
  // Delete notifications used to decide when another cycle is eligible.
  RS_Atomic(size_t) deletesFromLastRun;
  // Update notifications used to decide when another cycle is eligible.
  RS_Atomic(size_t) updatesFromLastRun;
  // Eligibility counters reset per attempt; this contribution survives until the
  // corresponding IDs are reclaimed or the collector is destroyed.
  RS_Atomic(size_t) logicallyDeleted;

  // Cumulative bytes freed across all GC attempts on this index.
  // Signed to match `InfoGCStats::totalCollectedBytes` / fork-GC semantics
  // (some compaction strategies may transiently allocate more than they free).
  RS_Atomic(ssize_t) totalCollectedBytes;
  // Number of GC attempts that returned, including retry outcomes.
  RS_Atomic(size_t) totalCycles;
  // Sum of wall-clock attempt durations in milliseconds.
  RS_Atomic(size_t) totalTimeMs;
  // Wall-clock duration of the most recent attempt, in milliseconds.
  RS_Atomic(size_t) lastRunTimeMs;

  // Bumped on every document write, update and delete, slot-trim deletes included. The vector
  // storage is compacted at most once per epoch.
  RS_Atomic(uint64_t) vectorEpoch;
  // Touched only by the periodic callback, which never overlaps itself for one index: the epoch
  // the vector storage was last compacted at (or found not to need it), and the epoch at which
  // every vector field was last seen idle (0 once one is seen busy).
  uint64_t compactedVectorEpoch;
  uint64_t idleVectorEpoch;
  // Vector storage compactions that ran to completion.
  RS_Atomic(size_t) vectorCompactions;
} DiskGC;

DiskGC *DiskGC_Create(StrongRef spec_ref, GCCallbacks *callbacks);

// Take the run lock exclusively and disable disk GC, waiting out any in-flight run.
// Returns with the run lock HELD (write mode) so the caller can close disk indexes and
// clear each sp->diskSpec while no run is executing and none can start.
void DiskGC_LockRunsAndDisable(void);

// Release the run lock taken by DiskGC_LockRunsAndDisable().
void DiskGC_UnlockRuns(void);

// Destroy the module-global disk GC lock (a readers-writer lock). Must be called only on
// full module teardown, after the GC thread pool has been destroyed so no GC thread can
// still use the lock.
void DiskGC_Cleanup(void);

#ifdef __cplusplus
}
#endif

#endif /* SRC_DISK_GC_H_ */
