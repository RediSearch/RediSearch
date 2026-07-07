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
  // Tracks only deletes for global stats (no updates)
  RS_Atomic(size_t) deletesFromLastRun;
  // Tracks only updates for global stats (no pure writes or deletes)
  RS_Atomic(size_t) updatesFromLastRun;

  // Cumulative bytes freed across all GC cycles run on this index.
  // Signed to match `InfoGCStats::totalCollectedBytes` / fork-GC semantics
  // (some compaction strategies may transiently allocate more than they free).
  RS_Atomic(ssize_t) totalCollectedBytes;
  // Total number of GC cycles that have completed on this index.
  RS_Atomic(size_t) totalCycles;
  // Sum of wall-clock cycle durations in milliseconds.
  RS_Atomic(size_t) totalTimeMs;
  // Wall-clock duration of the most recent cycle, in milliseconds.
  RS_Atomic(size_t) lastRunTimeMs;
} DiskGC;

DiskGC *DiskGC_Create(StrongRef spec_ref, GCCallbacks *callbacks);

// Take the run lock and disable disk GC, waiting out any in-flight run. Returns with
// the run lock HELD so the caller can close disk indexes and clear each sp->diskSpec
// while no run is executing and none can start; periodicCb touches sp->diskSpec only
// under this lock. Called on the main thread during shutdown teardown.
// There is no matching re-enable. Must be paired with DiskGC_UnlockRuns().
void DiskGC_LockRunsAndDisable(void);

// Release the run lock taken by DiskGC_LockRunsAndDisable().
void DiskGC_UnlockRuns(void);

// Destroy the module-global disk GC lock. Must be called only on full module
// teardown, after the GC thread pool has been destroyed so no GC thread can still
// use the lock.
void DiskGC_Cleanup(void);

#ifdef __cplusplus
}
#endif

#endif /* SRC_DISK_GC_H_ */
