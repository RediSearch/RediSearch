/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "thread_cycle.h"

#include <stdatomic.h>

#include "config.h"
#include "index_gc_ffi.h"
#include "info/global_stats.h"
#include "spec.h"
#include "time_sample.h"
#include "util/references.h"
#include "info/info_redis/threads/current_thread.h"
#include "vector_index.h"

bool ThreadCycle_Run(IndexGC *gc, GCForcedRun *forced, StrongRef spec_ref) {
  TimeSample ts;
  TimeSampler_Start(&ts);
  // The driver records each of its own promotions in this thread's crash-report slot, so free
  // the slot `spec_ref` filled; the reference itself keeps the spec alive to the end, even if it
  // is dropped meanwhile.
  CurrentThread_ClearIndexSpec();

  gc->retryIntervalMs = GC_IntervalSecToMs(RSGlobalConfig.gcConfigParams.gcSettings.threadGcRunIntervalSec);
  gc->cleanNumericEmptyNodes = RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes;

  IndexSpec_AssertLockNotHeld();  // the driver takes the spec locks itself
  // The scanners' deltas address blocks by position, so two cycles on one index must never
  // interleave; the scheduler's per-index GC_SCHED_RUN_PENDING keeps them apart.
  uint64_t pruned = 0;
  enum ThreadGCResult rv = ThreadGC_RunScanners(gc, &pruned);
  if (rv == THREAD_GC_FAILED && forced) {
    forced->outcome = GC_FORCED_RUN_FAILED;
  }
  bool gcrv = rv != THREAD_GC_SPEC_DELETED;
  if (gcrv) {
    gcrv = VecSim_CallTieredIndexesGC(gc->index);
  }

  // Every pruned ID was counted when it was deleted, possibly into the spec's deferred count; take
  // that first, so the subtraction never outruns the count. Deletions defer under the write lock,
  // so the deferred count covers every ID the cycle's copy of the set held.
  IndexGC_TakeDeferredDeletes(gc, StrongRef_Get(spec_ref));
  StrongRef_Release(spec_ref);
  RS_LOG_ASSERT(pruned <= gc->deletedOrUpdatedDocsFromLastRun, "pruned more IDs than were counted");
  atomic_fetch_sub(&gc->deletedOrUpdatedDocsFromLastRun, pruned);
  IndexsGlobalStats_DecreaseLogicallyDeleted(pruned);
  TimeSampler_End(&ts);
  long long msRun = TimeSampler_DurationMS(&ts);

  gc->stats.numCycles++;
  gc->stats.totalMSRun += msRun;
  gc->stats.lastRunTimeMs = msRun;

  return gcrv;
}
