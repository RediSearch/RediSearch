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
#include "vector_index.h"

bool ThreadCycle_Run(IndexGC *gc, StrongRef spec_ref) {
  TimeSample ts;
  TimeSampler_Start(&ts);

  gc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;
  gc->cleanNumericEmptyNodes = RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes;

  IndexSpec_AssertLockNotHeld();  // the driver takes the spec locks itself
  RS_LOG_ASSERT(!gc->threadCycleInFlight, "two thread GC cycles on one index at once");
  gc->threadCycleInFlight = true;
  uint64_t pruned = 0;
  enum ThreadGCResult rv = ThreadGC_runScanners(gc, &pruned);
  gc->threadCycleInFlight = false;
  bool gcrv = rv != THREAD_GC_SPEC_DELETED;
  if (gcrv) {
    gcrv = VecSim_CallTieredIndexesGC(gc->index);
  }

  // Every pruned ID was counted when it was deleted, possibly into the spec's deferred count; take
  // that first, so the subtraction never outruns the count. Deletions defer under the write lock,
  // so the deferred count covers every ID the cycle's copy of the set held.
  IndexGC_TakeDeferredDeletes(gc, StrongRef_Get(spec_ref));
  IndexSpecRef_Release(spec_ref);
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
