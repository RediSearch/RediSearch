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
#include "util/minmax.h"
#include "util/references.h"
#include "vector_index.h"

bool ThreadCycle_Run(IndexGC *gc, StrongRef spec_ref) {
  TimeSample ts;
  TimeSampler_Start(&ts);
  IndexSpecRef_Release(spec_ref);

  // Unlike a fork's snapshot, the scan sees deletes that land mid-cycle and may collect them while
  // they still count toward the next cycle's threshold: at worst an early next cycle.
  size_t num_docs_to_clean = atomic_exchange(&gc->deletedOrUpdatedDocsFromLastRun, 0);
  gc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;
  gc->cleanNumericEmptyNodes = RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes;

  IndexSpec_AssertLockNotHeld();  // the driver takes the spec locks itself
  RS_LOG_ASSERT(!gc->threadCycleInFlight, "two thread GC cycles on one index at once");
  gc->threadCycleInFlight = true;
  uint64_t retained = 0;
  enum ThreadGCResult rv = ThreadGC_RunScanners(gc, &retained);
  gc->threadCycleInFlight = false;
  bool gcrv = rv != THREAD_GC_SPEC_DELETED;
  if (gcrv) {
    gcrv = VecSim_CallTieredIndexesGC(gc->index);
  }

  // IDs the cycle left in the deleted set count toward the next cycle's threshold again, as a fork
  // cycle's denied blocks are found again; a failed cycle left every one.
  size_t requeued = 0;
  if (rv == THREAD_GC_DONE) {
    requeued = MIN(retained, num_docs_to_clean);
  } else if (gcrv) {
    requeued = num_docs_to_clean;
  }
  atomic_fetch_add(&gc->deletedOrUpdatedDocsFromLastRun, requeued);
  IndexsGlobalStats_DecreaseLogicallyDeleted(num_docs_to_clean - requeued);
  TimeSampler_End(&ts);
  long long msRun = TimeSampler_DurationMS(&ts);

  gc->stats.numCycles++;
  gc->stats.totalMSRun += msRun;
  gc->stats.lastRunTimeMs = msRun;

  return gcrv;
}
