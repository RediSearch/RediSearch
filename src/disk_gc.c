/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "disk_gc.h"
#include "config.h"
#include "spec.h"
#include "search_disk.h"
#include "module.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "info/global_stats.h"
#include <stdatomic.h>
#include <time.h>

static bool periodicCb(void *privdata, bool force) {
  DiskGC *gc = privdata;
  StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    return false;
  }
  if (!sp->diskSpec) {
    IndexSpecRef_Release(spec_ref);
    return true;
  }

  size_t num_docs_to_clean = atomic_load(&gc->deletedDocsFromLastRun);
  if (!force && num_docs_to_clean < RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold) {
    IndexSpecRef_Release(spec_ref);
    return true;
  }

  size_t num_docs_cleaned = SearchDisk_RunGC(sp->diskSpec, sp);

  IndexsGlobalStats_DecreaseLogicallyDeleted(num_docs_cleaned);
  atomic_fetch_sub(&gc->deletedDocsFromLastRun, num_docs_cleaned);

  gc->intervalSec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;

  IndexSpecRef_Release(spec_ref);
  return true;
}

static void onTerminateCb(void *privdata) {
  DiskGC *gc = privdata;
  size_t remaining = atomic_exchange(&gc->deletedDocsFromLastRun, 0);
  IndexsGlobalStats_DecreaseLogicallyDeleted(remaining);
  WeakRef_Release(gc->index);
  rm_free(gc);
}

/* Stats are maintained in disk info; do not add anything here. */
static void statsCb(RedisModule_Reply *reply, void *gcCtx) {
  (void)reply;
  (void)gcCtx;
}

static void statsForInfoCb(RedisModuleInfoCtx *ctx, void *gcCtx) {
  (void)ctx;
  (void)gcCtx;
}

static void deleteCb(void *ctx) {
  DiskGC *gc = ctx;
  atomic_fetch_add(&gc->deletedDocsFromLastRun, 1);
  IndexsGlobalStats_IncreaseLogicallyDeleted(1);
}

// Stats are maintained in disk info.
static void getStatsCb(void *gcCtx, InfoGCStats *out) {
  (void)gcCtx;
  out->totalCollectedBytes = 0;
  out->totalCycles = 0;
  out->totalTime = 0;
  out->lastRunTimeMs = 0;
}

static struct timespec getIntervalCb(void *ctx) {
  const DiskGC *gc = ctx;
  return (struct timespec){ .tv_sec = gc->intervalSec, .tv_nsec = 0 };
}

DiskGC *DiskGC_Create(StrongRef spec_ref, GCCallbacks *callbacks) {
  RS_LOG_ASSERT(SearchDisk_IsEnabled(), "Disk GC is not enabled");
  DiskGC *gc = rm_calloc(1, sizeof(*gc));
  *gc = (DiskGC){
      .index = StrongRef_Demote(spec_ref),
      .deletedDocsFromLastRun = 0,
  };
  gc->intervalSec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;

  callbacks->onTerm = onTerminateCb;
  callbacks->periodicCallback = periodicCb;
  callbacks->renderStats = statsCb;
  callbacks->renderStatsForInfo = statsForInfoCb;
  callbacks->getInterval = getIntervalCb;
  callbacks->onDelete = deleteCb;
  callbacks->getStats = getStatsCb;

  return gc;
}
