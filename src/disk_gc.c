/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "disk_gc.h"

#include <stdatomic.h>          // for atomic_fetch_add, atomic_fetch_sub
#include <time.h>               // for size_t, timespec
#include <stdbool.h>            // for true, bool, false

#include "config.h"             // for GCConfig, GCSettings, RSConfig, ...
#include "spec.h"               // for IndexSpecRef_Release, IndexSpec, ...
#include "search_disk.h"        // for SearchDisk_RunGC
#include "redismodule.h"        // for RedisModuleInfoCtx
#include "rmalloc.h"            // for rm_calloc, rm_free
#include "info/global_stats.h"
#include "reply.h"              // for RedisModule_Reply
#include "rmutil/rm_assert.h"   // for RS_LOG_ASSERT

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

  // Check total changes (deletes + adds + updates) to decide whether to run GC
  size_t num_writes = atomic_load(&gc->writesFromLastRun);
  size_t num_deletes = atomic_load(&gc->deletesFromLastRun);
  size_t num_updates = atomic_load(&gc->updatesFromLastRun);
  size_t num_changes = num_writes + num_deletes + num_updates;
  if (!force && num_changes < RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold) {
    IndexSpecRef_Release(spec_ref);
    return true;
  }
  // Reset counters before running GC
  atomic_fetch_sub(&gc->writesFromLastRun, num_writes);
  atomic_fetch_sub(&gc->deletesFromLastRun, num_deletes);
  atomic_fetch_sub(&gc->updatesFromLastRun, num_updates);

  size_t num_cleaned = SearchDisk_RunGC(sp->diskSpec, sp);
  IndexsGlobalStats_DecreaseLogicallyDeleted(num_cleaned);

  gc->intervalSec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;

  IndexSpecRef_Release(spec_ref);
  return true;
}

static void onTerminateCb(void *privdata) {
  DiskGC *gc = privdata;
  size_t updates = atomic_exchange(&gc->updatesFromLastRun, 0);
  size_t deletes = atomic_exchange(&gc->deletesFromLastRun, 0);
  IndexsGlobalStats_DecreaseLogicallyDeleted(updates + deletes);
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
  atomic_fetch_add(&gc->deletesFromLastRun, 1);
  IndexsGlobalStats_IncreaseLogicallyDeleted(1);
}

static void updateCb(void *ctx) {
  DiskGC *gc = ctx;
  atomic_fetch_add(&gc->updatesFromLastRun, 1);
  IndexsGlobalStats_IncreaseLogicallyDeleted(1);
}

static void writeCb(void *ctx) {
  DiskGC *gc = ctx;
  atomic_fetch_add(&gc->writesFromLastRun, 1);
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
  RS_LOG_ASSERT(SearchDisk_IsInitialized(), "Search Disk is not initialized");
  DiskGC *gc = rm_calloc(1, sizeof(*gc));
  *gc = (DiskGC){
      .index = StrongRef_Demote(spec_ref),
      .writesFromLastRun = 0,
      .deletesFromLastRun = 0,
      .updatesFromLastRun = 0,
  };
  gc->intervalSec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;

  callbacks->onTerm = onTerminateCb;
  callbacks->periodicCallback = periodicCb;
  callbacks->renderStats = statsCb;
  callbacks->renderStatsForInfo = statsForInfoCb;
  callbacks->getInterval = getIntervalCb;
  callbacks->onDelete = deleteCb;
  callbacks->onWrite = writeCb;
  callbacks->onUpdate = updateCb;
  callbacks->getStats = getStatsCb;

  return gc;
}
