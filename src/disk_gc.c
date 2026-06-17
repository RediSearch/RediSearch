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
#include "util/timeout.h"
#include <stdatomic.h>
#include <time.h>

// Fold one completed cycle's results into the cumulative per-index counters.
// Every path that runs a disk GC cycle must funnel its (stats, elapsed) pair
// through here so the accounting stays identical regardless of how the cycle
// was triggered — background tick, forced invoke, or an enterprise/RoR
// scheduler that drives compaction on its own. Keeping the per-cycle numbers
// (bytes, docs, time) in one place is also what lets FT.INFO/INFO report a
// single coherent view.
static void accumulateCycleStats(DiskGC *gc, const DiskGCRunStats *stats, long elapsed_ms) {
  if (elapsed_ms < 0) elapsed_ms = 0;
  atomic_fetch_add(&gc->totalCollectedBytes, stats->bytes_collected);
  atomic_fetch_add(&gc->totalCycles, (size_t)1);
  atomic_fetch_add(&gc->totalTimeMs, (size_t)elapsed_ms);
  atomic_store(&gc->lastRunTimeMs, (size_t)elapsed_ms);
  IndexsGlobalStats_DecreaseLogicallyDeleted(stats->num_cleaned_docs);
}

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

  struct timespec start, end;
  clock_gettime(CLOCK_MONOTONIC, &start);
  DiskGCRunStats stats = {0};
  SearchDisk_RunGC(sp->diskSpec, &stats);
  clock_gettime(CLOCK_MONOTONIC, &end);

  struct timespec elapsed;
  rs_timersub(&end, &start, &elapsed);
  long elapsed_ms = (long)rs_timer_ms(&elapsed);

  accumulateCycleStats(gc, &stats, elapsed_ms);

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

static void statsCb(RedisModule_Reply *reply, void *gcCtx) {
#define REPLY_KVNUM(k, v) RedisModule_ReplyKV_Double(reply, (k), (v))
  DiskGC *gc = gcCtx;
  RS_LOG_ASSERT(gc, "DiskGC stats callback invoked with NULL context");
  ssize_t bytes = atomic_load(&gc->totalCollectedBytes);
  size_t total_ms = atomic_load(&gc->totalTimeMs);
  size_t cycles = atomic_load(&gc->totalCycles);
  size_t last_ms = atomic_load(&gc->lastRunTimeMs);
  REPLY_KVNUM("bytes_collected", (double)bytes);
  REPLY_KVNUM("total_ms_run", (double)total_ms);
  REPLY_KVNUM("total_cycles", (double)cycles);
  REPLY_KVNUM("average_cycle_time_ms", cycles ? (double)total_ms / (double)cycles : 0.0);
  REPLY_KVNUM("last_run_time_ms", (double)last_ms);
#undef REPLY_KVNUM
}

static void statsForInfoCb(RedisModuleInfoCtx *ctx, void *gcCtx) {
  DiskGC *gc = gcCtx;
  RS_LOG_ASSERT(gc, "DiskGC stats callback invoked with NULL context");
  ssize_t bytes = atomic_load(&gc->totalCollectedBytes);
  size_t total_ms = atomic_load(&gc->totalTimeMs);
  size_t cycles = atomic_load(&gc->totalCycles);
  size_t last_ms = atomic_load(&gc->lastRunTimeMs);
  RedisModule_InfoBeginDictField(ctx, "gc_stats");
  RedisModule_InfoAddFieldLongLong(ctx, "bytes_collected", bytes);
  RedisModule_InfoAddFieldLongLong(ctx, "total_ms_run", total_ms);
  RedisModule_InfoAddFieldLongLong(ctx, "total_cycles", cycles);
  RedisModule_InfoAddFieldDouble(ctx, "average_cycle_time_ms",
                                 cycles ? (double)total_ms / (double)cycles : 0.0);
  RedisModule_InfoAddFieldDouble(ctx, "last_run_time_ms", (double)last_ms);
  RedisModule_InfoEndDictField(ctx);
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

static void getStatsCb(void *gcCtx, InfoGCStats *out) {
  const DiskGC *gc = gcCtx;
  out->totalCollectedBytes = atomic_load(&gc->totalCollectedBytes);
  out->totalCycles = atomic_load(&gc->totalCycles);
  out->totalTime = atomic_load(&gc->totalTimeMs);
  out->lastRunTimeMs = (long long)atomic_load(&gc->lastRunTimeMs);
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
