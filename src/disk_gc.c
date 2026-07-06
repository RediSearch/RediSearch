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
#include <pthread.h>
#include <time.h>

// Explicit handshake between a disk GC run (background GC thread) and disk-index
// teardown (main thread, on shutdown). The disk GC pool has a single thread, so
// at most one run is ever in flight; a single global lock + flag is enough.
//
// periodicCb holds g_diskGcRunLock across the actual SearchDisk_RunGC call, so
// DiskGC_LockRunsAndDisable() (main thread) can wait for an in-flight run to finish
// before the diskSpec it is compacting is closed, and holds the lock across that close
// so clearing sp->diskSpec is serialised against periodicCb. g_diskGcDisabled stops any
// new run from starting once teardown has begun. run_gc never acquires the GIL
// (its compaction callbacks take the IndexSpec rwlock, not the ThreadSafeContext),
// so the shutdown thread may hold the GIL while it waits on this lock without
// deadlocking. There is no re-enable: the flag is only ever set on shutdown.
static pthread_mutex_t g_diskGcRunLock = PTHREAD_MUTEX_INITIALIZER;
static bool g_diskGcDisabled = false;

// Fold one completed cycle's results into the cumulative per-index counters.
// Every path that runs a disk GC cycle must funnel its `DiskGCRunStats` through
// here so the accounting stays identical regardless of how the cycle was
// triggered — background tick, forced invoke, or an enterprise/RoR scheduler
// that drives compaction on its own. All per-cycle numbers (bytes, docs, time)
// are populated by the disk implementation in `stats`, so this is also what
// lets FT.INFO/INFO report a single coherent view.
static void accumulateCycleStats(DiskGC *gc, const DiskGCRunStats *stats) {
  atomic_fetch_add(&gc->totalCollectedBytes, stats->bytes_collected);
  atomic_fetch_add(&gc->totalCycles, (size_t)1);
  atomic_fetch_add(&gc->totalTimeMs, stats->cycle_time_ms);
  atomic_store(&gc->lastRunTimeMs, stats->cycle_time_ms);
  IndexsGlobalStats_DecreaseLogicallyDeleted(stats->num_cleaned_docs);
}

static bool periodicCb(void *privdata, bool force) {
  DiskGC *gc = privdata;
  StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    return false;
  }

  // Hold g_diskGcRunLock across the whole check-and-run so shutdown teardown
  // (DiskGC_LockRunsAndDisable) waits for an in-flight run to finish and then closes
  // the diskSpec under the same lock, and g_diskGcDisabled stops a new run from starting
  // once teardown has begun. run_gc never takes the GIL (its compaction callbacks take
  // the IndexSpec rwlock), so the shutdown thread may hold the GIL while it waits here.
  pthread_mutex_lock(&g_diskGcRunLock);

  // Run GC only when teardown has not disabled it, the index is still disk-backed,
  // and enough changes accumulated since the last run (unless forced).
  size_t num_writes = atomic_load(&gc->writesFromLastRun);
  size_t num_deletes = atomic_load(&gc->deletesFromLastRun);
  size_t num_updates = atomic_load(&gc->updatesFromLastRun);
  size_t num_changes = num_writes + num_deletes + num_updates;
  if (!g_diskGcDisabled && sp->diskSpec &&
      (force || num_changes >= RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold)) {
    // Reset counters before running GC
    atomic_fetch_sub(&gc->writesFromLastRun, num_writes);
    atomic_fetch_sub(&gc->deletesFromLastRun, num_deletes);
    atomic_fetch_sub(&gc->updatesFromLastRun, num_updates);

    DiskGCRunStats stats = {0};
    SearchDisk_RunGC(sp->diskSpec, &stats);
    accumulateCycleStats(gc, &stats);

    gc->intervalSec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;
  }

  pthread_mutex_unlock(&g_diskGcRunLock);
  IndexSpecRef_Release(spec_ref);
  return true;
}

// Take the run lock and disable disk GC, blocking until any in-flight run finishes.
// Returns with g_diskGcRunLock HELD so the caller can close the disk indexes and clear
// each sp->diskSpec while it holds the lock: periodicCb touches sp->diskSpec only under
// this lock, so it can neither be mid-run against a spec being closed nor read a pointer
// being cleared. g_diskGcDisabled, latched here under the lock, also makes every run
// that acquires the lock afterwards bail before touching a diskSpec. Must be paired with
// DiskGC_UnlockRuns(). Runs on the main thread during shutdown teardown; may hold the GIL
// (run_gc never needs it — see g_diskGcRunLock above). Not re-enabled.
void DiskGC_LockRunsAndDisable(void) {
  pthread_mutex_lock(&g_diskGcRunLock);
  g_diskGcDisabled = true;
}

// Release the run lock taken by DiskGC_LockRunsAndDisable().
void DiskGC_UnlockRuns(void) {
  pthread_mutex_unlock(&g_diskGcRunLock);
}

void DiskGC_Cleanup(void) {
  // Destroy the module-global disk GC lock on full teardown. Must run after the GC
  // thread pool has been destroyed (RediSearch_CleanupModule) so no GC thread can
  // still hold or take the lock. Only reached on the full-cleanup path
  // (RS_GLOBAL_DTORS / sanitizer builds); on a plain production shutdown the process
  // just exits and the static mutex is reclaimed by the OS.
  pthread_mutex_destroy(&g_diskGcRunLock);
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
