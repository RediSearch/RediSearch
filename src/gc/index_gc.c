/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <stdlib.h>
#include <unistd.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>

#include "fork_cycle.h"
#include "time_sample.h"
#include "module.h"
#include "info/global_stats.h"
#include "config.h"
#include "debug_commands.h"
#include "index_gc.h"
#include "gc.h"
#include "redismodule.h"
#include "reply.h"
#include "result_processor.h"
#include "rmalloc.h"
#include "search_ctx.h"
#include "spec.h"
#include "util/references.h"

bool IndexGC_RunCycle(IndexGC *gc, GCForcedRun *forced, const IndexGCHook *hook) {
  // This check must be done first, because some values (like `deletedDocsFromLastRun`) that are used for
  // early termination might never change after index deletion and will cause periodicCb to always return true,
  // which will cause the GC to never stop rescheduling itself.
  // If the index was deleted, we don't want to reschedule the GC, so we return false.
  // If the index is still valid, we MUST hold the strong reference to it until after the fork, to make sure
  // the child process has a valid reference to the index.
  // If we were to try and revalidate the index after the fork, it might already be dropped and the child
  // will exit before sending any data, and might left the parent waiting for data that will never arrive.
  // Attempting to revalidate the index after the fork is also problematic because the parent and child are
  // not synchronized, and the parent might see the index alive while the child sees it as deleted.
  StrongRef early_check = IndexSpecRef_Promote(gc->index);
  if (!StrongRef_Get(early_check)) {
    // Index was deleted
    return false;
  }

  // Taken before the fork: a deletion deferred after this point is counted next cycle, never reset
  // away uncounted.
  const size_t deferred = IndexSpec_TakeGCDeferredDeletes(StrongRef_Get(early_check));
  if (deferred) {
    gc->deletedOrUpdatedDocsFromLastRun += deferred;
    IndexsGlobalStats_IncreaseLogicallyDeleted(deferred);
  }

  if (!forced && gc->deletedOrUpdatedDocsFromLastRun < RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold) {
    IndexSpecRef_Release(early_check);
    return true;
  }

  return ForkCycle_Run(gc, forced, hook, early_check);
}

static bool periodicCb(void *privdata, GCForcedRun *forced) {
  return IndexGC_RunCycle(privdata, forced, NULL);
}

static void onTerminateCb(void *privdata) {
  IndexGC *gc = privdata;
  size_t deleted_or_updated_docs = atomic_exchange(&gc->deletedOrUpdatedDocsFromLastRun, 0);
  IndexsGlobalStats_DecreaseLogicallyDeleted(deleted_or_updated_docs);
  WeakRef_Release(gc->index);
  RedisModule_FreeThreadSafeContext(gc->ctx);
  rm_free(gc);
}

static void statsCb(RedisModule_Reply *reply, void *gcCtx) {
#define REPLY_KVNUM(k, v) RedisModule_ReplyKV_Double(reply, (k), (v))
  IndexGC *gc = gcCtx;
  if (!gc) return;
  REPLY_KVNUM("bytes_collected", gc->stats.totalCollected);
  REPLY_KVNUM("total_ms_run", gc->stats.totalMSRun);
  REPLY_KVNUM("total_cycles", gc->stats.numCycles);
  REPLY_KVNUM("average_cycle_time_ms", (double)gc->stats.totalMSRun / gc->stats.numCycles);
  REPLY_KVNUM("last_run_time_ms", (double)gc->stats.lastRunTimeMs);
  REPLY_KVNUM("gc_numeric_trees_missed", (double)gc->stats.gcNumericNodesMissed);
  REPLY_KVNUM("gc_blocks_denied", (double)gc->stats.gcBlocksDenied);
}

static void statsForInfoCb(RedisModuleInfoCtx *ctx, void *gcCtx) {
  IndexGC *gc = gcCtx;
  RedisModule_InfoBeginDictField(ctx, "gc_stats");
  RedisModule_InfoAddFieldLongLong(ctx, "bytes_collected", gc->stats.totalCollected);
  RedisModule_InfoAddFieldLongLong(ctx, "total_ms_run", gc->stats.totalMSRun);
  RedisModule_InfoAddFieldLongLong(ctx, "total_cycles", gc->stats.numCycles);
  RedisModule_InfoAddFieldDouble(ctx, "average_cycle_time_ms", (double)gc->stats.totalMSRun / gc->stats.numCycles);
  RedisModule_InfoAddFieldDouble(ctx, "last_run_time_ms", (double)gc->stats.lastRunTimeMs);
  RedisModule_InfoAddFieldDouble(ctx, "gc_numeric_trees_missed", (double)gc->stats.gcNumericNodesMissed);
  RedisModule_InfoAddFieldDouble(ctx, "gc_blocks_denied", (double)gc->stats.gcBlocksDenied);
  RedisModule_InfoEndDictField(ctx);
}

static void deleteOrUpdateCb(void *ctx) {
  IndexGC *gc = ctx;
  ++gc->deletedOrUpdatedDocsFromLastRun;
  IndexsGlobalStats_IncreaseLogicallyDeleted(1);
}

static void getStatsCb(void *gcCtx, InfoGCStats *out) {
  const IndexGC *gc = gcCtx;
  out->totalCollectedBytes = gc->stats.totalCollected;
  out->totalCycles = gc->stats.numCycles;
  out->totalTime = gc->stats.totalMSRun;
  out->lastRunTimeMs = gc->stats.lastRunTimeMs;
}

static long long getIntervalMsCb(void *ctx) {
  IndexGC *gc = ctx;
  return gc->retryIntervalMs;
}

IndexGC *IndexGC_Create(StrongRef spec_ref, GCCallbacks *callbacks) {
  IndexGC *forkGc = rm_calloc(1, sizeof(*forkGc));
  *forkGc = (IndexGC){
      .index = StrongRef_Demote(spec_ref),
      .deletedOrUpdatedDocsFromLastRun = 0,
  };
  forkGc->retryIntervalMs = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec * 1000LL;

  forkGc->cleanNumericEmptyNodes = RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes;
  forkGc->ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);

  callbacks->onTerm = onTerminateCb;
  callbacks->periodicCallback = periodicCb;
  callbacks->renderStats = statsCb;
  callbacks->renderStatsForInfo = statsForInfoCb;
  callbacks->getIntervalMs = getIntervalMsCb;
  callbacks->onDelete = deleteOrUpdateCb;
  callbacks->onWrite = NULL; // writes are not tracked for forkGC
  callbacks->onUpdate = deleteOrUpdateCb;
  callbacks->getStats = getStatsCb;

  return forkGc;
}
