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

static int periodicCb(void *privdata) {
  DiskGC *gc = privdata;
  StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    return 0;
  }
  if (!sp->diskSpec) {
    IndexSpecRef_Release(spec_ref);
    return 1;
  }

  if (gc->deletedDocsFromLastRun < RSGlobalConfig.gcConfigParams.forkGc.forkGcCleanThreshold) {
    IndexSpecRef_Release(spec_ref);
    return 1;
  }

  size_t num_docs_to_clean = gc->deletedDocsFromLastRun;
  gc->deletedDocsFromLastRun = 0;
  IndexsGlobalStats_UpdateLogicallyDeleted(-(int64_t)num_docs_to_clean);

  SearchDisk_RunGC(sp->diskSpec);

  gc->interval.tv_sec = RSGlobalConfig.gcConfigParams.forkGc.forkGcRunIntervalSec;
  gc->interval.tv_nsec = 0;

  IndexSpecRef_Release(spec_ref);
  return 1;
}

static void onTerminateCb(void *privdata) {
  DiskGC *gc = privdata;
  IndexsGlobalStats_UpdateLogicallyDeleted(-(int64_t)gc->deletedDocsFromLastRun);
  WeakRef_Release(gc->index);
  RedisModule_FreeThreadSafeContext(gc->ctx);
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
  ++gc->deletedDocsFromLastRun;
  IndexsGlobalStats_UpdateLogicallyDeleted(1);
}

static void getStatsCb(void *gcCtx, InfoGCStats *out) {
  (void)gcCtx;
  out->totalCollectedBytes = 0;
  out->totalCycles = 0;
  out->totalTime = 0;
}

static struct timespec getIntervalCb(void *ctx) {
  DiskGC *gc = ctx;
  return gc->interval;
}

DiskGC *DiskGC_New(StrongRef spec_ref, GCCallbacks *callbacks) {
  DiskGC *gc = rm_calloc(1, sizeof(*gc));
  *gc = (DiskGC){
      .index = StrongRef_Demote(spec_ref),
      .deletedDocsFromLastRun = 0,
  };
  gc->interval.tv_sec = RSGlobalConfig.gcConfigParams.forkGc.forkGcRunIntervalSec;
  gc->interval.tv_nsec = 0;
  gc->ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);

  callbacks->onTerm = onTerminateCb;
  callbacks->periodicCallback = periodicCb;
  callbacks->renderStats = statsCb;
  callbacks->renderStatsForInfo = statsForInfoCb;
  callbacks->getInterval = getIntervalCb;
  callbacks->onDelete = deleteCb;
  callbacks->getStats = getStatsCb;

  return gc;
}
