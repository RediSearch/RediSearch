/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <pthread.h>
#include <assert.h>
#include <unistd.h>

#include "gc.h"
#include "fork_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "module.h"
#include "spec.h"
#include "thpool/thpool.h"
#include "rmutil/rm_assert.h"
#include "util/logging.h"

static redisearch_threadpool gcThreadpool_g = NULL;

static GCDebugTask *GCDebugTaskCreate(GCContext *gc, RedisModuleBlockedClient* bClient) {
  GCDebugTask *task = rm_new(GCDebugTask);
  task->gc = gc;
  task->bClient = bClient;
  return task;
}

GCContext* GCContext_CreateGC(StrongRef spec_ref, uint32_t gcPolicy) {
  GCContext* ret = rm_calloc(1, sizeof(GCContext));
  switch (gcPolicy) {
    case GCPolicy_Fork:
      ret->gcCtx = FGC_New(spec_ref, &ret->callbacks);
      break;
  }
  return ret;
}

static void timerCallback(RedisModuleCtx* ctx, void* data);

static long long getNextPeriod(GCContext* gc) {
  struct timespec interval = gc->callbacks.getInterval(gc->gcCtx);
  long long ms = interval.tv_sec * 1000 + interval.tv_nsec / 1000000;  // convert to millisecond

  // add randomness to avoid congestion by multiple GCs from different shards
  ms += (rand() % interval.tv_sec) * 1000;

  return ms;
}

static RedisModuleTimerID scheduleNext(GCContext *gc) {
  if (RS_IsMock) return 0;

  long long period = getNextPeriod(gc);
  return RedisModule_CreateTimer(RSDummyContext, period, timerCallback, gc);
}

static void taskCallback(void* data) {
  GCContext* gc = data;

  int ret = gc->callbacks.periodicCallback(gc->gcCtx);

  if (ret) { // The common case
    // The index was not freed. We need to reschedule the task.
    RedisModule_ThreadSafeContextLock(RSDummyContext);
    if (gc->timerID) {
      gc->timerID = scheduleNext(gc);
    } else {
      // ... unless the GC was stopped by a debug command.
      RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_DEBUG, "GC %p: Not scheduling next collection", gc);
    }
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
  } else {
    // The index was freed. There is no need to reschedule the task.
    // We need to free the task and the GC.
    RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "GC %p: Self-Terminating. Index was freed.", gc);
    gc->callbacks.onTerm(gc->gcCtx);
    rm_free(gc);
  }
}

static void debugTaskCallback(void* data) {
  GCDebugTask *task = data;
  GCContext* gc = task->gc;
  RedisModuleBlockedClient* bc = task->bClient;

  int ret = gc->callbacks.periodicCallback(gc->gcCtx);

  // if GC was invoke by debug command, we release the client
  // and terminate without rescheduling the task again.
  if (bc) RedisModule_UnblockClient(bc, NULL);
  rm_free(task);
}

static void timerCallback(RedisModuleCtx* ctx, void* data) {
  if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
    // If slave traffic is not allow it means that there is a state machine running
    // we do not want to run any GC which might cause a FORK process to start for example).
    // Its better to just avoid it.
    GCContext* gc = data;
    gc->timerID = scheduleNext(gc);
    return;
  }
  redisearch_thpool_add_work(gcThreadpool_g, taskCallback, data, THPOOL_PRIORITY_HIGH);
}

void GCContext_Start(GCContext* gc) {
  gc->timerID = scheduleNext(gc);
  if (gc->timerID == 0) {
    RedisModule_Log(RSDummyContext, "warning", "GC did not schedule next collection");
  }
}

void GCContext_StopMock(GCContext* gc) {
  // for fork gc debug
  RedisModule_FreeThreadSafeContext(((ForkGC *)gc->gcCtx)->ctx);
  WeakRef_Release(((ForkGC *)gc->gcCtx)->index);
  free(gc->gcCtx);
  free(gc);
}

void GCContext_RenderStats(GCContext* gc, RedisModule_Reply* reply) {
  gc->callbacks.renderStats(reply, gc->gcCtx);
}

#ifdef FTINFO_FOR_INFO_MODULES
void GCContext_RenderStatsForInfo(GCContext* gc, RedisModuleInfoCtx* ctx) {
  gc->callbacks.renderStatsForInfo(ctx, gc->gcCtx);
}
#endif

void GCContext_OnDelete(GCContext* gc) {
  if (gc->callbacks.onDelete) {
    gc->callbacks.onDelete(gc->gcCtx);
  }
}

void GCContext_CommonForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc) {
  GCDebugTask *task = GCDebugTaskCreate(gc, bc);
  redisearch_thpool_add_work(gcThreadpool_g, debugTaskCallback, task, THPOOL_PRIORITY_HIGH);
}

void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc) {
  GCContext_CommonForceInvoke(gc, bc);
}

void GCContext_ForceBGInvoke(GCContext* gc) {
  GCContext_CommonForceInvoke(gc, NULL);
}

static void GCContext_UnblockClient(void* data) {
  RedisModuleBlockedClient *bc = data;
  RedisModule_BlockedClientMeasureTimeEnd(bc);
  RedisModule_UnblockClient(bc, NULL);
}

void GCContext_WaitForAllOperations(RedisModuleBlockedClient* bc) {
  redisearch_thpool_add_work(gcThreadpool_g, GCContext_UnblockClient, bc, THPOOL_PRIORITY_HIGH);
}

void GC_ThreadPoolStart() {
  if (gcThreadpool_g == NULL) {
    gcThreadpool_g = redisearch_thpool_create(GC_THREAD_POOL_SIZE, DEFAULT_PRIVILEGED_THREADS_NUM, LogCallback);
    redisearch_thpool_init(gcThreadpool_g);
  }
}

void GC_ThreadPoolDestroy() {
  if (gcThreadpool_g != NULL) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    redisearch_thpool_destroy(gcThreadpool_g);
    gcThreadpool_g = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}
