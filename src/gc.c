/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <pthread.h>
#include <assert.h>
#include <unistd.h>

#include "gc.h"
#include "fork_gc.h"
#include "disk_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "module.h"
#include "spec.h"
#include "thpool/thpool.h"
#include "rmutil/rm_assert.h"
#include "util/logging.h"

static redisearch_thpool_t *gcThreadpool_g = NULL;

typedef struct GCDebugTask {
  GCContext* gc;
  RedisModuleBlockedClient* bClient;
} GCDebugTask;

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
      ret->gcCtx = FGC_Create(spec_ref, &ret->callbacks);
      break;
    case GCPolicy_Disk:
      ret->gcCtx = DiskGC_Create(spec_ref, &ret->callbacks);
      break;
    default:
      RS_LOG_ASSERT(false, "Invalid GC policy");
      break;
  }
  __atomic_store_n(&ret->jobRunning, false, __ATOMIC_RELAXED);
  __atomic_store_n(&ret->shouldReschedule, true, __ATOMIC_RELAXED);  // Default to reschedule
  __atomic_store_n(&ret->shutdownRequested, false, __ATOMIC_RELAXED);
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

// Pool thread: runs GC work and stores result for monitor timer to handle.
// No GIL acquisition - monitor timer on main thread handles scheduling.
static void taskCallback(void* data) {
  GCContext* gc = data;

  bool ret = gc->callbacks.periodicCallback(gc->gcCtx, false);

  // Store result for monitor timer to handle
  __atomic_store_n(&gc->shouldReschedule, ret, __ATOMIC_RELEASE);
  __atomic_store_n(&gc->jobRunning, false, __ATOMIC_RELEASE);
}

static void debugTaskCallback(void* data) {
  GCDebugTask *task = data;
  GCContext* gc = task->gc;
  RedisModuleBlockedClient* bc = task->bClient;

  gc->callbacks.periodicCallback(gc->gcCtx, true);

  // if GC was invoke by debug command, we release the client
  // and terminate without rescheduling the task again.
  if (bc) RedisModule_UnblockClient(bc, NULL);
  rm_free(task);
}

// Forward declaration
static void monitorTimerCallback(RedisModuleCtx* ctx, void* data);
static inline void GCContext_RescheduleMonitor(GCContext *gc) {
  gc->monitorTimerID = RedisModule_CreateTimer(RSDummyContext, GC_MONITOR_INTERVAL_MS, monitorTimerCallback, gc);
}

static inline bool GCContext_IsShutdownRequested(const GCContext *gc) {
  return __atomic_load_n(&gc->shutdownRequested, __ATOMIC_ACQUIRE);
}

static inline bool GCContext_IsJobRunning(const GCContext *gc) {
  return __atomic_load_n(&gc->jobRunning, __ATOMIC_ACQUIRE);
}

static inline void GCContext_RunPostJobCallback(GCContext *gc) {
  if (gc->callbacks.postJobCallback) {
    gc->callbacks.postJobCallback(gc->gcCtx);
  }
}

static void GCContext_Terminate(GCContext *gc) {
  gc->callbacks.onTerm(gc->gcCtx);
  rm_free(gc);
}

static void GCContext_HandleCompletedJob(GCContext *gc) {
  GCContext_RunPostJobCallback(gc);

  if (GCContext_IsShutdownRequested(gc)) {
    GCContext_Terminate(gc);
    return;
  }

  // GC job finished - handle result
  bool shouldReschedule = __atomic_load_n(&gc->shouldReschedule, __ATOMIC_ACQUIRE);
  if (!shouldReschedule) {
    RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "GC %p: Self-Terminating. Index was freed.", gc);
    GCContext_Terminate(gc);
    return;
  }

  // Schedule next GC run after the configured interval
  gc->timerID = scheduleNext(gc);
}

// Main thread (has GIL): Called at GC interval to start a GC job
static void timerCallback(RedisModuleCtx* ctx, void* data) {
  GCContext* gc = data;
  gc->timerID = 0;  // Timer fired, clear it

  if (GCContext_IsShutdownRequested(gc)) {
    return;
  }

  if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
    // If slave traffic is not allowed it means that there is a state machine running
    // we do not want to run any GC which might cause a FORK process to start for example.
    // Its better to just reschedule.
    gc->timerID = scheduleNext(gc);
    return;
  }

  // Start GC job
  __atomic_store_n(&gc->jobRunning, true, __ATOMIC_RELAXED);
  __atomic_store_n(&gc->shouldReschedule, true, __ATOMIC_RELAXED);  // Default to reschedule
  redisearch_thpool_add_work(gcThreadpool_g, taskCallback, gc, THPOOL_PRIORITY_HIGH);

  // Start monitor timer to detect completion
  GCContext_RescheduleMonitor(gc);
}

// Main thread (has GIL): Short-interval timer to check if GC job completed
static void monitorTimerCallback(RedisModuleCtx* ctx, void* data) {
  GCContext* gc = data;
  gc->monitorTimerID = 0;  // Timer fired, clear it

  // If the worker is still running, keep monitoring until it completes.
  if (GCContext_IsJobRunning(gc)) {
    GCContext_RescheduleMonitor(gc);
    return;
  }

  GCContext_HandleCompletedJob(gc);
}

void GCContext_StartNow(GCContext* gc) {
  RS_LOG_ASSERT_FMT(gc->timerID == 0 &&
                    !GCContext_IsJobRunning(gc),
                    "GC %p: StartNow called while GC is already running", gc);

  // Start GC job immediately
  __atomic_store_n(&gc->shutdownRequested, false, __ATOMIC_RELAXED);
  __atomic_store_n(&gc->jobRunning, true, __ATOMIC_RELAXED);
  __atomic_store_n(&gc->shouldReschedule, true, __ATOMIC_RELAXED);
  redisearch_thpool_add_work(gcThreadpool_g, taskCallback, gc, THPOOL_PRIORITY_HIGH);

  // Start monitor timer
  GCContext_RescheduleMonitor(gc);
}

void GCContext_Start(GCContext* gc) {
  __atomic_store_n(&gc->shutdownRequested, false, __ATOMIC_RELAXED);
  gc->monitorTimerID = 0;
  gc->timerID = scheduleNext(gc);
  if (!gc->timerID) {
    RedisModule_Log(RSDummyContext, "warning", "GC did not schedule next collection");
  }
}

// Stop the GC timers and prevent rescheduling.
// Must be called from main thread with GIL.
void GCContext_Stop(GCContext* gc) {
  if (!gc) return;
  __atomic_store_n(&gc->shutdownRequested, true, __ATOMIC_RELEASE);

  // Stop timers
  if (gc->timerID) {
    RedisModule_StopTimer(RSDummyContext, gc->timerID, NULL);
  }
  gc->timerID = 0;

  if (gc->monitorTimerID) {
    RedisModule_StopTimer(RSDummyContext, gc->monitorTimerID, NULL);
  }
  gc->monitorTimerID = 0;

  // If no job is running and no monitor timer is active, free now.
  // Otherwise the monitor callback will free once the running job completes.
  if (!GCContext_IsJobRunning(gc)) {
    GCContext_Terminate(gc);
  }
}

void GCContext_StopMock(GCContext* gc) {
  gc->callbacks.onTerm(gc->gcCtx);
  rm_free(gc);
}

void GCContext_RenderStats(GCContext* gc, RedisModule_Reply* reply) {
  gc->callbacks.renderStats(reply, gc->gcCtx);
}

void GCContext_RenderStatsForInfo(GCContext* gc, RedisModuleInfoCtx* ctx) {
  gc->callbacks.renderStatsForInfo(ctx, gc->gcCtx);
}

void GCContext_OnDelete(GCContext* gc) {
  if (gc->callbacks.onDelete) {
    gc->callbacks.onDelete(gc->gcCtx);
  }
}

void GCContext_GetStats(GCContext* gc, InfoGCStats* out) {
  gc->callbacks.getStats(gc->gcCtx, out);
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
    gcThreadpool_g = redisearch_thpool_create(GC_THREAD_POOL_SIZE, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, LogCallback, "gc");
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
