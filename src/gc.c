/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "gc.h"
#include "fork_gc.h"
#include "disk_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "module.h"
#include "thpool/thpool.h"
#include "rmutil/rm_assert.h"
#include "util/logging.h"
#include "redisearch.h"
#include "result_processor.h"
#include "debug_commands.h"

static redisearch_thpool_t *gcThreadpool_g = NULL;
// Guarded by the GIL. Only used to pause GC contexts created while a window is already open;
// live ones are paused/resumed by walking IndexSpec->gc.
static bool gcSchedulingPauseActiveForConsistency_g = false;

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
  if (gcSchedulingPauseActiveForConsistency_g) {
    RS_AtomicUintFetchOrRelaxed(&ret->schedFlags, GC_SCHED_PAUSED);
  }
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
  return ret;
}

static void timerCallback(RedisModuleCtx* ctx, void* data);
static void taskCallback(void* data);

static bool GCContext_RunPending(GCContext* gc) {
  return (RS_AtomicUintLoadRelaxed(&gc->schedFlags) & GC_SCHED_RUN_PENDING) != 0;
}

static bool GCContext_PausedForConsistency(GCContext* gc) {
  return (RS_AtomicUintLoadRelaxed(&gc->schedFlags) & GC_SCHED_PAUSED) != 0;
}

static long long getNextPeriod(struct timespec interval) {
  long long ms = interval.tv_sec * 1000 + interval.tv_nsec / 1000000;  // convert to millisecond

  // add randomness to avoid congestion by multiple GCs from different shards
  ms += (rand() % interval.tv_sec) * 1000;

  return ms;
}

static RedisModuleTimerID scheduleNextIn(GCContext* gc, struct timespec interval) {
  if (RS_IsMock) return 0;

  IncrementGCTimerArm();
  return RedisModule_CreateTimer(RSDummyContext, getNextPeriod(interval), timerCallback, gc);
}

// Main thread only -- arming off it is unsafe even under the GIL (MOD-17309). Callable blindly;
// the no-timer-while-a-run-is-pending invariant comes from QueueRun declining and timerCallback
// zeroing timerID, not from the RunPending check here.
static bool GCContext_ArmTimerIn(GCContext* gc, struct timespec interval) {
  if (!gc->enabled || gc->timerID || GCContext_RunPending(gc) ||
      GCContext_PausedForConsistency(gc)) {
    return false;
  }
  gc->timerID = scheduleNextIn(gc, interval);
  if (gc->timerID == 0) {
    RedisModule_Log(RSDummyContext, "warning", "GC did not schedule next collection");
    return false;
  }
  return true;
}

static void GCContext_ArmTimer(GCContext* gc) {
  GCContext_ArmTimerIn(gc, gc->callbacks.getInterval(gc->gcCtx));
}

static void GCContext_DisarmTimer(GCContext* gc) {
  if (gc->timerID) {
    RedisModule_StopTimer(RSDummyContext, gc->timerID, NULL);
    gc->timerID = 0;
  }
}

// Claims RUN_PENDING and queues a run, or does nothing if a run already holds this context --
// two runs behind one bit would let the first one's tail clear it while the second is still
// queued, and every guard that reads RUN_PENDING would be void from then on.
static void GCContext_QueueRun(GCContext* gc) {
  if (RS_AtomicUintFetchOrRelaxed(&gc->schedFlags, GC_SCHED_RUN_PENDING) & GC_SCHED_RUN_PENDING) {
    return;  // that run's tail re-arms
  }
  if (redisearch_thpool_add_work(gcThreadpool_g, taskCallback, gc, THPOOL_PRIORITY_HIGH) != 0) {
    // Nothing else would clear the bit, and no run is coming to re-arm -- so without both of
    // these this index loses collection for the life of the process.
    RS_AtomicUintFetchAndRelaxed(&gc->schedFlags, ~(unsigned)GC_SCHED_RUN_PENDING);
    GCContext_ArmTimer(gc);
  }
}

// Posted from the GC thread, run on the main thread. The pass keeps RUN_PENDING until here,
// so nothing could arm a timer or queue a run while the post was in flight.
static void rearmOneShotCb(void* data) {
  GCContext* gc = data;
  RS_AtomicUintFetchAndRelaxed(&gc->schedFlags, ~(unsigned)GC_SCHED_RUN_PENDING);
  if (GCContext_ArmTimerIn(gc, gc->pendingInterval)) {
    IncrementGCTimerArmFromOneShot();
  }
}

static void taskCallback(void* data) {
  GCContext* gc = data;

  // Test hook (ENABLE_ASSERT): park before the pass starts, with RUN_PENDING held and no GIL --
  // this pass never takes it, the re-arm it posts runs on the main thread. Lets a test drive
  // the scheduling guards against a real in-flight run rather than racing one.
#ifdef ENABLE_ASSERT
  SyncPoint_Wait(SYNC_POINT_GC_TASK_START);
#endif

  bool ret = gc->callbacks.periodicCallback(gc->gcCtx, false);

  if (ret) { // The common case
    // The index was not freed. Hand the re-arm to the main thread.
    if (RS_IsMock) {  // the unit-test harness has no event loop
      RS_AtomicUintFetchAndRelaxed(&gc->schedFlags, ~(unsigned)GC_SCHED_RUN_PENDING);
      return;
    }
    gc->pendingInterval = gc->callbacks.getInterval(gc->gcCtx);
    RedisModule_EventLoopAddOneShot(rearmOneShotCb, gc);
  } else {
    // The index was freed. RUN_PENDING is still set for this run, so no timer is armed and no
    // second periodic run is queued -- free inline, which keeps the GC pool join in
    // GC_ThreadPoolDestroy as the guarantee that this happens at shutdown. Debug tasks
    // (GC_FORCEBGINVOKE) hold `gc` outside this model.
    RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "GC %p: Self-Terminating. Index was freed.", gc);
    gc->callbacks.onTerm(gc->gcCtx);
    rm_free(gc);
  }
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

static void timerCallback(RedisModuleCtx* ctx, void* data) {
  GCContext* gc = data;
  gc->timerID = 0;  // the timer that just fired is gone

  if (!gc->enabled || GCContext_PausedForConsistency(gc)) {
    return;  // the resume path re-arms
  }
  if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
    // If slave traffic is not allowed it means that there is a state machine running
    // we do not want to run any GC which might cause a FORK process to start for example.
    // Its better to just avoid it.
    GCContext_ArmTimer(gc);
    return;
  }
  GCContext_QueueRun(gc);
}

void GCContext_StartNow(GCContext* gc) {
  RS_LOG_ASSERT_FMT(!gc->enabled && gc->timerID == 0,
                    "GC %p: StartNow called while GC is already running", gc);
  gc->enabled = true;
  if (GCContext_PausedForConsistency(gc)) {
    return;  // the resume path arms a timer instead
  }
  GCContext_QueueRun(gc);
}

void GCContext_Start(GCContext* gc) {
  gc->enabled = true;
  GCContext_ArmTimer(gc);
}

bool GCContext_BeginDrop(GCContext* gc) {
  // The mock harness has no GC pool and frees the GC inside `IndexSpec_Free`.
  if (RS_IsMock) {
    return false;
  }
  // Re-enable unconditionally, before deciding anything else. A run in flight is about to
  // choose whether to re-arm, and if this GC was stopped that choice is the difference between
  // the timer eventually discovering the drop and nothing ever doing so. A no-op when already
  // enabled, which is why it cannot disturb the ordinary path below.
  gc->enabled = true;
  // Same for a consistency-window pause, and for the same reason -- the window's resume
  // re-derives its set by walking specDict_g, which this spec is about to leave.
  RS_AtomicUintFetchAndRelaxed(&gc->schedFlags, ~(unsigned)GC_SCHED_PAUSED);
  // An armed timer, or a run in flight (which now re-arms), will discover the drop on its own --
  // the mechanism `IndexSpec_Free` already relies on. Leave those alone: a run in flight also
  // frees this context itself, on the GC thread and without the GIL, as soon as the unlink makes
  // its promote fail, so it must not be touched after the unlink either. Reading the bit is
  // safe: that run's tail clears it under the GIL, and only the main thread ever sets it.
  if (gc->timerID != 0 || GCContext_RunPending(gc)) {
    return false;
  }
  // Neither exists, so nothing would discover the drop -- the state `GC_STOP_SCHEDULE` leaves
  // behind once its run has finished. Queue one after the unlink.
  return true;
}

void GCContext_FinishDrop(GCContext* gc) {
  // `GCContext_BeginDrop` returned true, so there is no timer to collide with: it established
  // `timerID == 0`, and only the main thread -- us, without yielding since -- arms one.
  GCContext_QueueRun(gc);
}

void GCContext_StopFutureRuns(GCContext* gc) {
  gc->enabled = false;
  GCContext_DisarmTimer(gc);
}

bool GCContext_IsEnabled(const GCContext* gc) {
  return gc->enabled;
}

void GCContext_PauseSchedulingForConsistency(GCContext* gc) {
  if (!gc) {
    return;
  }
  RS_AtomicUintFetchOrRelaxed(&gc->schedFlags, GC_SCHED_PAUSED);
  GCContext_DisarmTimer(gc);
}

void GCContext_ResumeSchedulingAfterConsistency(GCContext* gc) {
  if (!gc || !GCContext_PausedForConsistency(gc)) {
    return;
  }
  // Clearing PAUSED and reading RUN_PENDING is one read-modify-write, so this cannot see a
  // half-updated word. A run that outlived the window re-arms from its own one-shot; arming
  // here as well would leave two live timers for one GC, the first of them orphaned.
  const unsigned prev =
      RS_AtomicUintFetchAndRelaxed(&gc->schedFlags, ~(unsigned)GC_SCHED_PAUSED);
  if (!(prev & GC_SCHED_RUN_PENDING)) {
    GCContext_ArmTimer(gc);
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

void GCContext_OnWrite(GCContext* gc) {
  if (gc->callbacks.onWrite) {
    gc->callbacks.onWrite(gc->gcCtx);
  }
}

void GCContext_OnUpdate(GCContext* gc) {
  if (gc->callbacks.onUpdate) {
    gc->callbacks.onUpdate(gc->gcCtx);
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
    if (redisearch_thpool_paused(gcThreadpool_g)) {
      gcSchedulingPauseActiveForConsistency_g = false;
      redisearch_thpool_resume_threads(gcThreadpool_g);
    }
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    redisearch_thpool_destroy(gcThreadpool_g);
    gcThreadpool_g = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}

void GC_ThreadPoolPauseForConsistency(void) {
  gcSchedulingPauseActiveForConsistency_g = true;
  if (gcThreadpool_g) {
    redisearch_thpool_pause_threads_no_wait(gcThreadpool_g);
  }
}

bool GC_ThreadPoolWaitForPause(long timeoutMs) {
  if (!gcThreadpool_g) {
    return true;
  }
  // The queue is already flagged paused, so no new job can start; wait only for the running
  // ones to return. Bounded because this runs on the main thread with the GIL held -- an
  // unbounded spin here stalls the whole shard.
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  while (redisearch_thpool_num_jobs_in_progress(gcThreadpool_g)) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    long elapsedMs = (now.tv_sec - start.tv_sec) * 1000 + (now.tv_nsec - start.tv_nsec) / 1000000;
    if (elapsedMs >= timeoutMs) {
      return false;
    }
    usleep(100);
  }
  return true;
}

void GC_ThreadPoolResumeAfterConsistency(void) {
  gcSchedulingPauseActiveForConsistency_g = false;
  if (gcThreadpool_g && redisearch_thpool_paused(gcThreadpool_g)) {
    redisearch_thpool_resume_threads(gcThreadpool_g);
  }
}
