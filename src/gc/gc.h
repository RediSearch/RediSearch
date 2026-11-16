/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef SRC_GC_H_
#define SRC_GC_H_

#include "util/gil_gate.h"
#include "reply.h"

#include "redismodule.h"
#include "util/dllist.h"
#include "util/references.h"
#include "util/rs_atomic.h"
#include "util/module_timer.h"
#include <time.h>
#include <limits.h>

#ifdef __cplusplus
extern "C" {
#endif


// Bits of GCContext.schedFlags.
typedef enum {
  GC_SCHED_NONE = 0x00,
  GC_SCHED_RUN_PENDING = 0x01,  // a run is queued on the GC pool, or executing
  GC_SCHED_PAUSED = 0x02,       // a disk consistency window is open
} GCSchedFlags;

typedef struct InfoGCStats {
  // Total bytes collected by the GCs
  // This is signed because block splitting (when deltas are too big) can cause more bytes to be
  // allocated by a GC than the number of bytes collected.
  ssize_t totalCollectedBytes;
  size_t totalCycles;   // Total number of cycles ran
  size_t totalTime;     // In ms
  long long lastRunTimeMs;
} InfoGCStats;

// Retry granularity for a forced run that has to wait; the budget itself comes from the
// caller, so this only decides how often it looks again.
#define GC_FORCED_RUN_RETRY_INTERVAL_MS 250
// Budget for a forced run whose caller has nobody waiting on it (GC_FORCEBGINVOKE).
#define GC_FORCED_RUN_DEFAULT_WAIT_MS 30000
// Held back from a caller's timeout so its reply beats the blocked-client timeout; spending the
// whole timeout waiting would leave the two racing, and the generic timeout would often win.
#define GC_FORCED_RUN_REPLY_MARGIN_MS 500

typedef enum {
  // The cycle ran, or declined for a reason of its own -- collecting nothing under memory
  // pressure is GC policy, not a failure, so it reports OK.
  GC_FORCED_RUN_OK = 0,
  // No child process could be created, so nothing was collected.
  GC_FORCED_RUN_NO_FORK,
  // The disk cycle failed recoverably; pending changes remain eligible for retry.
  GC_DISK_FORCED_RUN_RETRY_REQUIRED,
  // `compactAll` was asked for but did not complete: the cycle was skipped (e.g. background
  // work was paused before it started), the index disappeared, or compaction was cancelled.
  GC_FORCED_RUN_COMPACT_ALL_INCOMPLETE,
  // The cycle stopped on an error, logged as a warning, and collected only what it applied first.
  GC_FORCED_RUN_FAILED,
  // Another run of the index was in flight, so this one did not start.
  GC_FORCED_RUN_BUSY,
} GCForcedRunOutcome;

// What a caller that demanded a collection asked for, and what it got. A cycle receives a
// pointer to one, or NULL when the scheduler drove it -- so "was this forced" and "on what
// budget" stop being separate questions. Only FT.DEBUG GC_FORCEINVOKE and GC_FORCEBGINVOKE
// build one today; the budget is theirs to set, which is why it is not a constant here.
typedef struct {
  long long forkWaitMs;         // wait this long for the fork slot; 0 means no limit
  long long retryIntervalMs;    // how often to retry while waiting, capped by what is left
  struct timespec forkDeadline; // stamped when queued; meaningful only if forkWaitMs > 0
  bool compactAll;              // disk GC only: also fully compact the rest of the disk storage
  GCForcedRunOutcome outcome;   // set by the cycle
} GCForcedRun;

// The longest GC interval a timer can be armed with. The jitter can double a period, and Redis
// arms a module timer at its monotonic clock plus the period in microseconds, so this keeps that
// sum in range while the clock reads below LLONG_MAX / 2 microseconds.
#define GC_MAX_INTERVAL_SEC (LLONG_MAX / 4 / 1000 / 1000)

// Interval configs may accept more than GC_MAX_INTERVAL_SEC, which is where this caps them.
static inline long long GC_IntervalSecToMs(size_t sec) {
  return (long long)(sec < GC_MAX_INTERVAL_SEC ? sec : GC_MAX_INTERVAL_SEC) * 1000;
}

// Builds a forced run with its deadline stamped from now, which is why callers use this rather
// than filling the struct themselves: a forkWaitMs without a matching deadline reads as a budget
// that expired before the first attempt. Call it on the thread asking for the collection, so the
// budget runs from there rather than from whenever the GC worker picks the job up.

static inline GCForcedRun GCForcedRun_New(long long forkWaitMs, long long retryIntervalMs) {
  GCForcedRun forced;
  forced.forkWaitMs = forkWaitMs;
  forced.retryIntervalMs = retryIntervalMs;
  forced.forkDeadline.tv_sec = 0;
  forced.forkDeadline.tv_nsec = 0;
  forced.compactAll = false;
  forced.outcome = GC_FORCED_RUN_OK;
  if (forkWaitMs > 0) {
    clock_gettime(CLOCK_MONOTONIC_RAW, &forced.forkDeadline);
    forced.forkDeadline.tv_sec += forkWaitMs / 1000;
    forced.forkDeadline.tv_nsec += (forkWaitMs % 1000) * 1000000;
    if (forced.forkDeadline.tv_nsec >= 1000000000) {
      forced.forkDeadline.tv_sec++;
      forced.forkDeadline.tv_nsec -= 1000000000;
    }
  }
  return forced;
}

// What a timer waits for.
typedef enum {
  GC_TIMER_RUN_INTERVAL = 0,  // the run interval
  GC_TIMER_RETRY,             // a retry of a fork that failed
} GCTimerKind;

// The timer a GC wants armed next. The interval is before jitter.
typedef struct {
  GCTimerKind kind;
  long long intervalMs;
} GCTimerPlan;

typedef struct GCCallbacks {
  // Returns true if the GC should be rescheduled, false if the GC should be stopped.
  // `forced` is non-NULL only for a demanded collection, which must really happen: it skips
  // the clean threshold and may wait on resources a scheduled cycle would skip over.
  bool (*periodicCallback)(void* gcCtx, GCForcedRun* forced);
  void (*renderStats)(RedisModule_Reply* reply, void* gc);
  void (*renderStatsForInfo)(RedisModuleInfoCtx* ctx, void* gc);
  void (*onDelete)(void* ctx);
  void (*onWrite)(void* ctx);
  void (*onUpdate)(void* ctx);
  void (*onTerm)(void* ctx);
  // Main thread only, and never while a scheduled run holds RUN_PENDING: called when arming,
  // so the interval is read where it is set, and a change before the arm is never missed.
  // Always yields a plan, even once the index is gone: a drop that overtakes a pending re-arm
  // relies on the timer armed from it to find the drop and free the context.
  GCTimerPlan (*nextTimer)(void* ctx);
  // Main thread only. Whether the index is still there, so a timer stepping toward a far
  // deadline can hand a drop to a run now rather than at that deadline.
  bool (*indexExists)(void* ctx);
  void (*getStats)(void* gcCtx, InfoGCStats* out);
  // Optional. Called on the main thread each time a scheduled run is queued, once the run holds
  // the context and before the GC pool can start it, so the run sees the settings as they were
  // when it was queued. Not called for forced runs.
  void (*onQueueRun)(void* gcCtx);
} GCCallbacks;

typedef struct GCContext {
  void* gcCtx;
  GCCallbacks callbacks;
  RedisModuleTimerID timerID;  // a live timer, or 0. Never a sentinel. Guarded by the GIL
  // The plan the live timer was armed with; meaningful only while timerID is set. Kept so an
  // interval change can tell whether the waiting timer is already armed with the new value.
  // Guarded by the GIL
  GCTimerPlan armedPlan;
  // A delay past RS_MAX_TIMER_DELAY_MS is armed in steps of at most that. While the live timer
  // is a step short of its deadline, this holds the deadline in monotonic ms, and the step's
  // callback arms the next step instead of running -- or arms afresh if the interval changed
  // since the arm; 0 once the live timer fires at the deadline. Guarded by the GIL
  uint64_t stepDeadlineMs;
  // Main thread only in the real path; the atomic covers the RS_IsMock fallback, where the GC
  // thread clears it. A bit set so a bit added later can be tested against RUN_PENDING in one
  // read-modify-write; separate relaxed accesses would not be ordered.
  RS_Atomic(unsigned) schedFlags;
  bool enabled;  // periodic collection wanted. Guarded by the GIL
  bool diskGC;   // a disk index's context, whatever GC_POLICY says
} GCContext;

GCContext* GCContext_CreateGC(StrongRef spec_ref, uint32_t gcPolicy);
// Start the GC periodic. Next run will be added to the job-queue after the interval
void GCContext_Start(GCContext* gc);
// Start the GC periodic. Next run is queued immediately -- unless a run already owns the
// re-arm, or a consistency window is open, in which case it comes after the interval instead.
void GCContext_StartNow(GCContext* gc);
void GCContext_StopMock(GCContext* gc);
void GCContext_RenderStats(GCContext* gc, RedisModule_Reply* ctx);
void GCContext_RenderStatsForInfo(GCContext* gc, RedisModuleInfoCtx* ctx);
void GCContext_OnDelete(GCContext* gc);
void GCContext_OnWrite(GCContext* gc);
void GCContext_OnUpdate(GCContext* gc);
// Queue a demanded collection, built with GCForcedRun_New. `forced` is copied, so the caller
// keeps no ownership. With a blocked client, the outcome reaches its reply as unblock privdata,
// freed by GCForcedRunOutcomeFree.
void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc, GCForcedRun forced);
// OK once the run is queued; BUSY or FAILED when it never will be.
GCForcedRunOutcome GCContext_ForceBGInvoke(GCContext* gc, GCForcedRun forced);
// free_privdata for the blocked client above; matches RedisModule_BlockClient's signature.
void GCForcedRunOutcomeFree(RedisModuleCtx* ctx, void* privdata);
void GCContext_WaitForAllOperations(RedisModuleBlockedClient* bc);
void GCContext_GetStats(GCContext* gc, InfoGCStats* out);
// Stop periodic collection and disarm the timer. A run in flight completes without re-arming.
// Requires the GIL, as does GCContext_IsEnabled: both touch GIL-guarded scheduling state.
void GCContext_StopFutureRuns(GCContext* gc);
// Drop-time hand-off, in two phases because a run already in flight frees this context without
// the GIL as soon as the unlink lands. Both require the GIL.
// Call before unlinking the spec; returns true if the context is still the caller's to schedule.
bool GCContext_BeginDrop(GCContext* gc);
// Call after unlinking the spec, only when GCContext_BeginDrop returned true. Queues the run
// that discovers the drop and frees the context.
void GCContext_FinishDrop(GCContext* gc);
bool GCContext_IsEnabled(const GCContext* gc);
// Main thread only; `gc` may be NULL, for an index without GC. Restarts a waiting run-interval
// timer from now, with fresh jitter, when the interval the GC would arm with now differs from
// the one that timer was armed with -- so each change to a different value postpones the run
// by up to a new period, while repeating the armed value does not. It never queues a run. With
// no run-interval timer waiting -- a run queued or running, GC stopped, scheduling paused, or a
// fork retry waiting -- it does nothing, and the next run-interval arm picks up the new
// interval.
void GCContext_OnIntervalChanged(GCContext* gc);
// The delay of a timer armed for `intervalMs`: the interval plus a jitter of
// `draw % intervalMs`, so it runs from the interval to almost twice it, or 0 for an interval
// that is not positive. An interval past GC_IntervalSecToMs's bound is taken at that bound, so
// the sum cannot overflow; GCContext arms a delay past RS_MAX_TIMER_DELAY_MS in steps. `draw`
// is a random value; a parameter so tests can pin it.
long long GC_TimerDelayMs(long long intervalMs, unsigned long draw);
// The monotonic clock, in ms, that timer deadlines are kept on. A variable so tests can move
// time forward between fake timer expiries.
extern uint64_t (*GC_NowMs)(void);
// Suspend/resume scheduling for a disk consistency window, leaving `enabled` untouched so a
// GC stopped by a debug command stays stopped across one. Both require the GIL.
void GCContext_PauseScheduling(GCContext* gc);
void GCContext_ResumeScheduling(GCContext* gc);

static inline void InfoGCStats_Add(InfoGCStats* dst, const InfoGCStats* src) {
  dst->totalCollectedBytes += src->totalCollectedBytes;
  dst->totalCycles += src->totalCycles;
  dst->totalTime += src->totalTime;
}

// The delay, in ms, from arming `gc` for `intervalMs` to its run, which GCContext reaches in
// steps when it is past RS_MAX_TIMER_DELAY_MS.
long long GCContext_NextPeriodMs(const GCContext* gc, long long intervalMs);

void GC_ThreadPoolStart();
// Resize a started GC pool to what the policy and search-gc-threads call for.
void GC_ThreadPoolSetSize(void);
void GC_ThreadPoolDestroy();
// Gate through which GC jobs take the GIL, so GC_ThreadPoolDestroy can join them while holding it.
extern GilGate gcGilGate_g;
// Stop the GC pool from pulling new jobs. Returns immediately; use GC_ThreadPoolWaitForPause
// to wait out the ones already running.
void GC_ThreadPoolPause(void);
// Wait, up to `timeoutMs`, for the running GC jobs to return. Returns false on timeout. Must
// follow GC_ThreadPoolPause, or a new job can start the moment this returns.
bool GC_ThreadPoolWaitForPause(long timeoutMs);
// GC jobs currently executing. For diagnosing a GC_ThreadPoolWaitForPause timeout; stale the
// moment it is read anywhere else.
size_t GC_ThreadPoolJobsInProgress(void);
// Let the pool pull queued jobs again, undoing GC_ThreadPoolPause, and apply a size change
// made while it was paused.
void GC_ThreadPoolResume(void);
// The GC pool's threads, a requested reduction counted as done; 0 before the pool exists.
size_t GC_ThreadPoolNumThreads(void);
// The GC pool's threads still running, which a reduction lowers only once its threads exit.
size_t GC_ThreadPoolThreadsAlive(void);
// Queue `fn(arg)` on the GC pool. No production caller: tests use it to hold a job in
// progress across a drain, which no ordinary entry point can do -- a real cycle returns
// long before GC_ThreadPoolWaitForPause could observe it.
void GC_ThreadPoolAddJobForTests(void (*fn)(void*), void* arg);

#ifdef __cplusplus
}
#endif
#endif /* SRC_GC_H_ */
