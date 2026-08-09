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

#include "reply.h"

#include "redismodule.h"
#include "util/dllist.h"
#include "util/references.h"
#include "util/rs_atomic.h"
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GC_THREAD_POOL_SIZE 1

// Bits of GCContext.schedFlags.
typedef enum {
  GC_SCHED_RUN_PENDING = 0x01,  // a run is queued on the GC pool, or executing
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
} GCForcedRunOutcome;

// What a caller that demanded a collection asked for, and what it got. A cycle receives a
// pointer to one, or NULL when the scheduler drove it -- so "was this forced" and "on what
// budget" stop being separate questions. Only FT.DEBUG GC_FORCEINVOKE and GC_FORCEBGINVOKE
// build one today; the budget is theirs to set, which is why it is not a constant here.
typedef struct {
  long long forkWaitMs;       // wait this long for the fork slot; 0 means no limit
  long long retryIntervalMs;  // how often to retry while waiting
  GCForcedRunOutcome outcome; // set by the cycle
} GCForcedRun;

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
  struct timespec (*getInterval)(void* ctx);
  void (*getStats)(void* gcCtx, InfoGCStats* out);
} GCCallbacks;

typedef struct GCContext {
  void* gcCtx;
  RedisModuleTimerID timerID;  // a live timer, or 0. Never a sentinel. Guarded by the GIL
  bool enabled;                // periodic collection wanted. Guarded by the GIL
  // Shared with the GC thread, though every access today is also under the GIL -- correctness
  // rests on that, not on the atomic. A bit set so a bit added later can be tested against
  // RUN_PENDING in one read-modify-write; separate relaxed accesses would not be ordered.
  RS_Atomic(unsigned) schedFlags;
  GCCallbacks callbacks;
} GCContext;

GCContext* GCContext_CreateGC(StrongRef spec_ref, uint32_t gcPolicy);
// Start the GC periodic. Next run will be added to the job-queue after the interval
void GCContext_Start(GCContext* gc);
// Start the GC periodic. Next run will be added to the job-queue immediately
void GCContext_StartNow(GCContext* gc);
void GCContext_StopMock(GCContext* gc);
void GCContext_RenderStats(GCContext* gc, RedisModule_Reply* ctx);
void GCContext_RenderStatsForInfo(GCContext* gc, RedisModuleInfoCtx* ctx);
void GCContext_OnDelete(GCContext* gc);
void GCContext_OnWrite(GCContext* gc);
void GCContext_OnUpdate(GCContext* gc);
// Queue a demanded collection. `forced` is copied, so the caller keeps no ownership. With a
// blocked client, the outcome reaches its reply as unblock privdata, freed by
// GCForcedRunOutcomeFree.
void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc, GCForcedRun forced);
void GCContext_ForceBGInvoke(GCContext* gc, GCForcedRun forced);
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

static inline void InfoGCStats_Add(InfoGCStats* dst, const InfoGCStats* src) {
  dst->totalCollectedBytes += src->totalCollectedBytes;
  dst->totalCycles += src->totalCycles;
  dst->totalTime += src->totalTime;
}

void GC_ThreadPoolStart();
void GC_ThreadPoolDestroy();

#ifdef __cplusplus
}
#endif
#endif /* SRC_GC_H_ */
