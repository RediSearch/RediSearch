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
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GC_THREAD_POOL_SIZE 1

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
  long long forkWaitMs;         // wait this long for the fork slot; 0 means no limit
  long long retryIntervalMs;    // how often to retry while waiting, capped by what is left
  struct timespec forkDeadline; // stamped when queued; meaningful only if forkWaitMs > 0
  GCForcedRunOutcome outcome;   // set by the cycle
} GCForcedRun;

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

typedef struct InfoGCStats {
  // Total bytes collected by the GCs
  // This is signed because block splitting (when deltas are too big) can cause more bytes to be
  // allocated by a GC than the number of bytes collected.
  ssize_t totalCollectedBytes;
  size_t totalCycles;   // Total number of cycles ran
  size_t totalTime;     // In ms
  long long lastRunTimeMs;
} InfoGCStats;

typedef struct GCCallbacks {
  // Returns true if the GC should be rescheduled, false if the GC should be stopped.
  // `forced` is non-NULL only for a demanded collection, which must really happen: it may wait
  // on resources a scheduled cycle would skip over.
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
  RedisModuleTimerID timerID;  // Guarded by the GIL
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
// Queue a demanded collection, built with GCForcedRun_New. `forced` is copied, so the caller
// keeps no ownership. With a blocked client, the outcome reaches its reply as unblock privdata,
// freed by GCForcedRunOutcomeFree.
void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc, GCForcedRun forced);
void GCContext_ForceBGInvoke(GCContext* gc, GCForcedRun forced);
// free_privdata for the blocked client above; matches RedisModule_BlockClient's signature.
void GCForcedRunOutcomeFree(RedisModuleCtx* ctx, void* privdata);
void GCContext_WaitForAllOperations(RedisModuleBlockedClient* bc);
void GCContext_GetStats(GCContext* gc, InfoGCStats* out);

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
