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

typedef struct GCCallbacks {
  // Returns true if the GC should be rescheduled, false if the GC should be stopped.
  bool (*periodicCallback)(void* gcCtx, bool force);
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
void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc);
void GCContext_ForceBGInvoke(GCContext* gc);
void GCContext_WaitForAllOperations(RedisModuleBlockedClient* bc);
void GCContext_GetStats(GCContext* gc, InfoGCStats* out);
// Stop periodic collection and disarm the timer. A run in flight completes without re-arming.
// Requires the GIL, as does GCContext_IsEnabled: both touch GIL-guarded scheduling state.
void GCContext_StopFutureRuns(GCContext* gc);
// Call when the index is being dropped: guarantees a run happens so the GC discovers the drop
// and frees itself, even if scheduling was stopped. Requires the GIL.
void GCContext_ScheduleTermination(GCContext* gc);
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
