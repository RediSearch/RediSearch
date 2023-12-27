/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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

typedef struct GCCallbacks {
  int  (*periodicCallback)(void* gcCtx);
  void (*renderStats)(RedisModule_Reply* reply, void* gc);
  void (*renderStatsForInfo)(RedisModuleInfoCtx* ctx, void* gc);
  void (*onDelete)(void* ctx);
  void (*onTerm)(void* ctx);
  struct timespec (*getInterval)(void* ctx);
} GCCallbacks;

typedef struct GCContext {
  void* gcCtx;
  RedisModuleTimerID timerID; // Guarded by the GIL
  GCCallbacks callbacks;
} GCContext;

GCContext* GCContext_CreateGC(StrongRef spec_ref, uint32_t gcPolicy);
// Start the GC periodic. Next run will be added to the job-queue after the interval
void GCContext_Start(GCContext* gc);
// Start the GC periodic. Next run will be added to the job-queue immediately
void GCContext_StartNow(GCContext* gc);
void GCContext_StopMock(GCContext* gc);
void GCContext_RenderStats(GCContext* gc, RedisModule_Reply* ctx);
#ifdef FTINFO_FOR_INFO_MODULES
void GCContext_RenderStatsForInfo(GCContext* gc, RedisModuleInfoCtx* ctx);
#endif
void GCContext_OnDelete(GCContext* gc);
void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc);
void GCContext_ForceBGInvoke(GCContext* gc);
void GCContext_WaitForAllOperations(RedisModuleBlockedClient* bc);

void GC_ThreadPoolStart();
void GC_ThreadPoolDestroy();

#ifdef __cplusplus
}
#endif
#endif /* SRC_GC_H_ */
