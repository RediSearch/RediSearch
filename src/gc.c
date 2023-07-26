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

static GCTask *GCTaskCreate(GCContext *gc, RedisModuleBlockedClient* bClient, int debug) {
  GCTask *task = rm_malloc(sizeof(*task));
  task->gc = gc;
  task->debug = debug;
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

static RedisModuleTimerID scheduleNext(GCTask *task) {
  if (RS_IsMock) return 0;

  long long period = getNextPeriod(task->gc);
  return RedisModule_CreateTimer(RSDummyContext, period, timerCallback, task);
}

static void threadCallback(void* data) {
  GCTask* task = data;
  GCContext* gc = task->gc;
  RedisModuleBlockedClient* bc = task->bClient;
  RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);

  int ret = gc->callbacks.periodicCallback(ctx, gc->gcCtx);

  // if GC was invoke by debug command, we release the client
  // and terminate without rescheduling the task again.
  if (task->debug) {
    if (bc) {
      RedisModule_UnblockClient(bc, NULL);
    }
    rm_free(task);
    goto end;
  }

  if (!ret) {
    rm_free (task);
    goto end;
  }

  RedisModule_ThreadSafeContextLock(ctx);
  gc->timerID = scheduleNext(task);
  RedisModule_ThreadSafeContextUnlock(ctx);

end:
  RedisModule_FreeThreadSafeContext(ctx);
}

static void destroyCallback(void* data) {
  GCContext* gc = data;

  gc->callbacks.onTerm(gc->gcCtx);
  rm_free(gc);
}

static void timerCallback(RedisModuleCtx* ctx, void* data) {
  if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
    // If slave traffic is not allow it means that there is a state machine running
    // we do not want to run any GC which might cause a FORK process to start for example).
    // Its better to just avoid it.
    GCTask* task = data;
    task->gc->timerID = scheduleNext(task);
    return;
  }
  redisearch_thpool_add_work(gcThreadpool_g, threadCallback, data, THPOOL_PRIORITY_HIGH);
}

void GCContext_Start(GCContext* gc) {
  GCTask* task = GCTaskCreate(gc, NULL, 0);
  gc->timerID = scheduleNext(task);
  if (gc->timerID == 0) {
    RedisModule_Log(RSDummyContext, "warning", "GC did not schedule next collection");
    rm_free(task);
  }
}

void GCContext_Stop(GCContext* gc) {
  if (RS_IsMock) {
    // for fork gc debug
    RedisModule_FreeThreadSafeContext(((ForkGC *)gc->gcCtx)->ctx);
    array_free(((ForkGC *)gc->gcCtx)->tieredIndexes);
    WeakRef_Release(((ForkGC *)gc->gcCtx)->index);
    free(gc->gcCtx);
    free(gc);
    return;
  }

  GCTask *data = NULL;
  if (RedisModule_StopTimer(RSDummyContext, gc->timerID, (void**)&data) == REDISMODULE_OK) {
    // GC is not running, we can free it immediately
    assert(data->gc == gc);
    rm_free(data);  // release task memory

    // free gc
    destroyCallback(gc);
  } else {
    // GC is running, we add a task to the thread pool to free it
    redisearch_thpool_add_work(gcThreadpool_g, destroyCallback, gc, THPOOL_PRIORITY_HIGH);
  }
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
  GCTask *task = GCTaskCreate(gc, bc, 1);
  redisearch_thpool_add_work(gcThreadpool_g, threadCallback, task, THPOOL_PRIORITY_HIGH);
}

void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc) {
  GCContext_CommonForceInvoke(gc, bc);
}

void GCContext_ForceBGInvoke(GCContext* gc) {
  GCContext_CommonForceInvoke(gc, NULL);
}

void GC_ThreadPoolStart() {
  if (gcThreadpool_g == NULL) {
    gcThreadpool_g = redisearch_thpool_create(GC_THREAD_POOL_SIZE, DEFAULT_PRIVILEGED_THREADS_NUM);
    redisearch_thpool_init(gcThreadpool_g, LogCallback);
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
