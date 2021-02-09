#include <pthread.h>
#include <assert.h>
#include <unistd.h>

#include "gc.h"
#include "fork_gc.h"
#include "default_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "module.h"
#include "spec.h"
#include "dep/thpool/thpool.h"
#include "rmutil/rm_assert.h"

static threadpool gcThreadpool_g = NULL;

static GCTask *GCTaskCreate(GCContext *gc, RedisModuleBlockedClient* bClient, int debug) {
  GCTask *task = rm_malloc(sizeof(*task));
  task->gc = gc;
  task->debug = debug;
  task->bClient = bClient;
  return task;
}

GCContext* GCContext_CreateGCFromSpec(IndexSpec* sp, float initialHZ, uint64_t uniqueId,
                                      uint32_t gcPolicy) {
  GCContext* ret = rm_calloc(1, sizeof(GCContext));
  switch (gcPolicy) {
    case GCPolicy_Fork:
      ret->gcCtx = FGC_NewFromSpec(sp, uniqueId, &ret->callbacks);
      break;
    case GCPolicy_Sync:
    default:
      // currently LLAPI only support FORK_GC, in the future we might allow default GC as well.
      // This is why we pass the GC_POLICY to the function.
      RS_LOG_ASSERT(0, "Invalid GC policy");
  }
  return ret;
}

GCContext* GCContext_CreateGC(RedisModuleString* keyName, float initialHZ, uint64_t uniqueId) {
  GCContext* ret = rm_calloc(1, sizeof(GCContext));
  switch (RSGlobalConfig.gcPolicy) {
    case GCPolicy_Fork:
      ret->gcCtx = FGC_New(keyName, uniqueId, &ret->callbacks);
      break;
    case GCPolicy_Sync:
    default:
      ret->gcCtx = NewGarbageCollector(keyName, initialHZ, uniqueId, &ret->callbacks);
      break;
  }
  return ret;
}

static void stopGC(GCContext* gc) {
  gc->stopped = 1;
  if (gc->callbacks.kill) {
    gc->callbacks.kill(gc->gcCtx);
  }  
}

static void timerCallback(RedisModuleCtx* ctx, void* data);

static long long getNextPeriod(GCContext* gc) {
  struct timespec interval = gc->callbacks.getInterval(gc->gcCtx);
  long long ms = interval.tv_sec * 1000 + interval.tv_nsec / 1000000;  // convert to millisecond
  return ms;
}

static RedisModuleTimerID scheduleNext(GCTask *task) {
  if (!RedisModule_CreateTimer) return 0;

  long long period = getNextPeriod(task->gc);
  return RedisModule_CreateTimer(RSDummyContext, period, timerCallback, task);
}

static void threadCallback(void* data) {
  GCTask* task= data;
  GCContext* gc = task->gc;
  RedisModuleBlockedClient* bc = task->bClient;
  RedisModuleCtx* ctx = RSDummyContext;

  if (gc->stopped) {
    // if the client is blocked, lets release it
    if (bc) {
      RedisModule_ThreadSafeContextLock(ctx);
      RedisModule_UnblockClient(bc, NULL);
      RedisModule_ThreadSafeContextUnlock(ctx); 
    }
    rm_free(task);
    return;
  }

  int ret = gc->callbacks.periodicCallback(ctx, gc->gcCtx);

  RedisModule_ThreadSafeContextLock(ctx);

  // if GC was invoke by debug command, we release the client
  // and terminate without rescheduling the task again.
  if (task->debug) { 
    if (bc) {
      RedisModule_UnblockClient(bc, NULL);
    }
    rm_free(task);
    goto end;
  }

  if (!ret || gc->stopped) {
    stopGC(gc);
    rm_free (task);
    goto end;
  }

  gc->timerID = scheduleNext(task);

end:
  RedisModule_ThreadSafeContextUnlock(ctx);
}

static void destroyCallback(void* data) {
  GCContext* gc = data;
  assert(gc->stopped == 1);

  RedisModule_ThreadSafeContextLock(RSDummyContext);
  gc->callbacks.onTerm(gc->gcCtx);
  rm_free(gc);
  RedisModule_ThreadSafeContextUnlock(RSDummyContext);
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
  thpool_add_work(gcThreadpool_g, threadCallback, data);
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
  if (!RedisModule_StopTimer) {
    // for fork gc debug
    RedisModule_FreeThreadSafeContext(((ForkGC *)gc->gcCtx)->ctx);
    free(gc->gcCtx);
    free(gc);
    return;
  }

  RedisModuleCtx* ctx = RSDummyContext;
  stopGC(gc);
  GCTask *data = NULL;

  if (RedisModule_StopTimer(ctx, gc->timerID, (void**)&data) == REDISMODULE_OK) {
    assert(data->gc == gc);
    rm_free(data);  // release task memory

    // free gc
    gc->callbacks.onTerm(gc->gcCtx);
    rm_free(gc);
    return;
  }
  thpool_add_work(gcThreadpool_g, destroyCallback, gc); 
}

void GCContext_RenderStats(GCContext* gc, RedisModuleCtx* ctx) {
  gc->callbacks.renderStats(ctx, gc->gcCtx);
}

void GCContext_OnDelete(GCContext* gc) {
  if (gc->callbacks.onDelete) {
    gc->callbacks.onDelete(gc->gcCtx);
  }
}

void GCContext_CommonForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc) {
  if (gc->stopped) {
    RedisModule_Log(RSDummyContext, "warning", "ForceInvokeGC command received after shut down");
    return;
  }

  GCTask *task = GCTaskCreate(gc, bc, 1);
  thpool_add_work(gcThreadpool_g, threadCallback, task);
}

void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc) {
  GCContext_CommonForceInvoke(gc, bc);
}

void GCContext_ForceBGInvoke(GCContext* gc) {
  GCContext_CommonForceInvoke(gc, NULL);
}

void GC_ThreadPoolStart() {
  if (gcThreadpool_g == NULL) {
    gcThreadpool_g = thpool_init(1);
  }
}

void GC_ThreadPoolDestroy() {
  if (gcThreadpool_g != NULL) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    thpool_destroy(gcThreadpool_g);
    gcThreadpool_g = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}