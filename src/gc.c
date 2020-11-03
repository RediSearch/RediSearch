
#include "gc.h"
#include "fork_gc.h"
#include "default_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "module.h"

#include "dep/thpool/thpool.h"
#include "rmutil/rm_assert.h"

#include <assert.h>
#include <unistd.h>
#include <pthread.h>

///////////////////////////////////////////////////////////////////////////////////////////////

static threadpool gcThreadpool_g = NULL;

//---------------------------------------------------------------------------------------------

GC::GC(IndexSpec* sp, float initialHZ, uint64_t uniqueId, uint32_t gcPolicy) {
  switch (gcPolicy) {
  case GCPolicy_Fork:
    gc = new ForkGC(sp, uniqueId);
    break;

  case GCPolicy_Sync:
  default:
    // currently LLAPI only support FORK_GC, in the future we might allow default GC as well.
    // This is why we pass the GC_POLICY to the function.
    RS_LOG_ASSERT(0, "Invalid GC policy");
  }
}

//---------------------------------------------------------------------------------------------

GC::GC(RedisModuleString* keyName, float initialHZ, uint64_t uniqueId) {
  switch (RSGlobalConfig.gcPolicy) {
  case GCPolicy_Fork:
    gc = new ForkGC(keyName, uniqueId);
    break;

  case GCPolicy_Sync:
  default:
    gc = new GarbageCollector(keyName, initialHZ, uniqueId);
    break;
  }
}

//---------------------------------------------------------------------------------------------

GC::~GC() {
  delete gc;
}

//---------------------------------------------------------------------------------------------

void GC::Kill() {
  stopped = true;
  gc->Kill();
}

//---------------------------------------------------------------------------------------------

long long GC::GetNextPeriod() {
  struct timespec interval = gc->GetInterval();
  long long ms = interval.tv_sec * 1000 + interval.tv_nsec / 1000000;  // convert to millisecond
  return ms;
}

//---------------------------------------------------------------------------------------------

RedisModuleTimerID GCTask::scheduleNext() {
  if (!RedisModule_CreateTimer) return 0;

  long long period = gc->GetNextPeriod();
  return RedisModule_CreateTimer(RSDummyContext, period, _timerCallback, this);
}

//---------------------------------------------------------------------------------------------

static void GCTask::_taskThread(GCTask *task) {
  GC* gc = task->gc;
  RedisModuleBlockedClient* bc = task->bClient;
  RedisModuleCtx* ctx = RSDummyContext;

  if (gc->stopped) {
    if (bc && bc != DEADBEEF) {
      RedisModule_ThreadSafeContextLock(ctx);
      RedisModule_UnblockClient(bc, NULL);
      RedisModule_ThreadSafeContextUnlock(ctx); 
    }
    delete task;
    return;
  }

  int periodic = gc->gc->PeriodicCallback(ctx);

  RedisModule_ThreadSafeContextLock(ctx);
  if (bc) { 
    if (bc != DEADBEEF) {
      RedisModule_UnblockClient(bc, NULL);
    }
    delete task;
    goto end;
  }

  if (!periodic || gc->stopped) {
    gc->Kill();
    delete task;
    goto end;
  }

  gc->timerID = task->scheduleNext();

end:
  RedisModule_ThreadSafeContextUnlock(ctx);
}

//---------------------------------------------------------------------------------------------

void GC::_destroyCallback(GC* gc) {
  RedisModuleCtx* ctx = RSDummyContext;
  assert(gc->stopped == 1);

  RedisModule_ThreadSafeContextLock(ctx);  
  gc->gc->OnTerm();
  delete gc;
  RedisModule_ThreadSafeContextUnlock(ctx);
}

//---------------------------------------------------------------------------------------------

void GCTask::_timerCallback(RedisModuleCtx* ctx, GCTask* task) {
  if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
    // If slave traffic is not allow it means that there is a state machine running
    // we do not want to run any GC which might cause a FORK process to start for example).
    // Its better to just avoid it.

    task->gc->timerID = task->scheduleNext();
    return;
  }
  thpool_add_work(gcThreadpool_g, _taskThread, task);
}

//---------------------------------------------------------------------------------------------

void GC::Start() {
  GCTask* task = new GCTask(this, NULL);
  timerID = task->scheduleNext();
  if (timerID == 0) {
    RedisModule_Log(RSDummyContext, "warning", "GC did not schedule next collection");
    delete task;
  }
}

//---------------------------------------------------------------------------------------------

void GC::Stop() {
  if (!RedisModule_StopTimer) {
    // for fork gc debug
    auto ctx = gc->GetRedisCtx();
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
    }
    delete this;
    return;
  }

  RedisModuleCtx* ctx = RSDummyContext;
  Stop();

  GCTask *task = NULL;
  if (RedisModule_StopTimer(ctx, timerID, (void**)&task) == REDISMODULE_OK) {
    assert(task->gc->gc == gc);
    delete task;
  }
  thpool_add_work(gcThreadpool_g, _destroyCallback, this); 
}

//---------------------------------------------------------------------------------------------

void GC::RenderStats(RedisModuleCtx* ctx) {
  gc->RenderStats(ctx);
}

//---------------------------------------------------------------------------------------------

void GC::OnDelete() {
  gc->OnDelete();
}

//---------------------------------------------------------------------------------------------

void GC::ForceInvoke(RedisModuleBlockedClient* bc) {
  if (stopped) {
    RedisModule_Log(RSDummyContext, "warning", "ForceInvokeGC command received after shut down");
    return;
  }

  thpool_add_work(gcThreadpool_g, GCTask::_taskThread, new GCTask(this, bc));
}

//---------------------------------------------------------------------------------------------

void GC::ForceBGInvoke() {
  ForceInvoke();
}

//---------------------------------------------------------------------------------------------

void GC::ThreadPoolStart() {
  if (gcThreadpool_g == NULL) {
    gcThreadpool_g = thpool_init(1);
  }
}

//---------------------------------------------------------------------------------------------

void GC::ThreadPoolDestroy() {
  if (gcThreadpool_g == NULL) {
    return;
  }
  RedisModule_ThreadSafeContextUnlock(RSDummyContext);
  thpool_destroy(gcThreadpool_g);
  gcThreadpool_g = NULL;
  RedisModule_ThreadSafeContextLock(RSDummyContext);
}

///////////////////////////////////////////////////////////////////////////////////////////////
