#include "gc.h"
#include "fork_gc.h"
#include "default_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include <pthread.h>
#include <assert.h>

threadpool gcThreadPools_g = NULL;

static void BlockClients_push(BlockClients* ctx, RedisModuleBlockedClient* bClient) {
  BlockClient* bc = rm_calloc(1, sizeof(BlockClient));
  bc->bClient = bClient;
  pthread_mutex_lock(&ctx->lock);
  dllist_prepend(&ctx->clients, &bc->llnode);
  pthread_mutex_unlock(&ctx->lock);
}

static RedisModuleBlockedClient* BlockClients_pop(BlockClients* ctx) {
  pthread_mutex_lock(&ctx->lock);
  RedisModuleBlockedClient* ret = NULL;
  DLLIST_node* nn = dllist_pop_tail(&ctx->clients);
  if (nn) {
    BlockClient* bc = DLLIST_ITEM(nn, BlockClient, llnode);
    ret = bc->bClient;
    rm_free(bc);
  }
  pthread_mutex_unlock(&ctx->lock);
  return ret;
}

GCContext* GCContext_CreateGCFromSpec(IndexSpec* sp, float initialHZ, uint64_t uniqueId,
                                      uint32_t gcPolicy) {
  GCContext* ret = rm_calloc(1, sizeof(GCContext));
  pthread_mutex_init(&ret->bClients.lock, NULL);
  dllist_init(&ret->bClients.clients);
  switch (gcPolicy) {
    case GCPolicy_Fork:
      ret->gcCtx = FGC_NewFromSpec(sp, uniqueId, &ret->callbacks);
      break;
    case GCPolicy_Sync:
    default:
      // currently LLAPI only support FORK_GC, in the future we might allow default GC as well.
      // This is why we pass the GC_POLICY to the function.
      assert(0);
  }
  return ret;
}

GCContext* GCContext_CreateGC(RedisModuleString* keyName, float initialHZ, uint64_t uniqueId) {
  GCContext* ret = rm_calloc(1, sizeof(GCContext));
  dllist_init(&ret->bClients.clients);
  pthread_mutex_init(&ret->bClients.lock, NULL);
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

void GCContext_Timer_PeriodicCallback(RedisModuleCtx *ctx, void *data);

static long long getNextPeriod(GCContext *gc) {
  struct timespec interval = gc->callbacks.getInterval(gc->gcCtx);
  long long ms = interval.tv_sec * 1000 + interval.tv_nsec / 1000000; // convert to millisecond
  return ms;
}

static RedisModuleTimerID scheduleNext(RedisModuleCtx *ctx, GCContext *gc) {
  long long period = getNextPeriod(gc);
  return RedisModule_CreateTimer(ctx, period, GCContext_Timer_PeriodicCallback, gc);
}

static void internal_PeriodicCallback(void* data) {
  GCContext *gc = data;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);

  int ret = gc->callbacks.periodicCallback(ctx, gc->gcCtx);

  RedisModule_ThreadSafeContextLock(ctx);
  RedisModuleBlockedClient* bClient = BlockClients_pop(&gc->bClients);
  if (bClient) {
    RedisModule_UnblockClient(bClient, NULL);
  }
  if (!ret) {
    gc->callbacks.onTerm(gc->gcCtx);
    RedisModule_ThreadSafeContextUnlock(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
    rm_free(gc);
    return;
  }
  gc->timerID = scheduleNext(ctx, gc);
  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_FreeThreadSafeContext(ctx);
}


void GCContext_Timer_PeriodicCallback(RedisModuleCtx *ctx, void *data) {
  if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
    // If slave traffic is not allow it means that there is a state machine running
    // we do not want to run any GC which might cause a FORK process to start for example).
    // Its better to just avoid it.
    GCContext *gc = data;
    gc->timerID = scheduleNext(ctx, gc);
    return;
  }
  thpool_add_work(gcThreadPools_g, internal_PeriodicCallback, data);  
}

void GCContext_Start(GCContext* gc) {
  struct timespec interval = gc->callbacks.getInterval(gc->gcCtx);
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  gc->timerID = scheduleNext(ctx, gc);
  RedisModule_FreeThreadSafeContext(ctx);
}

void GCContext_Stop(GCContext* gc) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModule_StopTimer(ctx, gc->timerID, NULL);
  gc->callbacks.onTerm(gc->gcCtx);
  RedisModule_FreeThreadSafeContext(ctx);
  /*if (gc->callbacks.kill) {
    gc->callbacks.kill(gc->gcCtx);
  }*/
  rm_free(gc);
}

void GCContext_RenderStats(GCContext* gc, RedisModuleCtx* ctx) {
  gc->callbacks.renderStats(ctx, gc->gcCtx);
}

void GCContext_OnDelete(GCContext* gc) {
  if (gc->callbacks.onDelete) {
    gc->callbacks.onDelete(gc->gcCtx);
  }
}

void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc) {
  BlockClients_push(&gc->bClients, bc);

  void *gcCtx;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModule_StopTimer(ctx, gc->timerID, &gcCtx);
  assert(gc == gcCtx);
  RedisModule_CreateTimer(ctx, 50, GCContext_Timer_PeriodicCallback, gc);
  RedisModule_FreeThreadSafeContext(ctx);
}

void GCContext_ForceBGInvoke(GCContext* gc) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModule_CreateTimer(ctx, 50, GCContext_Timer_PeriodicCallback, gc);
  RedisModule_FreeThreadSafeContext(ctx);
}

void GC_ThreadPoolStart() {
  if (gcThreadPools_g == NULL) {
    gcThreadPools_g = thpool_init(1);
  }
}

void GC_ThreadPoolDestroy() {
  thpool_destroy(gcThreadPools_g);
}