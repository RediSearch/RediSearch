#include "gc.h"
#include "fork_gc.h"
#include "default_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "module.h"
#include <pthread.h>
#include <assert.h>
#include <unistd.h>

threadpool gcThreadPools_g = NULL;

static volatile int gc_destroying = 0;

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

void GCContext_Timer_PeriodicCallback(RedisModuleCtx* ctx, void* data);

static long long getNextPeriod(GCContext* gc) {
  struct timespec interval = gc->callbacks.getInterval(gc->gcCtx);
  long long ms = interval.tv_sec * 1000 + interval.tv_nsec / 1000000;  // convert to millisecond
  return ms;
}

static RedisModuleTimerID scheduleNext(RedisModuleCtx* ctx, GCContext** gc) {
  if (!RedisModule_CreateTimer) return 0;
  long long period = getNextPeriod(*gc);
  return RedisModule_CreateTimer(ctx, period, GCContext_Timer_PeriodicCallback, gc);
}

static void freeGCInternals(GCContext** gcp) {
  GCContext* gc = *gcp;
  RedisModuleBlockedClient* bClient = BlockClients_pop(&gc->bClients);
  if (bClient) {
    RedisModule_UnblockClient(bClient, NULL);
  }
  gc->callbacks.onTerm(gc->gcCtx);
  gc->gcCtx = NULL;
  rm_free(gc);
  *gcp = NULL;
}

static void internal_PeriodicCallback(void* data) {
  GCContext** gcp = (GCContext**)data;
  GCContext* gc = *gcp;
  if (gc == NULL || gc_destroying) { // GC was freed
    return;
  }
  RedisModuleCtx* ctx = RSDummyContext;
  
  int ret = gc->callbacks.periodicCallback(ctx, gc->gcCtx);

  RedisModule_ThreadSafeContextLock(ctx);
  RedisModuleBlockedClient* bClient = BlockClients_pop(&gc->bClients);
  if (bClient) {
    RedisModule_UnblockClient(bClient, NULL);
  }
  if (!ret) {
    gc->callbacks.onTerm(gc->gcCtx);
    gc->gcCtx = NULL;
    rm_free(gc);
    *gcp = NULL;
    RedisModule_ThreadSafeContextUnlock(ctx);
    return;
  }
  gc->timerID = scheduleNext(ctx, gcp);
  RedisModule_ThreadSafeContextUnlock(ctx);
}

void GCContext_Timer_PeriodicCallback(RedisModuleCtx* ctx, void* data) {
  GCContext* gc = *(GCContext**)data;
  if (gc == NULL) { // GC was freed
    return;
  }

  if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
    // If slave traffic is not allow it means that there is a state machine running
    // we do not want to run any GC which might cause a FORK process to start for example).
    // Its better to just avoid it.
    gc->timerID = scheduleNext(ctx, data);
    return;
  }
  thpool_add_work(gcThreadPools_g, internal_PeriodicCallback, data);
}

void GCContext_Start(GCContext** gcp) {
  GCContext* gc = *gcp;
  assert(gc);
  struct timespec interval = gc->callbacks.getInterval(gc->gcCtx);
  RedisModuleCtx* ctx = RSDummyContext;
  gc->timerID = scheduleNext(ctx, gcp);
}

void GCContext_Stop(GCContext** gcp) {
  GCContext* gc = *gcp;
  if (gc == NULL) { // GC was freed
    return;
  }
  if (!RedisModule_StopTimer) {
    // for debug
    free(gc->gcCtx);
    free(gc);
    return;
  }

  RedisModuleCtx* ctx = RSDummyContext;
  if (RedisModule_StopTimer(ctx, gc->timerID, NULL) == REDISMODULE_OK) {
    freeGCInternals(gcp);
  }
}

void GCContext_RenderStats(GCContext* gc, RedisModuleCtx* ctx) {
  gc->callbacks.renderStats(ctx, gc->gcCtx);
}

void GCContext_OnDelete(GCContext* gc) {
  if (gc->callbacks.onDelete) {
    gc->callbacks.onDelete(gc->gcCtx);
  }
}

void GCContext_ForceInvoke(GCContext** gcp, RedisModuleBlockedClient* bc) {
  GCContext* gc = *gcp;
  if (gc == NULL) { // GC was freed
    return;
  }
  BlockClients_push(&gc->bClients, bc);
  GCContext_ForceBGInvoke(gcp);
}

void GCContext_ForceBGInvoke(GCContext** gcp) {
  GCContext* gc = *gcp;
  if (gc == NULL) { // GC was freed
    return;
  }
  void* gcCtx;
  RedisModuleCtx* ctx = RSDummyContext;
  if (RedisModule_StopTimer(ctx, gc->timerID, &gcCtx) == REDISMODULE_OK) {
    assert(gcp == gcCtx);
    thpool_add_work(gcThreadPools_g, internal_PeriodicCallback, gcp);
  }
  usleep(10000);
}

void GC_ThreadPoolStart() {
  if (gcThreadPools_g == NULL) {
    gc_destroying = 0;
    gcThreadPools_g = thpool_init(RSGlobalConfig.forkGcThreadPoolSize);
  }
}

void GC_ThreadPoolDestroy() {
  RedisModule_ThreadSafeContextUnlock(RSDummyContext);
  gc_destroying = 1;
  thpool_destroy(gcThreadPools_g);
  gcThreadPools_g = NULL;
  RedisModule_ThreadSafeContextLock(RSDummyContext);
}