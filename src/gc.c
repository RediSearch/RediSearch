#include "gc.h"
#include "fork_gc.h"
#include "default_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include <pthread.h>
#include <assert.h>

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

static int GCContext_PeriodicCallback(RedisModuleCtx* ctx, void* privdata) {
  if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
    // If slave trafic is not allow it means that there is a state machine running
    // we do not want to run any GC w(hich might cause a FORK process to start for example).
    // Its better to just avoid it.
    return 1;
  }

  GCContext* gc = privdata;
  int ret = gc->callbacks.periodicCallback(ctx, gc->gcCtx);

  RedisModuleBlockedClient* bClient = BlockClients_pop(&gc->bClients);
  if (bClient) {
    RedisModule_UnblockClient(bClient, NULL);
  }
  if (gc->timer) {
    // Timer could've been deleted..
    RMUtilTimer_SetInterval(gc->timer, gc->callbacks.getInterval(gc->gcCtx));
  }
  return ret;
}

static void GCContext_OnTerm(void* privdata) {
  GCContext* gc = privdata;
  gc->callbacks.onTerm(gc->gcCtx);
  rm_free(gc);
}

void GCContext_Start(GCContext* gc) {
  struct timespec interval = gc->callbacks.getInterval(gc->gcCtx);
  gc->timer = RMUtil_NewPeriodicTimer(GCContext_PeriodicCallback, GCContext_OnTerm, gc, interval);
}

void GCContext_Stop(GCContext* gc) {
  if (gc->callbacks.kill) {
    gc->callbacks.kill(gc->gcCtx);
  }
  RMUtilTimer_Terminate(gc->timer);
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
  RMUtilTimer_ForceInvoke(gc->timer);
}

void GCContext_ForceBGInvoke(GCContext* gc) {
  RMUtilTimer_Signal(gc->timer);
}
