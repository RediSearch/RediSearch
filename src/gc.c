#include "gc.h"
#include "fork_gc.h"
#include "default_gc.h"
#include "config.h"
#include "redismodule.h"
#include "rmalloc.h"
#include <pthread.h>
#include <assert.h>

static void BlockClients_push(BlockClients* ctx, RedisModuleBlockedClient* bClient) {
  pthread_mutex_lock(&ctx->lock);
  BlockClient* bc = rm_calloc(1, sizeof(BlockClient));
  bc->bClient = bClient;

  if (ctx->head == NULL) {
    ctx->head = ctx->tail = bc;
    pthread_mutex_unlock(&ctx->lock);
    return;
  }

  bc->next = ctx->head;
  ctx->head->prev = bc;
  ctx->head = bc;
  pthread_mutex_unlock(&ctx->lock);
}

static RedisModuleBlockedClient* BlockClients_pop(BlockClients* ctx) {
  pthread_mutex_lock(&ctx->lock);
  BlockClient* bc = ctx->tail;
  if (!bc) {
    pthread_mutex_unlock(&ctx->lock);
    return NULL;
  }

  ctx->tail = bc->prev;
  if (ctx->tail) {
    ctx->tail->next = NULL;
  } else {
    ctx->head = NULL;
  }

  RedisModuleBlockedClient* ret = bc->bClient;
  rm_free(bc);

  pthread_mutex_unlock(&ctx->lock);
  return ret;
}

GCContext* GCContext_CreateGCFromSpec(IndexSpec* sp, float initialHZ, uint64_t uniqueId,
                                      uint32_t gcPolicy) {
  GCContext* ret = rm_calloc(1, sizeof(GCContext));
  pthread_mutex_init(&ret->bClients.lock, NULL);
  switch (gcPolicy) {
    case GCPolicy_Fork:
      ret->gcCtx = NewForkGCFromSpec(sp, uniqueId, &ret->callbacks);
      break;
    case GCPolicy_Default:
    default:
      // currently LLAPI only support FORK_GC, in the future we might allow default GC as well.
      // This is why we pass the GC_POLICY to the function.
      assert(0);
  }
  return ret;
}

GCContext* GCContext_CreateGC(RedisModuleString* keyName, float initialHZ, uint64_t uniqueId) {
  GCContext* ret = rm_calloc(1, sizeof(GCContext));
  pthread_mutex_init(&ret->bClients.lock, NULL);
  switch (RSGlobalConfig.gcPolicy) {
    case GCPolicy_Fork:
      ret->gcCtx = NewForkGC(keyName, uniqueId, &ret->callbacks);
      break;
    case GCPolicy_Default:
    default:
      ret->gcCtx = NewGarbageCollector(keyName, initialHZ, uniqueId, &ret->callbacks);
      break;
  }
  return ret;
}

static int GCContext_PeriodicCallback(RedisModuleCtx* ctx, void* privdata) {
  GCContext* gc = privdata;
  int ret = gc->callbacks.periodicCallback(ctx, gc->gcCtx);

  RedisModuleBlockedClient* bClient = BlockClients_pop(&gc->bClients);
  if (bClient) {
    RedisModule_UnblockClient(bClient, NULL);
  }

  RMUtilTimer_SetInterval(gc->timer, gc->callbacks.getInterval(gc->gcCtx));

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
  if (gc->timer) {
    RMUtilTimer_Terminate(gc->timer);
  }
}

void GCContext_RenderStats(GCContext* gc, RedisModuleCtx* ctx) {
  gc->callbacks.renderStats(ctx, gc->gcCtx);
}

void GCContext_OnDelete(GCContext* gc) {
  gc->callbacks.onDelete(gc->gcCtx);
}

void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc) {
  BlockClients_push(&gc->bClients, bc);
  RMUtilTimer_ForceInvoke(gc->timer);
}
