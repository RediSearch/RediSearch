
#ifndef SRC_GC_H_
#define SRC_GC_H_

#include "redismodule.h"

typedef struct BlockClient {
  RedisModuleBlockedClient* bClient;
  struct BlockClient* next;
  struct BlockClient* prev;
} BlockClient;

typedef struct BlockClients {
  BlockClient* head;
  BlockClient* tail;
  pthread_mutex_t lock;
} BlockClients;

typedef struct GCCallbacks {
  int (*periodicCallback)(RedisModuleCtx* ctx, void* gcCtx);
  void (*renderStats)(RedisModuleCtx* ctx, void* gc);
  void (*onDelete)(void* ctx);
  void (*onTerm)(void* ctx);
  struct timespec (*getInterval)(void* ctx);
} GCCallbacks;

typedef struct GCContext {
  void* gcCtx;
  struct RMUtilTimer* timer;
  BlockClients bClients;
  GCCallbacks callbacks;
} GCContext;

typedef struct IndexSpec IndexSpec;
GCContext* GCContext_CreateGCFromSpec(IndexSpec* sp, float initialHZ, uint64_t uniqueId,
                                      uint32_t gcPolicy);
GCContext* GCContext_CreateGC(RedisModuleString* keyName, float initialHZ, uint64_t uniqueId);
void GCContext_Start(GCContext* gc);
void GCContext_Stop(GCContext* gc);
void GCContext_RenderStats(GCContext* gc, RedisModuleCtx* ctx);
void GCContext_OnDelete(GCContext* gc);
void GCContext_ForceInvoke(GCContext* gc, RedisModuleBlockedClient* bc);

#endif /* SRC_GC_H_ */
