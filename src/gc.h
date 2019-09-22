
#ifndef SRC_GC_H_
#define SRC_GC_H_

#include "redismodule.h"
#include "util/dllist.h"
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct BlockClient {
  DLLIST_node llnode;
  RedisModuleBlockedClient* bClient;
} BlockClient;

typedef struct BlockClients {
  DLLIST clients;
  pthread_mutex_t lock;
} BlockClients;

typedef struct GCCallbacks {
  int (*periodicCallback)(RedisModuleCtx* ctx, void* gcCtx);
  void (*renderStats)(RedisModuleCtx* ctx, void* gc);
  void (*onDelete)(void* ctx);
  void (*onTerm)(void* ctx);

  // Send a "kill signal" to the GC, requesting it to terminate asynchronously
  void (*kill)(void* ctx);
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
void GCContext_ForceBGInvoke(GCContext* gc);

#ifdef __cplusplus
}
#endif
#endif /* SRC_GC_H_ */
