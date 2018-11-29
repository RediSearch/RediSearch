#ifndef __SEARCH_CTX_H
#define __SEARCH_CTX_H

#include <sched.h>

#include "redismodule.h"
#include "spec.h"
#include "concurrent_ctx.h"
#include "trie/trie_type.h"
#include <time.h>

typedef struct RedisSearchCtx {
  RedisModuleCtx *redisCtx;
  RedisModuleKey *key;
  RedisModuleString *keyName;
  IndexSpec *spec;
  ConcurrentSearchCtx *conc;
  uint32_t refcount;
  int isStatic;
} RedisSearchCtx;

#define SEARCH_CTX_STATIC(ctx, sp)             \
  (RedisSearchCtx) {                           \
    .redisCtx = ctx, .spec = sp, .isStatic = 1 \
  }

#define SEARCH_CTX_SORTABLES(ctx) ((ctx && ctx->spec) ? ctx->spec->sortables : NULL)
// Create a string context on the heap
RedisSearchCtx *NewSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName);
RedisSearchCtx *NewSearchCtxDefault(RedisModuleCtx *ctx);

RedisSearchCtx *SearchCtx_Refresh(RedisSearchCtx *sctx, RedisModuleString *keyName);

// Same as above, only from c string (null terminated)
RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName);

#define SearchCtx_Incref(sctx) (sctx)->refcount++

#define SearchCtx_Decref(sctx) \
  if (!--((sctx)->refcount)) { \
    SearchCtx_Free(sctx);      \
  }

void SearchCtx_Free(RedisSearchCtx *sctx);
#endif
