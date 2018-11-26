#ifndef __SEARCH_CTX_H
#define __SEARCH_CTX_H

#include <sched.h>

#include "redismodule.h"
#include "spec.h"
#include "trie/trie_type.h"
#include <time.h>

#if defined(__FreeBSD__)
#define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

/** Context passed to all redis related search handling functions. */
typedef struct {
  RedisModuleCtx *redisCtx;
  RedisModuleKey *key;
  RedisModuleString *keyName;
  IndexSpec *spec;
} RedisSearchCtx;

#define SEARCH_CTX_STATIC(ctx, sp) \
  (RedisSearchCtx) {               \
    .redisCtx = ctx, .spec = sp    \
  }

#define SEARCH_CTX_SORTABLES(ctx) ((ctx && ctx->spec) ? ctx->spec->sortables : NULL)
// Create a string context on the heap
RedisSearchCtx *NewSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL);
RedisSearchCtx *NewSearchCtxDefault(RedisModuleCtx *ctx);

RedisSearchCtx *SearchCtx_Refresh(RedisSearchCtx *sctx, RedisModuleString *keyName);

// Same as above, only from c string (null terminated)
RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);

void SearchCtx_Free(RedisSearchCtx *sctx);
#endif
