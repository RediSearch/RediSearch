#ifndef __SEARCH_CTX_H
#define __SEARCH_CTX_H

#include <sched.h>

#include "redismodule.h"
#include "spec.h"
#include "trie/trie_type.h"
#include <time.h>

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
RedisSearchCtx *NewSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName);
void SearchCtx_Free(RedisSearchCtx *sctx);
#endif
