#ifndef __SEARCH_CTX_H
#define __SEARCH_CTX_H

#include "redismodule.h"
#include "spec.h"
#include "trie/trie_type.h"
/** Context passed to all redis related search handling functions. */
typedef struct {
  RedisModuleCtx *redisCtx;
  IndexSpec *spec;
} RedisSearchCtx;

#endif
