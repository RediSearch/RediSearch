#ifndef __SEARCH_CTX_H
#define __SEARCH_CTX_H

#include "redismodule.h"
#include "spec.h"

/** Context passed to all redis related search handling functions. */
typedef struct {
  RedisModuleCtx *redisCtx;
  IndexSpec *spec;

} RedisSearchCtx;

#endif
