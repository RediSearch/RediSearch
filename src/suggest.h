/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RS_SUGGEST_H
#define RS_SUGGEST_H
#include "redismodule.h"
#ifdef __cplusplus
extern "C" {
#endif

int RSSuggestAddCommand(RedisModuleCtx *, RedisModuleString **, int);
int RSSuggestDelCommand(RedisModuleCtx *, RedisModuleString **, int);
int RSSuggestLenCommand(RedisModuleCtx *, RedisModuleString **, int);
int RSSuggestGetCommand(RedisModuleCtx *, RedisModuleString **, int);

#ifdef __cplusplus
}
#endif
#endif