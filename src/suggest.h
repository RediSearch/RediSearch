#pragma once
#include "redismodule.h"

int RSSuggestAddCommand(RedisModuleCtx *, RedisModuleString **, int);
int RSSuggestDelCommand(RedisModuleCtx *, RedisModuleString **, int);
int RSSuggestLenCommand(RedisModuleCtx *, RedisModuleString **, int);
int RSSuggestGetCommand(RedisModuleCtx *, RedisModuleString **, int);
