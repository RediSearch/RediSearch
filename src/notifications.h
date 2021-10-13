#pragma once

#include "redismodule.h"

int HashNotificationCallback(RedisModuleCtx *ctx, int type, const char *event,
                             RedisModuleString *key);
void Initialize_KeyspaceNotifications(RedisModuleCtx *ctx);
void Initialize_CommandFilter(RedisModuleCtx *ctx);
void Initialize_RdbNotifications(RedisModuleCtx *ctx);
