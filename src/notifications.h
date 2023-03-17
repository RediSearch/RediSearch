/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redismodule.h"

int HashNotificationCallback(RedisModuleCtx *ctx, int type, const char *event,
                             RedisModuleString *key);
void Initialize_KeyspaceNotifications(RedisModuleCtx *ctx);
void Initialize_CommandFilter(RedisModuleCtx *ctx);
void Initialize_RdbNotifications(RedisModuleCtx *ctx);
void Initialize_RoleChangeNotifications(RedisModuleCtx *ctx);
