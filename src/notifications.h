/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "redismodule.h"

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Whether the per-index disk consistency hooks are currently open. An index created while
// they are must open its own window, or the close at POST_FORK finds it unpaired.
bool DiskConsistencyWindow_IsIndexWindowOpen(void);

int KeySpaceNotificationCallback(RedisModuleCtx *ctx, int type, const char *event,
                               RedisModuleString *key);
void Initialize_KeyspaceNotifications();
void Initialize_ServerEventNotifications(RedisModuleCtx *ctx);
void Initialize_CommandFilter(RedisModuleCtx *ctx);
void Initialize_RdbNotifications(RedisModuleCtx *ctx);
void Initialize_RoleChangeNotifications(RedisModuleCtx *ctx);
void RDB_LoadingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);
void LoadingProgressCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);

#ifdef __cplusplus
}
#endif
