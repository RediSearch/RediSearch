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

/**
 * Whether hash events are, or will be, served over the subkey channel — i.e. whether this
 * server can tell us which fields a hash command touched.
 */
bool HashSubkeyNotificationsSupported(void);

/**
 * Take hash events over the plain channel even where the subkey channel is available, so
 * the degraded path an older Redis takes can be tested. Testing only, reached solely from
 * `_FT.DEBUG FORCE_PLAIN_HASH_NOTIFICATIONS`.
 *
 * Returns false, having changed nothing, once the subscription has been made: the channel is
 * chosen at the first index and cannot be changed afterwards. A caller has to set this
 * before creating any index.
 */
bool ForcePlainHashNotifications_Set(bool force);

int KeySpaceNotificationCallback(RedisModuleCtx *ctx, int type, const char *event,
                               RedisModuleString *key);
void KeySpaceNotificationWithSubkeysCallback(RedisModuleCtx *ctx, int type, const char *event,
                                             RedisModuleString *key, RedisModuleString **subkeys,
                                             int count);
void Initialize_KeyspaceNotifications();
void Initialize_ServerEventNotifications(RedisModuleCtx *ctx);
void Initialize_RdbNotifications(RedisModuleCtx *ctx);
void Initialize_RoleChangeNotifications(RedisModuleCtx *ctx);
void RDB_LoadingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);
void LoadingProgressCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);

#ifdef __cplusplus
}
#endif
