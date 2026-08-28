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
 * Whether this server can tell us which fields a hash command touched, i.e. whether
 * `Initialize_KeyspaceNotifications` will take the subkey channel for hash events.
 *
 * Reports the server capability rather than whether the subscription has happened,
 * because the subscription is lazy — it waits for the first index — while callers
 * need an answer that does not depend on when they ask.
 *
 * Against a Redis predating subkey notifications the module degrades silently to
 * reindexing the whole document, so this is what distinguishes "the change set said
 * nothing changed" from "there was no change set". Exposed for
 * `_FT.DEBUG HASH_SUBKEY_NOTIFICATIONS`: without it a test for change-set-driven
 * behavior cannot tell an unsupported server from a broken one, and neither can an
 * operator.
 */
bool HashSubkeyNotificationsSupported(void);

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
