/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include <stdbool.h>
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

// Initialize the DocIdMeta module
void DocIdMeta_Init(RedisModuleCtx *ctx);

/*
 * Set the docId for the given key and index spec.
 * @param ctx The Redis module context
 * @param keyName The key name to set the docId for
 * @param specId The unique incarnation ID of the index spec
 * @param docId The docId to set
 * @return REDISMODULE_OK if the docId was set, REDISMODULE_ERR otherwise
*/
int DocIdMeta_Set(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  uint64_t specId, uint64_t docId);

/*
 * Get the docId for the given key and index spec.
 * @param ctx The Redis module context
 * @param keyName The key name to get the docId for
 * @param specId The unique incarnation ID of the index spec
 * @param docId Output parameter for the docId
 * @return REDISMODULE_OK if the docId was found, REDISMODULE_ERR otherwise
*/
int DocIdMeta_Get(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  uint64_t specId, uint64_t *docId);

/*
 * Delete the docId entry for the given key and index spec.
 * Unlike soft-delete, this removes the spec entry from the metadata map.
 * @param ctx The Redis module context
 * @param keyName The key name to delete the docId for
 * @param specId The unique incarnation ID of the index spec
 * @return REDISMODULE_OK if the entry was found and deleted, REDISMODULE_ERR otherwise
*/
int DocIdMeta_Delete(RedisModuleCtx *ctx, RedisModuleString *keyName, uint64_t specId);

/*
 * Open-key variants of Set/Get/Delete.
 *
 * These operate on a RedisModuleKey that the caller has already opened, instead
 * of opening (and closing) the key by name internally. Use them on hot paths
 * where the key is already open and pinned - e.g. the async scan key callback,
 * where the engine hands us an open key for the duration of the call - to avoid
 * a redundant reopen of the same key.
 *
 * The caller retains ownership of `key` and is responsible for opening it with
 * appropriate flags (read for Get, read+write for Set/Delete) and closing it.
 * The name-based variants above are thin wrappers that open the key and delegate
 * to these.
 */
int DocIdMeta_SetWithOpenKey(RedisModuleKey *key, uint64_t specId, uint64_t docId);
int DocIdMeta_GetWithOpenKey(RedisModuleKey *key, uint64_t specId, uint64_t *docId);
int DocIdMeta_DeleteWithOpenKey(RedisModuleKey *key, uint64_t specId);

// Set the persistence-in-progress flag. When true, RDB save/load callbacks
// become no-ops. Called from notifications.c during persistence events.
void DocIdMeta_SetForgetDocIdMetadata(bool inProgress);

// MOD-16954 instrumentation: aggregate RDB save/load counters for DocIdMeta.
// Reset at the start of a persistence/loading session and logged at its end,
// from notifications.c, to see how much per-key docId metadata crosses the wire
// on the SST path (where it is kept) vs is dropped on the plain-RDB path.
void DocIdMeta_ResetSaveStats(void);
void DocIdMeta_LogSaveStats(const char *phase);
void DocIdMeta_ResetLoadStats(void);
void DocIdMeta_LogLoadStats(const char *phase);

#ifdef __cplusplus
}
#endif
