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
 * Delete the docId for the given key and index spec.
 * @param ctx The Redis module context
 * @param keyName The key name to delete the docId for
 * @param specId The unique incarnation ID of the index spec
 * @return REDISMODULE_OK if the docId was found and deleted, REDISMODULE_ERR otherwise
*/
int DocIdMeta_Delete(RedisModuleCtx *ctx, RedisModuleString *keyName,
                     uint64_t specId);

// Subscribe to persistence events to disable RDB save/load during BGSAVE/AOF rewrite.
void DocIdMeta_SubscribePersistenceEvent(RedisModuleCtx *ctx);

/*
 * Track a dropped specId with a refcount equal to the number of keys
 * that still carry a DocIdEntry for this specId.
 * If refcount is 0, the entry is not added (no cleanup needed).
 */
void DocIdMeta_TrackDroppedSpecId(uint64_t specId, size_t refcount);

/*
 * Check if a specId belongs to a dropped index that is still being cleaned up.
 */
bool DocIdMeta_IsDroppedSpecId(uint64_t specId);

/*
 * Decrement the refcount for a dropped specId. When refcount reaches 0,
 * the tracking entry is removed (all keys have been cleaned).
 */
void DocIdMeta_DecrDroppedRefcount(uint64_t specId);

/*
 * Clear all dropped specId tracking entries.
 * Called on full RDB load and FLUSHDB.
 */
void DocIdMeta_ClearDroppedSpecIds(void);

/*
 * Save the dropped specIds set to RDB (auxiliary data).
 */
void DocIdMeta_DroppedSpecIdsRdbSave(RedisModuleIO *rdb);

/*
 * Load the dropped specIds set from RDB (auxiliary data).
 */
int DocIdMeta_DroppedSpecIdsRdbLoad(RedisModuleIO *rdb);

// Functions exposed to ease unit testing
int docIdMetaRDBLoad(RedisModuleIO *rdb, uint64_t *meta, int encver);
void docIdMetaRDBSave(RedisModuleIO *rdb, void *value, uint64_t *meta);
RedisModuleKeyMetaClassId DocIdMeta_GetClassId();

#ifdef __cplusplus
}
#endif
