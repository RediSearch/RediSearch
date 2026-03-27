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
#include "obfuscation/hidden.h"

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
 * @param specName The index spec name (stored for O(1) lookup in specDict_g)
 * @return REDISMODULE_OK if the docId was set, REDISMODULE_ERR otherwise
*/
int DocIdMeta_Set(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  uint64_t specId, uint64_t docId,
                  const HiddenString *specName);

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
 * Soft-delete the docId for the given key and index spec.
 * This invalidates the docId by setting it to DOCID_META_INVALID, but keeps
 * the entry in the hashmap. This allows efficient reuse if the same key is
 * re-indexed to the same spec (DocIdMeta_Set will reuse the existing entry).
 * @param ctx The Redis module context
 * @param keyName The key name to soft-delete the docId for
 * @param specId The unique incarnation ID of the index spec
 * @return REDISMODULE_OK if the docId was found and invalidated, REDISMODULE_ERR otherwise
*/
int DocIdMeta_SoftDelete(RedisModuleCtx *ctx, RedisModuleString *keyName,
                         uint64_t specId);

// Subscribe to persistence events to disable RDB save/load during BGSAVE/AOF rewrite.
void DocIdMeta_SubscribePersistenceEvent(RedisModuleCtx *ctx);

// Functions exposed to ease unit testing
int docIdMetaRDBLoad(RedisModuleIO *rdb, uint64_t *meta, int encver);
void docIdMetaRDBSave(RedisModuleIO *rdb, void *value, uint64_t *meta);
RedisModuleKeyMetaClassId DocIdMeta_GetClassId();

/*
 * Unlink callback - called when a key is being deleted from the DB.
 * Iterates through all entries and calls IndexSpec_DeleteDocById for each
 * valid entry (skipping soft-deleted entries and entries whose spec no longer exists).
 * After processing, marks all entries as invalid.
 * Exposed for unit testing.
 */
void docIdMetaUnlink(RedisModuleKeyOptCtx *ctx, uint64_t *meta);

#ifdef __cplusplus
}
#endif
