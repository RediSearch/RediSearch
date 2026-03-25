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
void DocIdMeta_Init(RedisModuleCtx *ctx, bool diskEnabled);

/*
 * Set the docId for the given key and index spec.
 * @param ctx The Redis module context
 * @param keyName The key name to set the docId for
 * @param specName The index spec name (used to identify which index this docId belongs to)
 * @param specNameLen The length of the spec name
 * @param docId The docId to set
 * @return REDISMODULE_OK if the docId was set, REDISMODULE_ERR otherwise
*/
int DocIdMeta_Set(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  const char *specName, size_t specNameLen, uint64_t docId);

/*
 * Get the docId for the given key and index spec.
 * @param ctx The Redis module context
 * @param keyName The key name to get the docId for
 * @param specName The index spec name
 * @param specNameLen The length of the spec name
 * @param docId Output parameter for the docId
 * @return REDISMODULE_OK if the docId was found, REDISMODULE_ERR otherwise
*/
int DocIdMeta_Get(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  const char *specName, size_t specNameLen, uint64_t *docId);

/*
 * Delete the docId for the given key and index spec.
 * @param ctx The Redis module context
 * @param keyName The key name to delete the docId for
 * @param specName The index spec name
 * @param specNameLen The length of the spec name
 * @return REDISMODULE_OK if the docId was found and deleted, REDISMODULE_ERR otherwise
*/
int DocIdMeta_Delete(RedisModuleCtx *ctx, RedisModuleString *keyName,
                     const char *specName, size_t specNameLen);

// Functions exposed to ease unit testing
int docIdMetaRDBLoad(RedisModuleIO *rdb, uint64_t *meta, int encver);
void docIdMetaRDBSave(RedisModuleIO *rdb, void *value, uint64_t *meta);
RedisModuleKeyMetaClassId DocIdMeta_GetClassId();

#ifdef __cplusplus
}
#endif
