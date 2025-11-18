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

#ifdef __cplusplus
extern "C" {
#endif

// Initialize the DocIdMeta module
void DocIdMeta_Init(RedisModuleCtx *ctx);

/*
 * TODO: Review if idx is a good API
 * Set the docId for the given key and index. If the key already has a docId for the given index, overwrite it.
 * If the key does not have a docId for the given index, add it.
 * @param key The key to set the docId for
 * @param idx The index (as position in the index global array) to set the docId for
 * @param docId The docId to set
 * @return REDISMODULE_OK if the docId was set, REDISMODULE_ERR otherwise
*/
int DocIdMeta_SetDocIdForIndex(RedisModuleKey *key, size_t idx, uint64_t docId);

/*
* Get the docId for the given key and index.
* @param key The key to get the docId for
* @param idx The index (as position in the index global array) to get the docId for
* @param docId The docId to get
* @return REDISMODULE_OK if the docId was found, REDISMODULE_ERR otherwise
*/
int DocIdMeta_GetDocIdForIndex(RedisModuleKey *key, size_t idx, uint64_t *docId);


/*
* Delete the docId for the given key and index.
* @param key The key to get the docId for
* @param idx The index (as position in the index global array) to get the docId for
* @return REDISMODULE_OK if the docId was found and deleted, REDISMODULE_ERR otherwise
*/
int DocIdMeta_DeleteDocIdForIndex(RedisModuleKey *key, size_t idx);

#ifdef __cplusplus
}
#endif
