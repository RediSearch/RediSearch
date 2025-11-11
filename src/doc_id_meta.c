/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "doc_id_meta.h"

static int docIdMetaCopy(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  return 1;
}

static RedisModuleKeyMetaClassId docIdKeyMetaClassId;
static RedisModuleKeyMetaClassConfig docIdKeyMetaClassIdConfig = {
    .version = 1,
    .reset_value = 0,
    .flags = 0,
    .copy = (RedisModuleKeyMetaCopyFunc)docIdMetaCopy,
    .rename = NULL, // If NULL, meta is kept during rename
    .move = NULL, // If NULL, meta is kept during move
    .unlink = NULL, // If NULL, meta is ignored during unlink
    .free = NULL, // No dynamic memory
    // TODO(Joan): Ask clarification for these callbacks
    .defrag = NULL,
    .mem_usage = NULL,
    .free_effort = NULL,
    // Since for now RediSearch indices are rebuilt during persistence, we don't need to consider persistence now.
    // (DocID would be added to key during Notification callbacks and indexing procedures as normal)
    .rdb_load = NULL,
    .rdb_save = NULL,
    .aof_rewrite = NULL,
};

void DocIdMeta_Init(RedisModuleCtx *ctx) {
  docIdKeyMetaClassId = RedisModule_CreateKeyMetaClass(ctx, "docId", 1, &docIdKeyMetaClassIdConfig);
}

int DocIdMeta_Set(RedisModuleKey *key, uint64_t docId) {
  return RedisModule_SetKeyMeta(key, docIdKeyMetaClassId, &docId);
}

uint64_t DocIdMeta_Get(RedisModuleKey *key) {
  uint64_t docId;
  RedisModule_GetKeyMeta(key, docIdKeyMetaClassId, &docId);
  return docId;
}
