/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "doc_id_meta.h"
#include "util/arr/arr.h"
#include "rdb.h"

#define DOCID_META_INVALID 0

static RedisModuleKeyMetaClassId docIdKeyMetaClassId;

// TODO (Joan): Should we use a hashmap instead of an array, where the key is the index name?
// Also review the growing policy (Should we care more for speed than space?)
// Can we rely on the index order to be constant, even considering RDB save/load?
struct DocIdMeta {
  arrayof(uint64_t) docId;
  size_t size;
};

static int docIdMetaCopy(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(meta);
  // We do not want to copy the meta, as the docID will not have meaning in the destination DB, or the new key will get reindexed in KeySpaceNotification
  return 0;
}

/* Free callback - called when metadata needs to be freed */
static void docIdMetaFree(const char *keyname, uint64_t meta) {
  REDISMODULE_NOT_USED(keyname);
  if (meta == 0) return;
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  array_free(docIdMeta->docId);
  rm_free(docIdMeta);
}

static int docIdMetaMove(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(meta);
  // We do not want to move the meta, as the docID will not have meaning in the destination DB
  return 0;
}

static int docIdMetaRDBLoad(RedisModuleIO *rdb, uint64_t *meta, int encver) {
  REDISMODULE_NOT_USED(encver);
  struct DocIdMeta *docIdMeta = NULL;

  // Load the size of the docId array
  size_t size = LoadUnsigned_IOError(rdb, goto cleanup);
  // Allocate the DocIdMeta structure
  docIdMeta = rm_malloc(sizeof(struct DocIdMeta));
  docIdMeta->size = size;
  if (size == 0) {
    docIdMeta->docId = NULL;
  } else {
    docIdMeta->docId = array_new(uint64_t, size);
    // Load each docId entry
    for (size_t i = 0; i < size; i++) {
      uint64_t docId = LoadUnsigned_IOError(rdb, goto cleanup);
      docIdMeta->docId[i] = docId;
    }
  }

  *meta = (uint64_t)docIdMeta;
  return REDISMODULE_OK;

cleanup:
  if (docIdMeta) {
    if (docIdMeta->docId) {
      array_free(docIdMeta->docId);
    }
    rm_free(docIdMeta);
  }
  *meta = 0;
  return REDISMODULE_ERR;
}

static void docIdMetaRDBSave(RedisModuleIO *rdb, void *value, uint64_t *meta) {
  REDISMODULE_NOT_USED(value);

  if (*meta == 0) {
    // No metadata to save - save size 0
    RedisModule_SaveUnsigned(rdb, 0);
    return;
  }

  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)*meta;

  // Save the size of the docId array
  RedisModule_SaveUnsigned(rdb, docIdMeta->size);

  // Save each docId entry
  for (size_t i = 0; i < docIdMeta->size; i++) {
    RedisModule_SaveUnsigned(rdb, docIdMeta->docId[i]);
  }
}

#define INITIAL_DOCID_META_SIZE 10

void DocIdMeta_Init(RedisModuleCtx *ctx) {
  RedisModuleKeyMetaClassConfig docIdKeyMetaClassIdConfig = {
    .version = 1,
    .reset_value = 0,
    .flags = 0,
    .copy = (RedisModuleKeyMetaCopyFunc)docIdMetaCopy,
    .rename = NULL, // If NULL, meta is kept during rename
    .move = (RedisModuleKeyMetaMoveFunc)docIdMetaMove, // If NULL, meta is kept during move (MOVE between DB, need to make sure it is ignored because docID will not have meaning in other DB)
    .unlink = NULL, // If NULL, meta is ignored during unlink. This is called when deattached before freeing. (While GIL is held)
    .free = (RedisModuleKeyMetaFreeFunc)docIdMetaFree, // Will need to free the DocIdMeta struct (GIL is not held)
    // TODO(Joan): Ask clarification for these callbacks
    .defrag = NULL,
    .mem_usage = NULL,
    .free_effort = NULL,
    // Since for now RediSearch indices are rebuilt during persistence, we don't need to consider persistence now.
    // (DocID would be added to key during Notification callbacks and indexing procedures as normal)
    .rdb_load = (RedisModuleKeyMetaLoadFunc)docIdMetaRDBLoad, // Callback called in Search on Flex when loading to SST
    .rdb_save = (RedisModuleKeyMetaSaveFunc)docIdMetaRDBSave, // Callback called in Search on Flex when saving to SST
    .aof_rewrite = NULL, // not used if Preamble RDB. We should not implement and let the KeySpaceNotifications handle it
};
  docIdKeyMetaClassId = RedisModule_CreateKeyMetaClass(ctx, "docId", 1, &docIdKeyMetaClassIdConfig);
}

int DocIdMeta_SetDocIdForIndex(RedisModuleKey *key, size_t idx, uint64_t docId) {
  RS_ASSERT(docId != DOCID_META_INVALID);
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) == REDISMODULE_OK) {
    if (meta != 0) {
      // key has meta
      // Interpret the meta as DocIdMeta. Here we should append to the list or to the hashmap

      struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
      if (idx >= docIdMeta->size) {
        // reallocate (review policy of resizing)
        size_t newSize = docIdMeta->size * 2;
        docIdMeta->docId = rm_realloc(docIdMeta->docId, sizeof(uint64_t) * newSize);
        memset(docIdMeta->docId + docIdMeta->size, DOCID_META_INVALID, sizeof(uint64_t) * docIdMeta->size);
        docIdMeta->size = newSize;
      }
      docIdMeta->docId[idx] = docId;
      return REDISMODULE_OK;
    }
  }
  size_t initialSize = idx >= INITIAL_DOCID_META_SIZE ? idx + 1 : INITIAL_DOCID_META_SIZE;
  struct DocIdMeta *docIdMeta = rm_malloc(sizeof(struct DocIdMeta));
  docIdMeta->size = initialSize;
  docIdMeta->docId = array_new(uint64_t, initialSize);
  memset(docIdMeta->docId, DOCID_META_INVALID, sizeof(uint64_t) * initialSize);
  docIdMeta->docId[idx] = docId;
  return RedisModule_SetKeyMeta(docIdKeyMetaClassId, key, (uint64_t)docIdMeta);
}

int DocIdMeta_GetDocIdForIndex(RedisModuleKey *key, size_t idx, uint64_t *docId) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (meta == 0) {
    return REDISMODULE_ERR;
  }
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  if (idx >= docIdMeta->size) {
    return REDISMODULE_ERR;
  }
  if (docIdMeta->docId[idx] == DOCID_META_INVALID) {
    return REDISMODULE_ERR;
  }
  *docId = docIdMeta->docId[idx];
  return REDISMODULE_OK;
}

int DocIdMeta_DeleteDocIdForIndex(RedisModuleKey *key, size_t idx) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (meta == 0) {
    return REDISMODULE_ERR;
  }
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  if (idx >= docIdMeta->size) {
    return REDISMODULE_ERR;
  }
  docIdMeta->docId[idx] = DOCID_META_INVALID;
  return REDISMODULE_OK;
}
