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
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  if (docIdMeta) {
    struct DocIdMeta *newDocIdMeta = rm_malloc(sizeof(struct DocIdMeta));
    newDocIdMeta->size = docIdMeta->size;
    newDocIdMeta->docId = array_new(uint64_t, docIdMeta->size);
    memcpy(newDocIdMeta->docId, docIdMeta->docId, sizeof(uint64_t) * docIdMeta->size);
    *meta = (uint64_t)newDocIdMeta;
  }
  return 1;
}

/* Free callback - called when metadata needs to be freed */
static void docIdMetaFree(const char *keyname, uint64_t meta) {
  REDISMODULE_NOT_USED(keyname);
  if (meta == 0) return;
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  array_free(docIdMeta->docId);
  rm_free(docIdMeta);
}

#define INITIAL_DOCID_META_SIZE 10

void DocIdMeta_Init(RedisModuleCtx *ctx) {
  RedisModuleKeyMetaClassConfig docIdKeyMetaClassIdConfig = {
    .version = 1,
    .reset_value = 0,
    .flags = 0,
    .copy = (RedisModuleKeyMetaCopyFunc)docIdMetaCopy,
    .rename = NULL, // If NULL, meta is kept during rename
    .move = NULL, // If NULL, meta is kept during move
    .unlink = NULL, // If NULL, meta is ignored during unlink
    .free = (RedisModuleKeyMetaFreeFunc)docIdMetaFree, // Will need to free the DocIdMeta struct
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
  docIdKeyMetaClassId = RedisModule_CreateKeyMetaClass(ctx, "docId", 1, &docIdKeyMetaClassIdConfig);
}

int DocIdMeta_Set(RedisModuleKey *key, size_t idx, uint64_t docId) {
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

int DocIdMeta_Get(RedisModuleKey *key, size_t idx, uint64_t *docId) {
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
