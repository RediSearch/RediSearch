/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "doc_id_meta.h"
#include "spec.h"
#include "util/arr/arr.h"
#include "util/dict.h"
#include "rdb.h"
#include <stdbool.h>

#define DOCID_META_INVALID 0
#define DOCID_META_CLASS_NAME "D-ID"

static RedisModuleKeyMetaClassId docIdKeyMetaClassId;

RedisModuleKeyMetaClassId DocIdMeta_GetClassId() {
  return docIdKeyMetaClassId;
}

// Entry in the hashmap: maps (specName, specId) to docId.
// The specId is a unique incarnation ID for the IndexSpec, so that if an index
// is dropped and recreated with the same name, the old entries won't collide
// with the new index.
typedef struct DocIdEntry {
  char *specName;    // Allocated copy of the spec name
  size_t specNameLen;
  uint64_t specId;   // Unique incarnation ID of the spec
  uint64_t docId;
} DocIdEntry;

// DocIdMeta uses a hashmap keyed by spec name for stable RDB save/load
struct DocIdMeta {
  dict *entries;  // dict of specName -> DocIdEntry*
};

// Dict type callbacks for DocIdMeta entries
static uint64_t docIdEntryHash(const void *key) {
  const DocIdEntry *entry = key;
  // Mix specName hash with specId for a composite key
  uint64_t h = dictGenHashFunction(entry->specName, entry->specNameLen);
  h ^= entry->specId * 0x9e3779b97f4a7c15ULL;  // golden ratio mixing
  return h;
}

static int docIdEntryCompare(void *privdata, const void *key1, const void *key2) {
  DICT_NOTUSED(privdata);
  const DocIdEntry *e1 = key1;
  const DocIdEntry *e2 = key2;
  if (e1->specId != e2->specId) return 0;  // Not equal
  if (e1->specNameLen != e2->specNameLen) return 0;  // Not equal
  return memcmp(e1->specName, e2->specName, e1->specNameLen) == 0;  // Return 1 if equal
}

static void docIdEntryFree(void *privdata, void *val) {
  DICT_NOTUSED(privdata);
  DocIdEntry *entry = val;
  rm_free(entry->specName);
  rm_free(entry);
}

static dictType docIdMetaDictType = {
  .hashFunction = docIdEntryHash,
  .keyDup = NULL,
  .valDup = NULL,
  .keyCompare = docIdEntryCompare,
  .keyDestructor = NULL,
  .valDestructor = docIdEntryFree,
};

static int docIdMetaCopy(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(meta);
  // We do not want to copy the meta, as the docID will not have meaning in the destination DB,
  // or the new key will get reindexed in KeySpaceNotification
  return 0;
}

/* Free callback - called when metadata needs to be freed */
static void docIdMetaFree(const char *keyname, uint64_t meta) {
  REDISMODULE_NOT_USED(keyname);
  if (meta == 0) return;
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  if (docIdMeta->entries) {
    dictRelease(docIdMeta->entries);
  }
  rm_free(docIdMeta);
}

static int docIdMetaMove(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(meta);
  // We do not want to move the meta, as the docID will not have meaning in the destination DB
  return 0;
}

/* Unlink callback - called when a key is being deleted/overwritten, BEFORE
 * the key and metadata are actually freed. At this point the metadata is still
 * valid and we can use it to clean up the document from all indexes that
 * reference it.
 *
 * This fires before the keyspace notification, which is why we handle
 * deletion here rather than in the notification handler. */
static void docIdMetaUnlink(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  if (*meta == 0) return;

  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)*meta;
  if (!docIdMeta->entries || dictSize(docIdMeta->entries) == 0) return;

  dictIterator *iter = dictGetIterator(docIdMeta->entries);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    DocIdEntry *entry = dictGetVal(de);
    if (entry->docId == DOCID_META_INVALID) continue;

    // Look up the IndexSpec by name in the global dict
    HiddenString *specName = NewHiddenString(entry->specName, entry->specNameLen, false);
    StrongRef spec_ref = {dictFetchValue(specDict_g, specName)};
    IndexSpec *spec = StrongRef_Get(spec_ref);
    HiddenString_Free(specName, false);

    if (!spec) continue;  // Spec may have been dropped already

    // If the spec was recreated with the same name, the specId will differ.
    // In that case, this entry is stale and should be skipped.
    if (spec->specId != entry->specId) continue;

    // Delete the document from this index by its docId
    IndexSpec_DeleteDocById(spec, (t_docId)entry->docId);

    // Invalidate the entry so it won't be used again
    entry->docId = DOCID_META_INVALID;
  }
  dictReleaseIterator(iter);
}

int docIdMetaRDBLoad(RedisModuleIO *rdb, uint64_t *meta, int encver) {
  REDISMODULE_NOT_USED(encver);
  struct DocIdMeta *docIdMeta = rm_malloc(sizeof(struct DocIdMeta));
  docIdMeta->entries = dictCreate(&docIdMetaDictType, NULL);

  // Load the number of entries
  size_t numEntries = LoadUnsigned_IOError(rdb, goto cleanup);

  // Load each entry (specName + specId + docId)
  for (size_t i = 0; i < numEntries; i++) {
    size_t specNameLen = 0;
    char *specName = LoadStringBuffer_IOError(rdb, &specNameLen, goto cleanup);
    uint64_t specId = LoadUnsigned_IOError(rdb, rm_free(specName); goto cleanup);
    uint64_t docId = LoadUnsigned_IOError(rdb, rm_free(specName); goto cleanup);

    // Create entry
    DocIdEntry *entry = rm_malloc(sizeof(DocIdEntry));
    entry->specName = specName;
    entry->specNameLen = specNameLen;
    entry->specId = specId;
    entry->docId = docId;

    dictAdd(docIdMeta->entries, entry, entry);
  }

  *meta = (uint64_t)docIdMeta;
  return REDISMODULE_OK;

cleanup:
  if (docIdMeta) {
    if (docIdMeta->entries) {
      dictRelease(docIdMeta->entries);
    }
    rm_free(docIdMeta);
  }
  *meta = 0;
  return REDISMODULE_ERR;
}

void docIdMetaRDBSave(RedisModuleIO *rdb, void *value, uint64_t *meta) {
  REDISMODULE_NOT_USED(value);

  if (*meta == 0) {
    return;
  }

  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)*meta;
  if (!docIdMeta->entries) {
    return;
  }

  size_t numEntries = dictSize(docIdMeta->entries);
  if (numEntries == 0) {
    return;
  }

  // Save the number of entries
  RedisModule_SaveUnsigned(rdb, numEntries);

  // Save each entry (specName + specId + docId)
  dictIterator *iter = dictGetIterator(docIdMeta->entries);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    DocIdEntry *entry = dictGetVal(de);
    RedisModule_SaveStringBuffer(rdb, entry->specName, entry->specNameLen);
    RedisModule_SaveUnsigned(rdb, entry->specId);
    RedisModule_SaveUnsigned(rdb, entry->docId);
  }
  dictReleaseIterator(iter);
}

void DocIdMeta_Init(RedisModuleCtx *ctx, bool diskEnabled) {
  RedisModuleKeyMetaClassConfig docIdKeyMetaClassIdConfig = {
    .version = 1,
    .reset_value = 0,
    .flags = REDISMODULE_META_ALLOW_IGNORE,
    .copy = (RedisModuleKeyMetaCopyFunc)docIdMetaCopy,
    .rename = NULL, // If NULL, meta is kept during rename
    .move = (RedisModuleKeyMetaMoveFunc)docIdMetaMove,
    .unlink = (RedisModuleKeyMetaUnlinkFunc)docIdMetaUnlink,
    .free = (RedisModuleKeyMetaFreeFunc)docIdMetaFree,
    .rdb_load = diskEnabled ? (RedisModuleKeyMetaLoadFunc)docIdMetaRDBLoad : NULL,
    .rdb_save = diskEnabled ? (RedisModuleKeyMetaSaveFunc)docIdMetaRDBSave : NULL,
    .aof_rewrite = NULL,
    .defrag = NULL,
    .mem_usage = NULL,
    .free_effort = NULL,
  };
  docIdKeyMetaClassId = RedisModule_CreateKeyMetaClass(ctx, DOCID_META_CLASS_NAME, 1, &docIdKeyMetaClassIdConfig);
  RS_LOG_ASSERT(docIdKeyMetaClassId >= 0, "Failed to create DocIdMeta class");
}

// Helper to find or create an entry in the DocIdMeta hashmap
static DocIdEntry *findOrCreateEntry(struct DocIdMeta *docIdMeta,
                                      const char *specName, size_t specNameLen,
                                      uint64_t specId, bool create) {
  // Create a temporary entry for lookup
  DocIdEntry lookupKey = {.specName = (char*)specName, .specNameLen = specNameLen,
                          .specId = specId, .docId = 0};
  dictEntry *de = dictFind(docIdMeta->entries, &lookupKey);
  if (de) {
    return dictGetVal(de);
  }
  if (!create) {
    return NULL;
  }
  // Create new entry
  DocIdEntry *entry = rm_malloc(sizeof(DocIdEntry));
  entry->specName = rm_strndup(specName, specNameLen);
  entry->specNameLen = specNameLen;
  entry->specId = specId;
  entry->docId = DOCID_META_INVALID;
  dictAdd(docIdMeta->entries, entry, entry);
  return entry;
}

// Internal function that works with RedisModuleKey
static int DocIdMeta_SetInternal(RedisModuleKey *key, const char *specName,
                                  size_t specNameLen, uint64_t specId,
                                  uint64_t docId) {
  RS_ASSERT(docId != DOCID_META_INVALID);
  uint64_t meta = 0;
  struct DocIdMeta *docIdMeta = NULL;

  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) == REDISMODULE_OK && meta != 0) {
    docIdMeta = (struct DocIdMeta *)meta;
    DocIdEntry *entry = findOrCreateEntry(docIdMeta, specName, specNameLen, specId, true);
    entry->docId = docId;
    return REDISMODULE_OK;
  }

  // Create new DocIdMeta
  docIdMeta = rm_malloc(sizeof(struct DocIdMeta));
  docIdMeta->entries = dictCreate(&docIdMetaDictType, NULL);

  DocIdEntry *entry = findOrCreateEntry(docIdMeta, specName, specNameLen, specId, true);
  entry->docId = docId;

  return RedisModule_SetKeyMeta(docIdKeyMetaClassId, key, (uint64_t)docIdMeta);
}

static int DocIdMeta_GetInternal(RedisModuleKey *key, const char *specName,
                                  size_t specNameLen, uint64_t specId,
                                  uint64_t *docId) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (meta == 0) {
    return REDISMODULE_ERR;
  }
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  DocIdEntry *entry = findOrCreateEntry(docIdMeta, specName, specNameLen, specId, false);
  if (!entry || entry->docId == DOCID_META_INVALID) {
    return REDISMODULE_ERR;
  }
  *docId = entry->docId;
  return REDISMODULE_OK;
}

static int DocIdMeta_DeleteInternal(RedisModuleKey *key, const char *specName,
                                     size_t specNameLen, uint64_t specId) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (meta == 0) {
    return REDISMODULE_ERR;
  }
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  DocIdEntry *entry = findOrCreateEntry(docIdMeta, specName, specNameLen, specId, false);
  if (!entry) {
    return REDISMODULE_ERR;
  }
  entry->docId = DOCID_META_INVALID;
  return REDISMODULE_OK;
}

// Set docId using key name, spec name, and spec incarnation ID
int DocIdMeta_Set(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  const char *specName, size_t specNameLen, uint64_t specId,
                  uint64_t docId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_SetInternal(key, specName, specNameLen, specId, docId);
  RedisModule_CloseKey(key);
  return result;
}

// Get docId using key name, spec name, and spec incarnation ID
int DocIdMeta_Get(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  const char *specName, size_t specNameLen, uint64_t specId,
                  uint64_t *docId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_GetInternal(key, specName, specNameLen, specId, docId);
  RedisModule_CloseKey(key);
  return result;
}

// Delete docId using key name, spec name, and spec incarnation ID
int DocIdMeta_Delete(RedisModuleCtx *ctx, RedisModuleString *keyName,
                     const char *specName, size_t specNameLen, uint64_t specId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_DeleteInternal(key, specName, specNameLen, specId);
  RedisModule_CloseKey(key);
  return result;
}
