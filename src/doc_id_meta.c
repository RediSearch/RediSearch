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

// When true, RDB save/load callbacks become no-ops.
// Set during persistence events (BGSAVE/BGREWRITEAOF) to avoid
// saving/loading DocIdMeta data while persistence is in progress.
static bool docIdMetaPersistenceInProgress = false;

///////////////////////////////////////////////////////////////////////////////////////////////
// SpecId lookup in global spec dictionary
///////////////////////////////////////////////////////////////////////////////////////////////

// Find an IndexSpec in specDict_g by spec name + specId.
// Uses O(1) dict lookup by name, then verifies the specId matches
// (guards against index drop + recreate with the same name).
// Returns the IndexSpec pointer if found and specId matches, or NULL otherwise.
static IndexSpec *findSpecByNameAndId(const char *name, size_t nameLen, uint64_t specId) {
  if (!specDict_g) return NULL;
  HiddenString *hidden = NewHiddenString(name, nameLen, false);
  RefManager *rm = dictFetchValue(specDict_g, hidden);
  HiddenString_Free(hidden, false);
  if (!rm) return NULL;
  IndexSpec *spec = StrongRef_Get((StrongRef){rm});
  if (spec && spec->specId == specId) return spec;
  return NULL;
}

RedisModuleKeyMetaClassId DocIdMeta_GetClassId() {
  return docIdKeyMetaClassId;
}

// Entry in the hashmap: maps specId to docId.
// The specId is a unique incarnation ID for the IndexSpec, so that if an index
// is dropped and recreated with the same name, the old entries won't collide
// with the new index.
// The specName is stored to allow O(1) lookup in specDict_g (which is keyed by name).
typedef struct DocIdEntry {
  uint64_t specId;      // Unique incarnation ID of the spec
  uint64_t docId;
  char *specName;       // Owned copy of the spec name (for O(1) lookup in specDict_g)
  size_t specNameLen;
} DocIdEntry;

#define DOCID_META_VERSION 0

// DocIdMeta uses a hashmap keyed by specId
struct DocIdMeta {
  uint16_t version;  // Version of the DocIdMeta format, must be DOCID_META_VERSION
  dict *entries;     // dict of specId -> DocIdEntry*
};

// Dict type callbacks for DocIdMeta entries
static uint64_t docIdEntryHash(const void *key) {
  const DocIdEntry *entry = key;
  // Use specId directly as hash (it's already unique)
  return entry->specId;
}

static int docIdEntryCompare(void *privdata, const void *key1, const void *key2) {
  DICT_NOTUSED(privdata);
  const DocIdEntry *e1 = key1;
  const DocIdEntry *e2 = key2;
  return e1->specId == e2->specId;  // Return 1 if equal
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

    // Find the IndexSpec by name + specId in the global dict (O(1) lookup).
    IndexSpec *spec = findSpecByNameAndId(entry->specName, entry->specNameLen, entry->specId);
    if (!spec) continue;  // Spec may have been dropped already

    // Delete the document from this index by its docId
    IndexSpec_DeleteDocById(spec, (t_docId)entry->docId);

    // Invalidate the entry so it won't be used again
    entry->docId = DOCID_META_INVALID;
  }
  dictReleaseIterator(iter);
}

int docIdMetaRDBLoad(RedisModuleIO *rdb, uint64_t *meta, int encver) {
  REDISMODULE_NOT_USED(encver);

  if (docIdMetaPersistenceInProgress) {
    *meta = 0;
    return REDISMODULE_OK;
  }

  struct DocIdMeta *docIdMeta = rm_malloc(sizeof(struct DocIdMeta));
  docIdMeta->entries = dictCreate(&docIdMetaDictType, NULL);

  uint16_t version;
  size_t numEntries;

  // Load and validate the version
  version = LoadUnsigned_IOError(rdb, goto cleanup);
  RS_LOG_ASSERT(version == DOCID_META_VERSION, "DocIdMeta: unexpected version in RDB load");
  docIdMeta->version = version;

  // Load the number of entries
  numEntries = LoadUnsigned_IOError(rdb, goto cleanup);

  // Load each entry (specName + specId + docId), skipping entries whose spec no longer exists.
  for (size_t i = 0; i < numEntries; i++) {
    size_t specNameLen = 0;
    char *specName = LoadStringBuffer_IOError(rdb, &specNameLen, goto cleanup);
    uint64_t specId = LoadUnsigned_IOError(rdb, goto cleanup);
    uint64_t docId = LoadUnsigned_IOError(rdb, goto cleanup);

    // Skip entries belonging to indexes that are no longer in specDict_g (O(1) lookup).
    if (!findSpecByNameAndId(specName, specNameLen, specId)) {
      RedisModule_Free(specName);
      continue;
    }

    // Create entry
    DocIdEntry *entry = rm_malloc(sizeof(DocIdEntry));
    entry->specId = specId;
    entry->docId = docId;
    // Take ownership of the name string (convert from RedisModule_Alloc to rm_alloc)
    entry->specNameLen = specNameLen;
    entry->specName = rm_strndup(specName, specNameLen);
    RedisModule_Free(specName);

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

  if (docIdMetaPersistenceInProgress) {
    return;
  }

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

  // First pass: count valid entries (those whose spec still exists in specDict_g).
  size_t validEntries = 0;
  dictIterator *iter = dictGetIterator(docIdMeta->entries);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    DocIdEntry *entry = dictGetVal(de);
    if (findSpecByNameAndId(entry->specName, entry->specNameLen, entry->specId)) {
      validEntries++;
    }
  }
  dictReleaseIterator(iter);

  if (validEntries == 0) {
    return;
  }

  // Save the version
  RedisModule_SaveUnsigned(rdb, docIdMeta->version);

  // Save the number of valid entries
  RedisModule_SaveUnsigned(rdb, validEntries);

  // Second pass: save only entries whose spec still exists.
  iter = dictGetIterator(docIdMeta->entries);
  while ((de = dictNext(iter))) {
    DocIdEntry *entry = dictGetVal(de);
    if (!findSpecByNameAndId(entry->specName, entry->specNameLen, entry->specId)) {
      continue;
    }
    RedisModule_SaveStringBuffer(rdb, entry->specName, entry->specNameLen);
    RedisModule_SaveUnsigned(rdb, entry->specId);
    RedisModule_SaveUnsigned(rdb, entry->docId);
  }
  dictReleaseIterator(iter);
}

void DocIdMeta_Init(RedisModuleCtx *ctx) {
  RedisModuleKeyMetaClassConfig docIdKeyMetaClassIdConfig = {
    .version = 1,
    .reset_value = 0,
    .flags = REDISMODULE_META_ALLOW_IGNORE,
    .copy = (RedisModuleKeyMetaCopyFunc)docIdMetaCopy,
    .rename = NULL, // If NULL, meta is kept during rename
    .move = (RedisModuleKeyMetaMoveFunc)docIdMetaMove,
    .unlink = (RedisModuleKeyMetaUnlinkFunc)docIdMetaUnlink,
    .free = (RedisModuleKeyMetaFreeFunc)docIdMetaFree,
    .rdb_load = (RedisModuleKeyMetaLoadFunc)docIdMetaRDBLoad,
    .rdb_save = (RedisModuleKeyMetaSaveFunc)docIdMetaRDBSave,
    .aof_rewrite = NULL,
    .defrag = NULL,
    .mem_usage = NULL,
    .free_effort = NULL,
  };
  docIdKeyMetaClassId = RedisModule_CreateKeyMetaClass(ctx, DOCID_META_CLASS_NAME, 1, &docIdKeyMetaClassIdConfig);
  RS_LOG_ASSERT(docIdKeyMetaClassId >= 0, "Failed to create DocIdMeta class");
}

static void DocIdMeta_PersistenceEvent(RedisModuleCtx *ctx, RedisModuleEvent eid,
                                        uint64_t subevent, void *data) {
  REDISMODULE_NOT_USED(eid);
  REDISMODULE_NOT_USED(data);

  switch (subevent) {
  case REDISMODULE_SUBEVENT_PERSISTENCE_RDB_START:
  case REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START:
    RedisModule_Log(ctx, "notice", "DocIdMeta: Persistence started, disabling RDB save/load");
    docIdMetaPersistenceInProgress = true;
    break;
  case REDISMODULE_SUBEVENT_PERSISTENCE_ENDED:
  case REDISMODULE_SUBEVENT_PERSISTENCE_FAILED:
    RedisModule_Log(ctx, "notice", "DocIdMeta: Persistence ended, re-enabling RDB save/load");
    docIdMetaPersistenceInProgress = false;
    break;
  }
}

void DocIdMeta_SubscribePersistenceEvent(RedisModuleCtx *ctx) {
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Persistence, DocIdMeta_PersistenceEvent);
}

// Helper to find or create an entry in the DocIdMeta hashmap.
// When creating, specName/specNameLen must be valid (an owned copy is made).
static DocIdEntry *findOrCreateEntry(struct DocIdMeta *docIdMeta,
                                      uint64_t specId, bool create,
                                      const char *specName, size_t specNameLen) {
  // Create a temporary entry for lookup
  DocIdEntry lookupKey = {.specId = specId, .docId = 0, .specName = NULL, .specNameLen = 0};
  dictEntry *de = dictFind(docIdMeta->entries, &lookupKey);
  if (de) {
    return dictGetVal(de);
  }
  if (!create) {
    return NULL;
  }
  // Create new entry
  DocIdEntry *entry = rm_malloc(sizeof(DocIdEntry));
  entry->specId = specId;
  entry->docId = DOCID_META_INVALID;
  entry->specName = rm_strndup(specName, specNameLen);
  entry->specNameLen = specNameLen;
  dictAdd(docIdMeta->entries, entry, entry);
  return entry;
}

// Internal function that works with RedisModuleKey
static int DocIdMeta_SetInternal(RedisModuleKey *key, uint64_t specId,
                                  uint64_t docId,
                                  const char *specName, size_t specNameLen) {
  RS_ASSERT(docId != DOCID_META_INVALID);
  uint64_t meta = 0;
  struct DocIdMeta *docIdMeta = NULL;

  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) == REDISMODULE_OK && meta != 0) {
    docIdMeta = (struct DocIdMeta *)meta;
    DocIdEntry *entry = findOrCreateEntry(docIdMeta, specId, true, specName, specNameLen);
    entry->docId = docId;
    return REDISMODULE_OK;
  }

  // Create new DocIdMeta
  docIdMeta = rm_malloc(sizeof(struct DocIdMeta));
  docIdMeta->version = DOCID_META_VERSION;
  docIdMeta->entries = dictCreate(&docIdMetaDictType, NULL);

  DocIdEntry *entry = findOrCreateEntry(docIdMeta, specId, true, specName, specNameLen);
  entry->docId = docId;

  return RedisModule_SetKeyMeta(docIdKeyMetaClassId, key, (uint64_t)docIdMeta);
}

static int DocIdMeta_GetInternal(RedisModuleKey *key, uint64_t specId,
                                  uint64_t *docId) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (meta == 0) {
    return REDISMODULE_ERR;
  }
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  RS_LOG_ASSERT(docIdMeta->version == DOCID_META_VERSION, "DocIdMeta: unexpected version in Get");
  DocIdEntry *entry = findOrCreateEntry(docIdMeta, specId, false, NULL, 0);
  if (!entry || entry->docId == DOCID_META_INVALID) {
    return REDISMODULE_ERR;
  }
  *docId = entry->docId;
  return REDISMODULE_OK;
}

static int DocIdMeta_DeleteInternal(RedisModuleKey *key, uint64_t specId) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (meta == 0) {
    return REDISMODULE_ERR;
  }
  struct DocIdMeta *docIdMeta = (struct DocIdMeta *)meta;
  DocIdEntry *entry = findOrCreateEntry(docIdMeta, specId, false, NULL, 0);
  if (!entry) {
    return REDISMODULE_ERR;
  }
  entry->docId = DOCID_META_INVALID;
  return REDISMODULE_OK;
}

// Set docId using key name and spec incarnation ID.
// specName/specNameLen identify the index name (stored for O(1) lookup in specDict_g).
int DocIdMeta_Set(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  uint64_t specId, uint64_t docId,
                  const char *specName, size_t specNameLen) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_SetInternal(key, specId, docId, specName, specNameLen);
  RedisModule_CloseKey(key);
  return result;
}

// Get docId using key name and spec incarnation ID
int DocIdMeta_Get(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  uint64_t specId, uint64_t *docId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_GetInternal(key, specId, docId);
  RedisModule_CloseKey(key);
  return result;
}

// Delete docId using key name and spec incarnation ID
int DocIdMeta_Delete(RedisModuleCtx *ctx, RedisModuleString *keyName,
                     uint64_t specId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_DeleteInternal(key, specId);
  RedisModule_CloseKey(key);
  return result;
}
