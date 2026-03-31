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
static bool PersistenceInProgress = false;

// Helper macros for casting between uint64_t and void* for dict keys/values.
#define SPECID_TO_KEY(specId) ((void*)(uintptr_t)(specId))
#define KEY_TO_SPECID(key)    ((uint64_t)(uintptr_t)(key))
#define DOCID_TO_VAL(docId)   ((void*)(uintptr_t)(docId))
#define VAL_TO_DOCID(val)     ((uint64_t)(uintptr_t)(val))

///////////////////////////////////////////////////////////////////////////////////////////////
// SpecId lookup in global spec dictionary
///////////////////////////////////////////////////////////////////////////////////////////////

// Find an IndexSpec in specIdDict_g by specId.
// Uses O(1) dict lookup by specId (unique incarnation ID).
// Returns the IndexSpec pointer if found, or NULL otherwise.
static IndexSpec *findSpecBySpecId(uint64_t specId) {
  if (!specIdDict_g) return NULL;
  StrongRef global_ref = dictFetchRef(specIdDict_g, SPECID_TO_KEY(specId));
  return StrongRef_Get(global_ref);
}

RedisModuleKeyMetaClassId DocIdMeta_GetClassId() {
  return docIdKeyMetaClassId;
}

// DocIdMeta V1: a dict of specId (void*) -> docId (void*), using dictTypeUint64.
// The meta value stored on a key is a `dict*` cast to `uint64_t` directly (no wrapper struct).
#define DOCID_META_VERSION 1
#define KEY_OPEN_META_SET_FLAGS (REDISMODULE_READ | REDISMODULE_WRITE | REDISMODULE_OPEN_KEY_NOEFFECTS)
#define KEY_OPEN_META_GET_FLAGS (REDISMODULE_READ | REDISMODULE_OPEN_KEY_NOEFFECTS)

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
  dict *d = (dict *)meta;
  dictRelease(d);
}

static int docIdMetaMove(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(meta);
  // We do not want to move the meta, as the docID will not have meaning in the destination DB
  return 0;
}

/* Unlink callback - called when a key is being deleted from the DB, BEFORE
 * the key and metadata are actually freed. At this point the metadata is still
 * valid and we can use it to clean up the document from all indexes that
 * reference it.
 *
 * This fires before the keyspace notification, which is why we handle
 * deletion here rather than in the notification handler. */
void docIdMetaUnlink(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  if (*meta == 0) return;

  dict *specIdToDocId = (dict *)*meta;
  if (dictSize(specIdToDocId) == 0) return;

  dictIterator *iter = dictGetIterator(specIdToDocId);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    uint64_t docId = VAL_TO_DOCID(dictGetVal(de));
    if (docId == DOCID_META_INVALID) continue;  // Already soft-deleted

    // Find the IndexSpec by specId in the global dict (O(1) lookup).
    uint64_t specId = KEY_TO_SPECID(dictGetKey(de));
    IndexSpec *spec = findSpecBySpecId(specId);
    if (spec) {
      // Delete the document from this index by its docId
      IndexSpec_DeleteDocById(spec, (t_docId)docId);
    }
    // Spec may have been dropped already, but we still invalidate the entry
    // since the key is being deleted

    // Invalidate the entry so it won't be used again
    dictSetVal(specIdToDocId, de, DOCID_TO_VAL(DOCID_META_INVALID));
  }
  dictReleaseIterator(iter);
}

int docIdMetaRDBLoad(RedisModuleIO *rdb, uint64_t *meta, int encver) {
  RS_LOG_ASSERT(encver == 1, "DocIdMeta: unexpected encver in RDB load");

  if (PersistenceInProgress) {
    *meta = 0;
    return REDISMODULE_OK;
  }

  dict *specIdToDocId = dictCreate(&dictTypeUint64, NULL);
  size_t numEntries;

  // Load the number of entries
  numEntries = LoadUnsigned_IOError(rdb, goto cleanup);

  // Load each entry (specId + docId), skipping entries whose spec no longer exists.
  for (size_t i = 0; i < numEntries; i++) {
    uint64_t specId = LoadUnsigned_IOError(rdb, goto cleanup);
    uint64_t docId = LoadUnsigned_IOError(rdb, goto cleanup);

    // Skip entries belonging to indexes that are no longer in specIdDict_g (O(1) lookup).
    if (!findSpecBySpecId(specId)) {
      continue;
    }

    dictAdd(specIdToDocId, SPECID_TO_KEY(specId), DOCID_TO_VAL(docId));
  }

  *meta = (uint64_t)(specIdToDocId);
  return REDISMODULE_OK;

cleanup:
  if (specIdToDocId) {
    dictRelease(specIdToDocId);
  }
  *meta = 0;
  return REDISMODULE_ERR;
}

void docIdMetaRDBSave(RedisModuleIO *rdb, void *value, uint64_t *meta) {
  REDISMODULE_NOT_USED(value);

  if (PersistenceInProgress) {
    return;
  }

  if (*meta == 0) {
    return;
  }

  dict *specIdToDocId = (dict *)*meta;

  // First pass: count valid entries.
  // Skip entries that are soft-deleted (DOCID_META_INVALID) or whose spec no longer exists.
  size_t validEntries = 0;
  if (dictSize(specIdToDocId) > 0) {
    dictIterator *iter = dictGetIterator(specIdToDocId);
    dictEntry *de;
    while ((de = dictNext(iter))) {
      uint64_t docId = VAL_TO_DOCID(dictGetVal(de));
      uint64_t specId = KEY_TO_SPECID(dictGetKey(de));
      if (docId != DOCID_META_INVALID && findSpecBySpecId(specId)) {
        validEntries++;
      }
    }
    dictReleaseIterator(iter);
  }

  // Save entry count. Version is handled by encver in the KeyMeta API.
  RedisModule_SaveUnsigned(rdb, validEntries);

  if (validEntries == 0) {
    return;
  }

  // Second pass: save only valid entries (not soft-deleted and spec still exists).
  dictIterator *iter = dictGetIterator(specIdToDocId);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    uint64_t docId = VAL_TO_DOCID(dictGetVal(de));
    uint64_t specId = KEY_TO_SPECID(dictGetKey(de));
    if (docId == DOCID_META_INVALID || !findSpecBySpecId(specId)) {
      continue;
    }
    RedisModule_SaveUnsigned(rdb, specId);
    RedisModule_SaveUnsigned(rdb, docId);
  }
  dictReleaseIterator(iter);
}

void DocIdMeta_Init(RedisModuleCtx *ctx) {
  RedisModuleKeyMetaClassConfig docIdKeyMetaClassIdConfig = {
    .version = REDISMODULE_KEY_META_VERSION,
    .reset_value = 0,
    .flags = 1 << REDISMODULE_META_ALLOW_IGNORE,
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
  docIdKeyMetaClassId = RedisModule_CreateKeyMetaClass(ctx, DOCID_META_CLASS_NAME, DOCID_META_VERSION, &docIdKeyMetaClassIdConfig);
  RS_LOG_ASSERT_ALWAYS(docIdKeyMetaClassId >= 0, "Failed to create DocIdMeta class");
}


// We need to listen to the persistence event to skip the RDB loading/saving while RDB is used for replication/key transfer.
// For now, RDB save/load is intended to serialize/deserialize in swap IN/OUT ops.
static void DocIdMeta_PersistenceEvent(RedisModuleCtx *ctx, RedisModuleEvent eid,
                                        uint64_t subevent, void *data) {
  REDISMODULE_NOT_USED(eid);
  REDISMODULE_NOT_USED(data);

  switch (subevent) {
  case REDISMODULE_SUBEVENT_PERSISTENCE_RDB_START:
  case REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START:
    RedisModule_Log(ctx, "notice", "DocIdMeta: Persistence started, disabling RDB save/load");
    PersistenceInProgress = true;
    break;
  case REDISMODULE_SUBEVENT_PERSISTENCE_ENDED:
  case REDISMODULE_SUBEVENT_PERSISTENCE_FAILED:
    RedisModule_Log(ctx, "notice", "DocIdMeta: Persistence ended, re-enabling RDB save/load");
    PersistenceInProgress = false;
    break;
  }
}

void DocIdMeta_SubscribePersistenceEvent(RedisModuleCtx *ctx) {
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Persistence, DocIdMeta_PersistenceEvent);
}

// Internal function that works with RedisModuleKey
static int DocIdMeta_SetInternal(RedisModuleKey *key, uint64_t specId,
                                  uint64_t docId) {
  RS_ASSERT(docId != DOCID_META_INVALID);
  uint64_t meta = 0;

  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK || meta == 0) {
    // Create new dict for this key's metadata
    dict *d = dictCreate(&dictTypeUint64, NULL);

    int result = RedisModule_SetKeyMeta(docIdKeyMetaClassId, key, (uint64_t)d);
    if (result != REDISMODULE_OK) {
      dictRelease(d);
      return result;
    }
    meta = (uint64_t)d;
  }

  dict *specIdToDocId = (dict *)meta;
  // Set the docId for this specId (dictReplace handles both insert and update)
  dictReplace(specIdToDocId, SPECID_TO_KEY(specId), DOCID_TO_VAL(docId));
  return REDISMODULE_OK;
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
  dict *specIdToDocId = (dict *)meta;
  dictEntry *de = dictFind(specIdToDocId, SPECID_TO_KEY(specId));
  if (!de) {
    return REDISMODULE_ERR;
  }
  uint64_t storedDocId = VAL_TO_DOCID(dictGetVal(de));
  if (storedDocId == DOCID_META_INVALID) {
    return REDISMODULE_ERR;
  }
  *docId = storedDocId;
  return REDISMODULE_OK;
}

// Soft-delete: invalidate the docId but keep the entry for potential reuse.
static int DocIdMeta_SoftDeleteInternal(RedisModuleKey *key, uint64_t specId) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (meta == 0) {
    return REDISMODULE_ERR;
  }
  dict *specIdToDocId = (dict *)meta;
  dictEntry *de = dictFind(specIdToDocId, SPECID_TO_KEY(specId));
  if (!de) {
    return REDISMODULE_ERR;
  }
  dictSetVal(specIdToDocId, de, DOCID_TO_VAL(DOCID_META_INVALID));
  return REDISMODULE_OK;
}

// Set docId using key name and spec incarnation ID.
int DocIdMeta_Set(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  uint64_t specId, uint64_t docId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_SET_FLAGS);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_SetInternal(key, specId, docId);
  RedisModule_CloseKey(key);
  return result;
}

// Get docId using key name and spec incarnation ID
int DocIdMeta_Get(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  uint64_t specId, uint64_t *docId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_GET_FLAGS);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_GetInternal(key, specId, docId);
  RedisModule_CloseKey(key);
  return result;
}

// Soft-delete docId using key name and spec incarnation ID.
// Invalidates the docId but keeps the entry for efficient reuse on re-indexing.
int DocIdMeta_SoftDelete(RedisModuleCtx *ctx, RedisModuleString *keyName,
                         uint64_t specId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_SET_FLAGS);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_SoftDeleteInternal(key, specId);
  RedisModule_CloseKey(key);
  return result;
}
