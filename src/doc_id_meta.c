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
#include <assert.h>
#include <inttypes.h>

#define DOCID_META_INVALID 0
#define DOCID_META_CLASS_NAME "D-ID"

static RedisModuleKeyMetaClassId docIdKeyMetaClassId;

// When true, RDB save/load callbacks become no-ops.
// Controlled via DocIdMeta_SetPersistenceInProgress, called from notifications.c
// during persistence events (BGSAVE/BGREWRITEAOF) to avoid saving/loading
// DocIdMeta data while persistence is in progress.
static bool PersistenceInProgress = false;

void DocIdMeta_SetPersistenceInProgress(bool inProgress) {
  const char *message = inProgress ?
                          "DocIdMeta: disabling RDB callbacks during persistence" :
                          "DocIdMeta: re-enabling RDB callbacks after persistence";
  RedisModule_Log(RSDummyContext, "verbose", "%s", message);
  PersistenceInProgress = inProgress;
}

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
static inline IndexSpec *findSpecBySpecId(uint64_t specId) {
  StrongRef global_ref = dictFetchRef(specIdDict_g, SPECID_TO_KEY(specId));
  return StrongRef_Get(global_ref);
}

// Check whether specIdDict_g still contains this specId.
// Uses O(1) dict lookup by specId (unique incarnation ID).
// Returns true if the spec is still registered, false otherwise.
static inline bool isSpecValid(uint64_t specId) {
  return dictFetchValue(specIdDict_g, SPECID_TO_KEY(specId)) != NULL;
}

// DocIdMeta V1: a dict of specId (void*) -> docId (void*), using dictTypeUint64.
// The meta value stored on a key is a `dict*` cast to `uint64_t` directly (no wrapper struct).
#define DOCID_META_VERSION 1
// Compose side-effect suppressors individually instead of REDISMODULE_OPEN_KEY_NOEFFECTS.
// The aggregate NOEFFECTS flag also inhibits BigRedis swap-in, which would leave the key
// meta unloaded on disk-resident keys and cause DocIdMeta_Get to return meta=0.
#define KEY_OPEN_META_NO_SIDE_EFFECTS (REDISMODULE_OPEN_KEY_NOTOUCH   | \
                                       REDISMODULE_OPEN_KEY_NONOTIFY  | \
                                       REDISMODULE_OPEN_KEY_NOSTATS   | \
                                       REDISMODULE_OPEN_KEY_NOEXPIRE)
#define KEY_OPEN_META_SET_FLAGS (REDISMODULE_READ | REDISMODULE_WRITE | KEY_OPEN_META_NO_SIDE_EFFECTS)
#define KEY_OPEN_META_GET_FLAGS (REDISMODULE_READ | KEY_OPEN_META_NO_SIDE_EFFECTS)

/* Free callback - called when metadata needs to be freed */
static void docIdMetaFree(const char *keyname, uint64_t meta) {
  size_t nEntries = (meta != 0) ? dictSize((dict *)meta) : 0;
  RedisModule_Log(RSDummyContext, "notice",
                  "DocIdMeta[FREE] key='%s' meta=%s entries=%zu",
                  keyname ? keyname : "(null)",
                  meta == 0 ? "NULL" : "non-null",
                  nEntries);
  if (meta == 0) return;
  dict *d = (dict *)meta;
  dictRelease(d);
}

static int docIdMetaMove(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  const RedisModuleString *fromKey = RedisModule_GetKeyNameFromOptCtx(ctx);
  const RedisModuleString *toKey = RedisModule_GetToKeyNameFromOptCtx(ctx);
  size_t fromLen = 0, toLen = 0;
  const char *fromStr = fromKey ? RedisModule_StringPtrLen((RedisModuleString *)fromKey, &fromLen) : NULL;
  const char *toStr = toKey ? RedisModule_StringPtrLen((RedisModuleString *)toKey, &toLen) : NULL;
  size_t nEntries = (meta && *meta) ? dictSize((dict *)*meta) : 0;
  RedisModule_Log(RSDummyContext, "notice",
                  "DocIdMeta[MOVE] from='%.*s' to='%.*s' entries=%zu (dropping meta)",
                  (int)fromLen, fromStr ? fromStr : "",
                  (int)toLen, toStr ? toStr : "",
                  nEntries);
  REDISMODULE_NOT_USED(meta);
  // We do not want to move the meta, as the docID will not have meaning in the destination DB.
  // Returning 0 tells redis to drop the meta and not move it with the key - see the docs for more info.
  return 0;
}

/* Unlink callback - called when a key is being deleted from the DB, BEFORE
 * the key and metadata are actually freed. At this point the metadata is still
 * valid and we can use it to clean up the document from all indexes that
 * reference it.
 *
 * This fires before the keyspace notification, which is why we handle
 * deletion here rather than in the notification handler. */
static void docIdMetaUnlink(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  const RedisModuleString *keyName = RedisModule_GetKeyNameFromOptCtx(ctx);
  size_t keyLen = 0;
  const char *keyStr = keyName ? RedisModule_StringPtrLen((RedisModuleString *)keyName, &keyLen) : NULL;
  if (*meta == 0) {
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[UNLINK] key='%.*s' meta=NULL (nothing to do)",
                    (int)keyLen, keyStr ? keyStr : "");
    return;
  }

  dict *specIdToDocId = (dict *)*meta;
  size_t nEntries = dictSize(specIdToDocId);
  RedisModule_Log(RSDummyContext, "notice",
                  "DocIdMeta[UNLINK] key='%.*s' entries=%zu",
                  (int)keyLen, keyStr ? keyStr : "", nEntries);
  if (nEntries == 0) return;

  dictIterator *iter = dictGetIterator(specIdToDocId);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    uint64_t docId = VAL_TO_DOCID(dictGetVal(de));
    uint64_t specId = KEY_TO_SPECID(dictGetKey(de));
    if (docId == DOCID_META_INVALID) {
      RedisModule_Log(RSDummyContext, "notice",
                      "DocIdMeta[UNLINK]   key='%.*s' specId=%" PRIu64 " docId=INVALID (skip)",
                      (int)keyLen, keyStr ? keyStr : "", specId);
      continue;
    }

    // Find the IndexSpec by specId in the global dict (O(1) lookup).
    IndexSpec *spec = findSpecBySpecId(specId);
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[UNLINK]   key='%.*s' specId=%" PRIu64 " docId=%" PRIu64 " spec=%s (deleting)",
                    (int)keyLen, keyStr ? keyStr : "", specId, docId,
                    spec ? "found" : "NULL");
    if (spec) {
      // Delete the document from this index by its docId
      IndexSpec_DeleteDocById(spec, docId);
    }
    // Spec may have been dropped already, but we still invalidate the entry
    // since the key is being deleted

    // Invalidate the entry so it won't be used again
    dictSetVal(specIdToDocId, de, DOCID_TO_VAL(DOCID_META_INVALID));
  }
  dictReleaseIterator(iter);
}

// Return values for RedisModuleKeyMetaLoadFunc (documented on RM_CreateKeyMetaClass):
//   1: attach the loaded meta to the key
//   0: skip/ignore (do not attach) - not an error
//  -1: error, abort RDB load
#define DOCID_META_RDB_LOAD_ATTACH 1
#define DOCID_META_RDB_LOAD_SKIP   0
#define DOCID_META_RDB_LOAD_ERROR  (-1)

static int docIdMetaRDBLoad(RedisModuleIO *rdb, uint64_t *meta, int encver) {
  RS_LOG_ASSERT(encver == 1, "DocIdMeta: unexpected encver in RDB load");

  const RedisModuleString *keyName = RedisModule_GetKeyNameFromIO(rdb);
  size_t keyLen = 0;
  const char *keyStr = keyName ? RedisModule_StringPtrLen((RedisModuleString *)keyName, &keyLen) : NULL;

  if (PersistenceInProgress) {
    // Skip actual loading during persistence events. We don't store this metadata in the RDB/AOF files.
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[RDB_LOAD] key='%.*s' SKIPPED (PersistenceInProgress)",
                    (int)keyLen, keyStr ? keyStr : "");
    *meta = 0;
    return DOCID_META_RDB_LOAD_SKIP;
  }

  dict *specIdToDocId = dictCreate(&dictTypeUint64, NULL);
  size_t numEntries;
  size_t kept = 0, dropped = 0;

  // Load the number of entries
  numEntries = LoadUnsigned_IOError(rdb, goto cleanup);
  RedisModule_Log(RSDummyContext, "notice",
                  "DocIdMeta[RDB_LOAD] key='%.*s' numEntries=%zu",
                  (int)keyLen, keyStr ? keyStr : "", numEntries);

  // Load each entry (specId + docId), skipping entries whose spec no longer exists.
  for (size_t i = 0; i < numEntries; i++) {
    uint64_t specId = LoadUnsigned_IOError(rdb, goto cleanup);
    uint64_t docId = LoadUnsigned_IOError(rdb, goto cleanup);

    // Skip entries belonging to indexes that are no longer in specIdDict_g (O(1) lookup).
    if (!isSpecValid(specId)) {
      RedisModule_Log(RSDummyContext, "notice",
                      "DocIdMeta[RDB_LOAD]   key='%.*s' specId=%" PRIu64 " docId=%" PRIu64 " DROPPED (spec invalid)",
                      (int)keyLen, keyStr ? keyStr : "", specId, docId);
      dropped++;
      continue;
    }

    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[RDB_LOAD]   key='%.*s' specId=%" PRIu64 " docId=%" PRIu64 " LOADED",
                    (int)keyLen, keyStr ? keyStr : "", specId, docId);
    kept++;
    dictAdd(specIdToDocId, SPECID_TO_KEY(specId), DOCID_TO_VAL(docId));
  }
  RedisModule_Log(RSDummyContext, "notice",
                  "DocIdMeta[RDB_LOAD] key='%.*s' done kept=%zu dropped=%zu",
                  (int)keyLen, keyStr ? keyStr : "", kept, dropped);

  *meta = (uint64_t)(specIdToDocId);
  return DOCID_META_RDB_LOAD_ATTACH;

cleanup:
  RedisModule_Log(RSDummyContext, "warning",
                  "DocIdMeta[RDB_LOAD] key='%.*s' FAILED (io error)",
                  (int)keyLen, keyStr ? keyStr : "");
  if (specIdToDocId) {
    dictRelease(specIdToDocId);
  }
  *meta = 0;
  return DOCID_META_RDB_LOAD_ERROR;
}

static void docIdMetaRDBSave(RedisModuleIO *rdb, void *value, uint64_t *meta) {
  REDISMODULE_NOT_USED(value);

  const RedisModuleString *keyName = RedisModule_GetKeyNameFromIO(rdb);
  size_t keyLen = 0;
  const char *keyStr = keyName ? RedisModule_StringPtrLen((RedisModuleString *)keyName, &keyLen) : NULL;

  if (PersistenceInProgress) {
    // Skip saving during persistence events. We don't want to save this metadata to an RDB/AOF file
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[RDB_SAVE] key='%.*s' SKIPPED (PersistenceInProgress)",
                    (int)keyLen, keyStr ? keyStr : "");
    return;
  }

  if (*meta == 0) {
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[RDB_SAVE] key='%.*s' meta=NULL (nothing to save)",
                    (int)keyLen, keyStr ? keyStr : "");
    return;
  }

  dict *specIdToDocId = (dict *)*meta;

  // First pass: count valid entries.
  // Skip entries that are unlinked (DOCID_META_INVALID) or whose spec no longer exists.
  size_t validEntries = 0;
  if (dictSize(specIdToDocId) > 0) {
    dictIterator *iter = dictGetIterator(specIdToDocId);
    dictEntry *de;
    while ((de = dictNext(iter))) {
      uint64_t docId = VAL_TO_DOCID(dictGetVal(de));
      uint64_t specId = KEY_TO_SPECID(dictGetKey(de));
      if (docId != DOCID_META_INVALID && isSpecValid(specId)) {
        validEntries++;
      }
    }
    dictReleaseIterator(iter);
  }
  RedisModule_Log(RSDummyContext, "notice",
                  "DocIdMeta[RDB_SAVE] key='%.*s' totalEntries=%lu validEntries=%zu",
                  (int)keyLen, keyStr ? keyStr : "",
                  (unsigned long)dictSize(specIdToDocId), validEntries);

  // Save entry count. Version is handled by encver in the KeyMeta API.
  RedisModule_SaveUnsigned(rdb, validEntries);

  if (validEntries == 0) {
    return;
  }

  // Second pass: save only valid entries (not unlinked and spec still exists).
  dictIterator *iter = dictGetIterator(specIdToDocId);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    uint64_t docId = VAL_TO_DOCID(dictGetVal(de));
    uint64_t specId = KEY_TO_SPECID(dictGetKey(de));
    if (docId == DOCID_META_INVALID || !isSpecValid(specId)) {
      continue;
    }
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[RDB_SAVE]   key='%.*s' specId=%" PRIu64 " docId=%" PRIu64,
                    (int)keyLen, keyStr ? keyStr : "", specId, docId);
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
    .copy = NULL, // If NULL, meta is not copied during copy operations
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
      RedisModule_Log(RSDummyContext, "warning", "DocIdMeta: failed to set metadata for key during DocIdMeta_SetInternal");
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

static int DocIdMeta_DeleteInternal(RedisModuleKey *key, uint64_t specId) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (meta == 0) {
    return REDISMODULE_ERR;
  }
  dict *specIdToDocId = (dict *)meta;
  static_assert(DICT_OK == REDISMODULE_OK);
  static_assert(DICT_ERR == REDISMODULE_ERR);
  return dictDelete(specIdToDocId, SPECID_TO_KEY(specId));
}

// Set docId using key name and spec incarnation ID.
int DocIdMeta_Set(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  uint64_t specId, uint64_t docId) {
  size_t keyLen = 0;
  const char *keyStr = RedisModule_StringPtrLen(keyName, &keyLen);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_SET_FLAGS);
  if (!key) {
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[SET] key='%.*s' specId=%" PRIu64 " docId=%" PRIu64 " OpenKey=NULL -> ERR",
                    (int)keyLen, keyStr, specId, docId);
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_SetInternal(key, specId, docId);
  RedisModule_CloseKey(key);
  RedisModule_Log(RSDummyContext, "notice",
                  "DocIdMeta[SET] key='%.*s' specId=%" PRIu64 " docId=%" PRIu64 " result=%s",
                  (int)keyLen, keyStr, specId, docId,
                  result == REDISMODULE_OK ? "OK" : "ERR");
  return result;
}

// Get docId using key name and spec incarnation ID
int DocIdMeta_Get(RedisModuleCtx *ctx, RedisModuleString *keyName,
                  uint64_t specId, uint64_t *docId) {
  size_t keyLen = 0;
  const char *keyStr = RedisModule_StringPtrLen(keyName, &keyLen);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_GET_FLAGS);
  if (!key) {
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[GET] key='%.*s' specId=%" PRIu64 " OpenKey=NULL -> ERR",
                    (int)keyLen, keyStr, specId);
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_GetInternal(key, specId, docId);
  RedisModule_CloseKey(key);
  if (result == REDISMODULE_OK) {
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[GET] key='%.*s' specId=%" PRIu64 " -> OK docId=%" PRIu64,
                    (int)keyLen, keyStr, specId, *docId);
  } else {
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[GET] key='%.*s' specId=%" PRIu64 " -> ERR",
                    (int)keyLen, keyStr, specId);
  }
  return result;
}

int DocIdMeta_Delete(RedisModuleCtx *ctx, RedisModuleString *keyName, uint64_t specId) {
  size_t keyLen = 0;
  const char *keyStr = RedisModule_StringPtrLen(keyName, &keyLen);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_SET_FLAGS);
  if (!key) {
    RedisModule_Log(RSDummyContext, "notice",
                    "DocIdMeta[DEL] key='%.*s' specId=%" PRIu64 " OpenKey=NULL -> ERR",
                    (int)keyLen, keyStr, specId);
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_DeleteInternal(key, specId);
  RedisModule_CloseKey(key);
  RedisModule_Log(RSDummyContext, "notice",
                  "DocIdMeta[DEL] key='%.*s' specId=%" PRIu64 " result=%s",
                  (int)keyLen, keyStr, specId,
                  result == REDISMODULE_OK ? "OK" : "ERR");
  return result;
}
