/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "doc_id_meta.h"

#include <stdbool.h>
#include <assert.h>
#include <stddef.h>

#include "spec.h"
#include "indexes.h"
#include "rdb.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"
#include "search_disk.h"
#include "util/dict/dict.h"
#include "util/references.h"

#define DOCID_META_INVALID 0
#define DOCID_META_CLASS_NAME "D-ID"

static RedisModuleKeyMetaClassId docIdKeyMetaClassId;
static bool docIdMetaCompactRam;

// When true, RDB save/load callbacks become no-ops.
// Controlled via DocIdMeta_SetForgetDocIdMetadata, called from notifications.c
// during persistence events (BGSAVE/BGREWRITEAOF) to avoid saving/loading
// DocIdMeta data while persistence is in progress.
static bool ForgetDocIdMetadata = false;

void DocIdMeta_SetForgetDocIdMetadata(bool inProgress) {
  const char *message = inProgress ? "DocIdMeta: disabling RDB callbacks during persistence"
                                   : "DocIdMeta: re-enabling RDB callbacks after persistence";
  RedisModule_Log(RSDummyContext, "verbose", "%s", message);
  ForgetDocIdMetadata = inProgress;
}

// Helper macros for casting between uint64_t and void* for dict keys.
#define SPECID_TO_KEY(specId) ((void *)(uintptr_t)(specId))
#define KEY_TO_SPECID(key) ((uint64_t)(uintptr_t)(key))

// Document metadata for one index membership.
typedef struct {
  uint64_t docId;   // DOCID_META_INVALID once the key has been unlinked
  uint32_t docLen;  // DOCID_META_DOCLEN_UNKNOWN for entries loaded from a version-1 payload
} DocIdMetaEntry;

typedef struct {
  uint64_t specId;
  DocIdMetaEntry entry;
} DocIdMetaSingle;

// The uint64_t Redis metadata slot has three states.
//   0                 no metadata
//   aligned pointer   dict of specId -> entry
//   pointer | 1       one RAM membership (specId, docId, docLen)
// Allocations are aligned, so the low bit is available as a tag.
#define DOCID_META_SINGLE_TAG UINT64_C(1)

static DocIdMetaSingle *docIdMetaAsSingle(uint64_t meta) {
  if (!(meta & DOCID_META_SINGLE_TAG)) return NULL;
  return (DocIdMetaSingle *)(uintptr_t)(meta & ~DOCID_META_SINGLE_TAG);
}

static uint64_t docIdMetaSingleCreate(uint64_t specId, DocIdMetaEntry entry) {
  DocIdMetaSingle *single = rm_malloc(sizeof(*single));
  *single = (DocIdMetaSingle){.specId = specId, .entry = entry};
  uint64_t meta = (uint64_t)(uintptr_t)single;
  RS_ASSERT((meta & DOCID_META_SINGLE_TAG) == 0);
  return meta | DOCID_META_SINGLE_TAG;
}

static void docIdMetaEntryFree(void *privdata, void *entry) {
  REDISMODULE_NOT_USED(privdata);
  rm_free(entry);
}

// dictTypeUint64 keyed by specId, owning its DocIdMetaEntry values. Filled in by DocIdMeta_Init.
static dictType docIdMetaDictType;

static dict *docIdMetaDictCreate(void) {
  return dictCreate(&docIdMetaDictType, NULL);
}

// Insert or overwrite the entry for `specId`.
static void docIdMetaDictPut(dict *specIdToEntry, uint64_t specId, uint64_t docId,
                             uint32_t docLen) {
  dictEntry *de = dictFind(specIdToEntry, SPECID_TO_KEY(specId));
  DocIdMetaEntry *entry;
  if (de) {
    entry = dictGetVal(de);
  } else {
    entry = rm_malloc(sizeof(*entry));
    dictAdd(specIdToEntry, SPECID_TO_KEY(specId), entry);
  }
  entry->docId = docId;
  entry->docLen = docLen;
}

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

// Version 1 persisted (specId, docId) pairs; version 2 appends the document length so that
// replace and delete need no doc-table read. docIdMetaRDBLoad accepts both.
#define DOCID_META_VERSION 2
#define KEY_OPEN_META_SET_FLAGS \
  (REDISMODULE_READ | REDISMODULE_WRITE | REDISMODULE_OPEN_KEY_NOEFFECTS)
#define KEY_OPEN_META_GET_FLAGS (REDISMODULE_READ | REDISMODULE_OPEN_KEY_NOEFFECTS)

static void docIdMetaRelease(uint64_t meta) {
  if (!meta) return;

  DocIdMetaSingle *single = docIdMetaAsSingle(meta);
  if (single) {
    rm_free(single);
  } else {
    dictRelease((dict *)meta);
  }
}

/* May run on a Redis BIO thread without the GIL. Indexes_DeleteSlots removes
 * the documents from the indexes during background slot trim. */
static void docIdMetaFree(const char *keyname, uint64_t meta) {
  REDISMODULE_NOT_USED(keyname);
  docIdMetaRelease(meta);
}

// Free the old payload on a successful swap or the proposed payload on failure.
static int docIdMetaReplace(RedisModuleKey *key, uint64_t oldMeta, uint64_t newMeta) {
  int rc = RedisModule_SetKeyMeta(docIdKeyMetaClassId, key, newMeta);
  if (rc == REDISMODULE_OK) {
    docIdMetaRelease(oldMeta);
  } else {
    docIdMetaRelease(newMeta);
  }
  return rc;
}

static int docIdMetaMove(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(meta);
  // We do not want to move the meta, as the docID will not have meaning in the destination DB.
  // Returning 0 tells redis to drop the meta and not move it with the key - see the docs for more
  // info.
  return 0;
}

static void docIdMetaUnlinkEntry(RedisModuleKeyOptCtx *ctx, const RedisModuleString **keyName,
                                 uint64_t specId, DocIdMetaEntry *entry) {
  if (entry->docId == DOCID_META_INVALID) return;

  IndexSpec *spec = findSpecBySpecId(specId);
  if (spec) {
    if (!*keyName) *keyName = RedisModule_GetKeyNameFromOptCtx(ctx);
    IndexSpec_DeleteDocById(spec, *keyName, entry->docId, entry->docLen);
  }
  entry->docId = DOCID_META_INVALID;
}

/* Unlink callback - called when a key is being deleted from the DB, BEFORE
 * the key and metadata are actually freed. At this point the metadata is still
 * valid and we can use it to clean up the document from all indexes that
 * reference it.
 *
 * This fires before the keyspace notification, which is why we handle
 * deletion here rather than in the notification handler. */
static void docIdMetaUnlink(RedisModuleKeyOptCtx *ctx, uint64_t *meta) {
  if (*meta == 0) return;

  // Resolve the key name only when a registered index needs it.
  const RedisModuleString *keyName = NULL;
  DocIdMetaSingle *single = docIdMetaAsSingle(*meta);
  if (single) {
    docIdMetaUnlinkEntry(ctx, &keyName, single->specId, &single->entry);
    return;
  }

  dict *specIdToEntry = (dict *)*meta;
  if (dictSize(specIdToEntry) == 0) return;

  dictIterator *iter = dictGetIterator(specIdToEntry);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    docIdMetaUnlinkEntry(ctx, &keyName, KEY_TO_SPECID(dictGetKey(de)), dictGetVal(de));
  }
  dictReleaseIterator(iter);
}

static bool docIdMetaEntryIsStale(dictEntry *de) {
  const DocIdMetaEntry *entry = dictGetVal(de);
  return entry->docId == DOCID_META_INVALID || !isSpecValid(KEY_TO_SPECID(dictGetKey(de)));
}

// Tell Redis about an in-place edit so Flex can write the updated metadata back.
static int docIdMetaMarkChanged(RedisModuleKey *key, uint64_t meta) {
  return RedisModule_SetKeyMeta(docIdKeyMetaClassId, key, meta);
}

// Clear an empty dictionary. In RAM, compact a dictionary with one entry left.
static int docIdMetaAfterDictRemoval(RedisModuleKey *key, uint64_t meta) {
  dict *entries = (dict *)meta;
  size_t count = dictSize(entries);
  if (count == 0) return docIdMetaReplace(key, meta, 0);
  if (!docIdMetaCompactRam || count > 1) return docIdMetaMarkChanged(key, meta);

  // Start before the first bucket to find the only entry.
  dictIterator iter = {.d = entries, .index = -1};
  dictEntry *last = dictNext(&iter);
  const DocIdMetaEntry *entry = dictGetVal(last);
  uint64_t single = docIdMetaSingleCreate(KEY_TO_SPECID(dictGetKey(last)), *entry);

  if (docIdMetaReplace(key, meta, single) == REDISMODULE_OK) return REDISMODULE_OK;

  // If the swap fails, the dictionary still owns the surviving entry.
  return docIdMetaMarkChanged(key, meta);
}

static int docIdMetaPruneDeletedSpecsWithOpenKey(RedisModuleKey *key) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (meta == 0) {
    return REDISMODULE_OK;
  }

  DocIdMetaSingle *single = docIdMetaAsSingle(meta);
  if (single) {
    if (single->entry.docId == DOCID_META_INVALID || !isSpecValid(single->specId)) {
      single->entry.docId = DOCID_META_INVALID;
      return docIdMetaReplace(key, meta, 0);
    }
    return REDISMODULE_OK;
  }

  dict *specIdToEntry = (dict *)meta;
  bool removed = false;
  dictIterator *iter = dictGetSafeIterator(specIdToEntry);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    if (docIdMetaEntryIsStale(de)) {
      dictDelete(specIdToEntry, dictGetKey(de));
      removed = true;
    }
  }
  dictReleaseIterator(iter);

  if (removed || dictSize(specIdToEntry) == 0) {
    return docIdMetaAfterDictRemoval(key, meta);
  }
  return REDISMODULE_OK;
}

// Return values for RedisModuleKeyMetaLoadFunc (documented on RM_CreateKeyMetaClass):
//   1: attach the loaded meta to the key
//   0: skip/ignore (do not attach) - not an error
//  -1: error, abort RDB load
#define DOCID_META_RDB_LOAD_ATTACH 1
#define DOCID_META_RDB_LOAD_SKIP 0
#define DOCID_META_RDB_LOAD_ERROR (-1)

static int docIdMetaRDBLoad(RedisModuleIO *rdb, uint64_t *meta, int encver) {
  if (encver < 1 || encver > DOCID_META_VERSION) {
    // A reader is never older than its data (upgrades only), so this is corruption or a
    // downgrade; refuse rather than misparse the stream.
    RedisModule_Log(RSDummyContext, "warning",
                    "DocIdMeta: cannot load key metadata version %d, this build reads up to %d",
                    encver, DOCID_META_VERSION);
    *meta = 0;
    return DOCID_META_RDB_LOAD_ERROR;
  }

  // Cache the flag locally to ensure all decisions in this callback observe a
  // consistent value, although it cannot really happen, this gives certainty to static analyzers.
  const bool forgetDocIDMetadata = ForgetDocIdMetadata;

  // Even when forgetDocIDMetadata is set we must consume exactly the bytes
  // that docIdMetaRDBSave wrote: the key-meta framework reads a trailing EOF
  // marker right after this callback returns and expects the stream to be
  // positioned at it. Discarding the parsed entries is fine; skipping the
  // reads would desynchronize the stream and fail the EOF check.
  dict *specIdToEntry = forgetDocIDMetadata ? NULL : docIdMetaDictCreate();
  size_t numEntries;

  // Load the number of entries
  numEntries = LoadUnsigned_IOError(rdb, goto cleanup);

  // Load each entry, skipping entries whose spec no longer exists.
  for (size_t i = 0; i < numEntries; i++) {
    uint64_t specId = LoadUnsigned_IOError(rdb, goto cleanup);
    uint64_t docId = LoadUnsigned_IOError(rdb, goto cleanup);
    // Version 1 did not record the length, so the doc-table row stays its only source.
    uint32_t docLen = DOCID_META_DOCLEN_UNKNOWN;
    if (encver >= 2) {
      uint64_t loadedLen = LoadUnsigned_IOError(rdb, goto cleanup);
      if (loadedLen > UINT32_MAX) {
        RedisModule_Log(RSDummyContext, "warning",
                        "DocIdMeta: document length %llu does not fit in 32 bits",
                        (unsigned long long)loadedLen);
        goto cleanup;
      }
      docLen = (uint32_t)loadedLen;
    }

    // While persistence is in progress, drain the bytes but do not attach.
    if (forgetDocIDMetadata) continue;

    // Skip entries belonging to indexes that are no longer in specIdDict_g (O(1) lookup).
    // Entries are keyed by the specIds of the process that saved the key. Those match this
    // process's indexes only after an SST load, where each index keeps its saved specId
    // (see IndexSpec_RdbLoad). A key imported by slot migration would carry the source
    // shard's specIds, and one of them can be the id of a different index on this shard.
    if (!isSpecValid(specId)) {
      continue;
    }

    docIdMetaDictPut(specIdToEntry, specId, docId, docLen);
  }

  if (forgetDocIDMetadata) {
    *meta = 0;
    return DOCID_META_RDB_LOAD_SKIP;
  }

  *meta = (uint64_t)(specIdToEntry);
  return DOCID_META_RDB_LOAD_ATTACH;

cleanup:
  if (specIdToEntry) {
    dictRelease(specIdToEntry);
  }
  *meta = 0;
  return DOCID_META_RDB_LOAD_ERROR;
}

static void docIdMetaRDBSave(RedisModuleIO *rdb, void *value, uint64_t *meta) {
  REDISMODULE_NOT_USED(value);

  if (ForgetDocIdMetadata) {
    // Skip saving during persistence events. We don't want to save this metadata to an RDB/AOF file
    return;
  }

  if (*meta == 0) {
    return;
  }

  dict *specIdToEntry = (dict *)*meta;

  // First pass: count the entries worth saving.
  size_t validEntries = 0;
  dictIterator *iter = dictGetIterator(specIdToEntry);
  dictEntry *de;
  while ((de = dictNext(iter))) {
    if (!docIdMetaEntryIsStale(de)) {
      validEntries++;
    }
  }
  dictReleaseIterator(iter);

  // Save entry count. Version is handled by encver in the KeyMeta API.
  RedisModule_SaveUnsigned(rdb, validEntries);

  if (validEntries == 0) {
    return;
  }

  // Second pass: save the live entries, in the layout docIdMetaRDBLoad reads for this version.
  iter = dictGetIterator(specIdToEntry);
  while ((de = dictNext(iter))) {
    if (docIdMetaEntryIsStale(de)) {
      continue;
    }
    const DocIdMetaEntry *entry = dictGetVal(de);
    RedisModule_SaveUnsigned(rdb, KEY_TO_SPECID(dictGetKey(de)));
    RedisModule_SaveUnsigned(rdb, entry->docId);
    RedisModule_SaveUnsigned(rdb, entry->docLen);
  }
  dictReleaseIterator(iter);
}

bool DocIdMeta_Init(RedisModuleCtx *ctx) {
  if (!RedisModule_CreateKeyMetaClass || !RedisModule_SetKeyMeta || !RedisModule_GetKeyMeta) {
    RedisModule_Log(ctx, "warning",
                    "DocIdMeta requires the Redis key metadata API (Redis 8.6.0 or newer)");
    return false;
  }
  RS_ASSERT(RedisModule_CreateKeyMetaClass);
  RS_ASSERT(RedisModule_SetKeyMeta);
  RS_ASSERT(RedisModule_GetKeyMeta);

  docIdMetaDictType = dictTypeUint64;
  docIdMetaDictType.valDestructor = docIdMetaEntryFree;

  // RAM rebuilds DocIdMeta by reindexing on load. The ignore flag lets RAM
  // skip Flex metadata in an RDB when there is no load callback.
  const bool onDisk = SearchDisk_IsEnabledForValidation();
  docIdMetaCompactRam = !onDisk;
  RedisModuleKeyMetaClassConfig docIdKeyMetaClassIdConfig = {
      .version = REDISMODULE_KEY_META_VERSION,
      .reset_value = 0,
      .flags = 1 << REDISMODULE_META_ALLOW_IGNORE,
      .copy = NULL,    // If NULL, meta is not copied during copy operations
      .rename = NULL,  // If NULL, meta is kept during rename
      .move = (RedisModuleKeyMetaMoveFunc)docIdMetaMove,
      .unlink = (RedisModuleKeyMetaUnlinkFunc)docIdMetaUnlink,
      .free = (RedisModuleKeyMetaFreeFunc)docIdMetaFree,
      .rdb_load = onDisk ? (RedisModuleKeyMetaLoadFunc)docIdMetaRDBLoad : NULL,
      .rdb_save = onDisk ? (RedisModuleKeyMetaSaveFunc)docIdMetaRDBSave : NULL,
      .aof_rewrite = NULL,
      .defrag = NULL,
      .mem_usage = NULL,
      .free_effort = NULL,
  };
  docIdKeyMetaClassId = RedisModule_CreateKeyMetaClass(
      ctx, DOCID_META_CLASS_NAME, DOCID_META_VERSION, &docIdKeyMetaClassIdConfig);
  if (docIdKeyMetaClassId < 0) {
    RedisModule_Log(ctx, "error", "Failed to create DocIdMeta class");
    return false;
  }
  return true;
}

// Set the entry on an already-open key. The caller owns `key` and must have opened
// it with read+write access. The name-based DocIdMeta_Set delegates here.
int DocIdMeta_SetWithOpenKey(RedisModuleKey *key, uint64_t specId, uint64_t docId,
                             uint32_t docLen) {
  RS_ASSERT(docId != DOCID_META_INVALID);
  const DocIdMetaEntry newEntry = {.docId = docId, .docLen = docLen};
  uint64_t meta = 0;

  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK || meta == 0) {
    // The first membership starts out compact in RAM and in a dictionary in Flex.
    uint64_t newMeta;
    if (docIdMetaCompactRam) {
      newMeta = docIdMetaSingleCreate(specId, newEntry);
    } else {
      dict *entries = docIdMetaDictCreate();
      docIdMetaDictPut(entries, specId, docId, docLen);
      newMeta = (uint64_t)entries;
    }
    int rc = docIdMetaReplace(key, 0, newMeta);
    if (rc != REDISMODULE_OK) {
      RedisModule_Log(RSDummyContext, "warning",
                      "DocIdMeta: failed to set metadata for key during DocIdMeta_SetWithOpenKey");
    }
    return rc;
  }

  DocIdMetaSingle *single = docIdMetaAsSingle(meta);
  if (!single) {
    docIdMetaDictPut((dict *)meta, specId, docId, docLen);
    return docIdMetaMarkChanged(key, meta);
  }
  if (single->specId == specId) {
    int rc = docIdMetaMarkChanged(key, meta);
    if (rc == REDISMODULE_OK) single->entry = newEntry;
    return rc;
  }

  // Build the two-entry dictionary before replacing the compact record.
  dict *entries = docIdMetaDictCreate();
  docIdMetaDictPut(entries, single->specId, single->entry.docId, single->entry.docLen);
  docIdMetaDictPut(entries, specId, docId, docLen);
  return docIdMetaReplace(key, meta, (uint64_t)entries);
}

// The live entry for `specId` on an already-open key, or NULL when there is none.
static const DocIdMetaEntry *docIdMetaFindWithOpenKey(RedisModuleKey *key, uint64_t specId) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK || meta == 0) {
    return NULL;
  }
  const DocIdMetaEntry *entry;
  const DocIdMetaSingle *single = docIdMetaAsSingle(meta);
  if (single) {
    if (single->specId != specId) return NULL;
    entry = &single->entry;
  } else {
    const dictEntry *de = dictFind((dict *)meta, SPECID_TO_KEY(specId));
    if (!de) return NULL;
    entry = dictGetVal(de);
  }
  return entry->docId == DOCID_META_INVALID ? NULL : entry;
}

// Get docId from an already-open key. The caller owns `key` and must have opened
// it with read access. The name-based DocIdMeta_Get delegates here.
int DocIdMeta_GetWithOpenKey(RedisModuleKey *key, uint64_t specId, uint64_t *docId) {
  const DocIdMetaEntry *entry = docIdMetaFindWithOpenKey(key, specId);
  if (!entry) {
    return REDISMODULE_ERR;
  }
  *docId = entry->docId;
  return REDISMODULE_OK;
}

int DocIdMeta_GetEntryWithOpenKey(RedisModuleKey *key, uint64_t specId, uint64_t *docId,
                                  uint32_t *docLen) {
  const DocIdMetaEntry *entry = docIdMetaFindWithOpenKey(key, specId);
  if (!entry) {
    return REDISMODULE_ERR;
  }
  *docId = entry->docId;
  *docLen = entry->docLen;
  return REDISMODULE_OK;
}

// Delete the specId entry from an already-open key. The caller owns `key` and
// must have opened it with read+write access. The name-based DocIdMeta_Delete
// delegates here.
int DocIdMeta_DeleteWithOpenKey(RedisModuleKey *key, uint64_t specId) {
  uint64_t meta = 0;
  if (RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta) != REDISMODULE_OK || meta == 0) {
    return REDISMODULE_ERR;
  }
  DocIdMetaSingle *single = docIdMetaAsSingle(meta);
  if (single) {
    if (single->specId != specId) return REDISMODULE_ERR;
    // Invalidate the entry even if Redis fails to clear the slot.
    single->entry.docId = DOCID_META_INVALID;
    return docIdMetaReplace(key, meta, 0);
  }

  dict *specIdToEntry = (dict *)meta;
  static_assert(DICT_OK == REDISMODULE_OK);
  static_assert(DICT_ERR == REDISMODULE_ERR);
  int rc = dictDelete(specIdToEntry, SPECID_TO_KEY(specId));
  if (rc != DICT_OK) {
    return rc;
  }
  return docIdMetaAfterDictRemoval(key, meta);
}

// Set the entry using key name and spec incarnation ID.
int DocIdMeta_Set(RedisModuleCtx *ctx, RedisModuleString *keyName, uint64_t specId,
                  uint64_t docId, uint32_t docLen) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_SET_FLAGS);
  RS_LOG_ASSERT(key, "DocIdMeta: failed to open key for write");
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_SetWithOpenKey(key, specId, docId, docLen);
  RedisModule_CloseKey(key);
  return result;
}

// Get docId using key name and spec incarnation ID
int DocIdMeta_Get(RedisModuleCtx *ctx, RedisModuleString *keyName, uint64_t specId,
                  uint64_t *docId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_GET_FLAGS);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_GetWithOpenKey(key, specId, docId);
  RedisModule_CloseKey(key);
  return result;
}

int DocIdMeta_GetEntry(RedisModuleCtx *ctx, RedisModuleString *keyName, uint64_t specId,
                       uint64_t *docId, uint32_t *docLen) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_GET_FLAGS);
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_GetEntryWithOpenKey(key, specId, docId, docLen);
  RedisModule_CloseKey(key);
  return result;
}

int DocIdMeta_Delete(RedisModuleCtx *ctx, RedisModuleString *keyName, uint64_t specId) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_SET_FLAGS);
  RS_LOG_ASSERT(key, "DocIdMeta: failed to open key for write");
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = DocIdMeta_DeleteWithOpenKey(key, specId);
  RedisModule_CloseKey(key);
  return result;
}

int DocIdMeta_Clear(RedisModuleCtx *ctx, RedisModuleString *keyName) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_SET_FLAGS);
  RS_LOG_ASSERT(key, "DocIdMeta: failed to open key for write");
  if (!key) {
    return REDISMODULE_ERR;
  }
  uint64_t meta = 0;
  int rc = RedisModule_GetKeyMeta(docIdKeyMetaClassId, key, &meta);
  if (rc == REDISMODULE_OK && meta != 0) {
    rc = docIdMetaReplace(key, meta, 0);
    RS_LOG_ASSERT(rc == REDISMODULE_OK, "DocIdMeta: failed to clear metadata");
  }
  RedisModule_CloseKey(key);
  return rc;
}

int DocIdMeta_PruneDeletedSpecs(RedisModuleCtx *ctx, RedisModuleString *keyName) {
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, KEY_OPEN_META_SET_FLAGS);
  RS_LOG_ASSERT(key, "DocIdMeta: failed to open key for write");
  if (!key) {
    return REDISMODULE_ERR;
  }
  int result = docIdMetaPruneDeletedSpecsWithOpenKey(key);
  RedisModule_CloseKey(key);
  return result;
}
