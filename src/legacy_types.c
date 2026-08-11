/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "legacy_types.h"

#include <stdbool.h>

#include "rmalloc.h"
#include "notifications.h"
#include <stddef.h>

#include "util/misc.h"
#include "rmutil/rm_assert.h"

#define LEGACY_ENC_VER 1
#define LEGACY_LEGACY_ENC_VER 0

// RDB load callback cannot return NULL, as it indicates an error
void *dummyNonNull = (void*)0xDEADBEEF;

// Retained so the cleanup below recognises a key by type rather than by name - an untrusted payload
// does not have to respect any naming convention.
static RedisModuleType *LegacyInvertedIndexType = NULL;
static RedisModuleType *LegacyNumericIndexType = NULL;
static RedisModuleType *LegacyTagIndexType = NULL;

// How many legacy keys the load in progress materialized. Only the loaders below touch it, so it
// stays zero for every database that has never held one - which is what keeps the post-load sweep
// free for everyone else.
static size_t legacyKeysLoaded = 0;

// Called whenever a legacy key is deserialized, from the type callbacks below.
//
// Subscribing to keyspace events here rather than at module load is what keeps this free for everyone
// else: a database that never deserializes one of these keys never starts listening. It has to happen
// in the loader because the subscription is otherwise lazy - Initialize_KeyspaceNotifications runs
// only when an index is created or loaded, so a database with no index has no subscriber at all and
// the `restore` event this cleanup depends on would never reach us. That is exactly the shape of the
// database this was reported on: ~101k legacy keys and search_number_of_indexes:0.
static void noteLegacyKeyLoaded(void) {
  legacyKeysLoaded++;
  Initialize_KeyspaceNotifications();
}

// Dummy no-op functions for type methods
void GenericType_DummyRdbSave(RedisModuleIO *rdb, void *value) {
  RS_ABORT("Attempted to save a legacy type to RDB");
}

void GenericType_DummyFree(void *value) {
  RS_ASSERT(value == dummyNonNull);
}

// Consume an inverted index payload. Separate from the type callback because the tag loader consumes
// one of these per tag, and a tag is not a key - counting them would inflate the load count.
static bool consumeInvertedIndex(RedisModuleIO *rdb, int encver) {
  if (encver > LEGACY_ENC_VER) {
    return false;
  }

  RedisModule_LoadUnsigned(rdb); // Consume the flags of the index
  RedisModule_LoadUnsigned(rdb); // Consume the lastId of the index
  RedisModule_LoadUnsigned(rdb); // Consume the number of documents in the index
  size_t n_blocks = RedisModule_LoadUnsigned(rdb); // Load the number of blocks in the index

  for (size_t i = 0; i < n_blocks; i++) {
    RedisModule_LoadUnsigned(rdb); // Consume the firstId of the block
    RedisModule_LoadUnsigned(rdb); // Consume the lastId of the block
    RedisModule_LoadUnsigned(rdb); // Consume the number of entries in the block
    RedisModule_Free(RedisModule_LoadStringBuffer(rdb, NULL)); // Consume the buffer of the block
  }
  return true;
}

void *InvertedIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
  if (!consumeInvertedIndex(rdb, encver)) {
    return NULL;
  }
  noteLegacyKeyLoaded();
  return dummyNonNull;
}

// Consume a numeric index type from RDB
void *NumericIndexType_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
  if (encver > LEGACY_ENC_VER) {
    return NULL;
  }

  if (encver == LEGACY_LEGACY_ENC_VER) {
    // Version 0 stores the number of entries beforehand, and then loads them
    size_t num = RedisModule_LoadUnsigned(rdb);
    for (size_t ii = 0; ii < num; ++ii) {
      RedisModule_LoadUnsigned(rdb); // Consume the document ID
      RedisModule_LoadDouble(rdb); // Consume the value
    }
  } else if (encver == LEGACY_ENC_VER) {
    // Version 1 stores (id,value) pairs, with a final 0 as a terminator
    while (RedisModule_LoadUnsigned(rdb)) { // Consume the document ID
      RedisModule_LoadDouble(rdb); // Consume the value
    }
  }

  noteLegacyKeyLoaded();
  return dummyNonNull;
}

// Consume a tag index type from RDB
void *TagIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
  size_t n_tags = RedisModule_LoadUnsigned(rdb); // Consume the number of tags in the index

  for (size_t i = 0; i < n_tags; i++) {
    RedisModule_Free(RedisModule_LoadStringBuffer(rdb, NULL)); // Consume the tag value
    consumeInvertedIndex(rdb, encver); // Consume the inverted index for the tag
  }
  noteLegacyKeyLoaded();
  return dummyNonNull;
}

/* ---------------------------------------------------------------------------------------------
 * Cleanup of orphaned legacy keys. See legacy_types.h for why they exist at all.
 * ------------------------------------------------------------------------------------------- */

// True when `kp` holds one of our legacy types *and* the sentinel. Both halves matter: the type alone
// would match a future non-sentinel value, and the sentinel alone is just an integer.
static bool isOrphanedLegacyKey(RedisModuleKey *kp) {
  if (kp == NULL || RedisModule_KeyType(kp) != REDISMODULE_KEYTYPE_MODULE) {
    return false;
  }
  RedisModuleType *type = RedisModule_ModuleTypeGetType(kp);
  if (type != LegacyInvertedIndexType && type != LegacyNumericIndexType &&
      type != LegacyTagIndexType) {
    return false;
  }
  return RedisModule_ModuleTypeGetValue(kp) == dummyNonNull;
}

static bool isOrphanedLegacyKeyByName(RedisModuleCtx *ctx, RedisModuleString *key) {
  RedisModuleKey *kp = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
  const bool orphaned = isOrphanedLegacyKey(kp);
  if (kp != NULL) {
    RedisModule_CloseKey(kp);
  }
  return orphaned;
}

static void deleteWithPropagation(RedisModuleCtx *ctx, RedisModuleString *key) {
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "DEL", "!s", key);
  if (rep != NULL) {
    RedisModule_FreeCallReply(rep);
  }
}

static void freeOwnedKey(void *pd) {
  // Created with a NULL context, so it is ours to release.
  RedisModule_FreeString(NULL, (RedisModuleString *)pd);
}

// Writing from inside a keyspace notification is not allowed, so the delete happens here, once Redis
// says writes are safe. The value is re-checked first: a post-notification job runs after the whole
// execution unit, so `MULTI; RESTORE k <legacy>; SET k valuable; EXEC` would otherwise lose
// `valuable`.
//
// The key arrives as owned privdata rather than through the per-key job variant, because
// RedisModule_AddPostNotificationJobForKey exists only in the Enterprise build - against OSS Redis
// that pointer is NULL and calling it crashes the server.
static void deleteOrphanedLegacyKey(RedisModuleCtx *ctx, void *pd) {
  RedisModuleString *key = pd;
  if (isOrphanedLegacyKeyByName(ctx, key)) {
    deleteWithPropagation(ctx, key);
  }
}

void LegacyTypes_CleanupRestoredKey(RedisModuleCtx *ctx, RedisModuleString *key) {
  // Only a primary may delete. A replica keeps the key until the primary's DEL arrives - deleting
  // locally would diverge from a primary that still holds it, and post-notification jobs are rejected
  // on read-only replicas anyway. Tested positively: if the role is somehow unknown, doing nothing is
  // the safe outcome.
  if (!(RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_MASTER)) {
    return;
  }
  if (!isOrphanedLegacyKeyByName(ctx, key)) {
    return;
  }
  // The notification's key is not ours to retain, so hand the job an owned copy.
  RedisModuleString *owned = RedisModule_CreateStringFromString(NULL, key);
  if (RedisModule_AddPostNotificationJob(ctx, deleteOrphanedLegacyKey, owned, freeOwnedKey) !=
      REDISMODULE_OK) {
    RedisModule_FreeString(NULL, owned);
    RedisModule_Log(ctx, "warning",
                    "Could not schedule cleanup of a restored legacy index key; it will remain until "
                    "the next load or a manual UNLINK");
  }
}

void LegacyTypes_ResetLoadedCount(void) {
  legacyKeysLoaded = 0;
}

typedef struct {
  RedisModuleString **keys;
  size_t len;
  size_t cap;
} OrphanedKeys;

// Collect rather than delete inline: mutating the keyspace while `RedisModule_Scan` walks it is not
// something the API promises to tolerate.
static void collectOrphanedLegacyKey(RedisModuleCtx *ctx, RedisModuleString *keyname,
                                     RedisModuleKey *key, void *privdata) {
  if (!isOrphanedLegacyKey(key)) {
    return;
  }
  OrphanedKeys *found = privdata;
  if (found->len == found->cap) {
    found->cap = found->cap ? found->cap * 2 : 64;
    found->keys = rm_realloc(found->keys, found->cap * sizeof(*found->keys));
  }
  found->keys[found->len++] = RedisModule_HoldString(ctx, keyname);
}

void LegacyTypes_SweepOrphansAfterLoad(RedisModuleCtx *ctx) {
  const size_t counted = legacyKeysLoaded;
  legacyKeysLoaded = 0;
  if (counted == 0) {
    return;
  }

  if (!(RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_MASTER)) {
    // During a full sync only the replica is loading; the primary is not. Deleting here would diverge
    // from a primary that still holds these keys, so wait for its DEL.
    RedisModule_Log(ctx, "notice",
                    "Loaded %zu legacy index keys without being a writable primary; leaving them for "
                    "the primary to clean up", counted);
    return;
  }

  OrphanedKeys found = {0};
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  while (RedisModule_Scan(ctx, cursor, collectOrphanedLegacyKey, &found)) {
  }
  RedisModule_ScanCursorDestroy(cursor);

  size_t deleted = 0;
  for (size_t i = 0; i < found.len; i++) {
    // Re-check: the spec-driven upgrade sweep ran before this and may already have removed the key,
    // and an AOF can replay `RESTORE k <legacy>` followed by `SET k valuable`.
    if (isOrphanedLegacyKeyByName(ctx, found.keys[i])) {
      deleteWithPropagation(ctx, found.keys[i]);
      deleted++;
    }
    RedisModule_FreeString(ctx, found.keys[i]);
  }
  rm_free(found.keys);

  // `counted` exceeding `deleted` is normal rather than a failure: the upgrade sweep removes the keys
  // it knows about first. Report both so the difference is attributable instead of alarming.
  RedisModule_Log(ctx, "notice",
                  "Removed %zu orphaned legacy index keys (%zu were loaded; the rest had already been "
                  "cleaned up by the index upgrade)", deleted, counted);
}

int RegisterLegacyTypes(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {
    .version = REDISMODULE_TYPE_METHOD_VERSION,
    .rdb_save = GenericType_DummyRdbSave,
    .aof_rewrite = GenericAofRewrite_DisabledHandler,
    .free = GenericType_DummyFree,
  };

  // Register the inverted index type
  tm.rdb_load = InvertedIndex_RdbLoad_Consume;
  LegacyInvertedIndexType = RedisModule_CreateDataType(ctx, "ft_invidx", LEGACY_ENC_VER, &tm);
  if (!LegacyInvertedIndexType) {
    return REDISMODULE_ERR;
  }

  // Register the numeric index type
  tm.rdb_load = NumericIndexType_RdbLoad_Consume;
  LegacyNumericIndexType = RedisModule_CreateDataType(ctx, "numericdx", LEGACY_ENC_VER, &tm);
  if (!LegacyNumericIndexType) {
    return REDISMODULE_ERR;
  }

  // Register the tag index type
  tm.rdb_load = TagIndex_RdbLoad_Consume;
  LegacyTagIndexType = RedisModule_CreateDataType(ctx, "ft_tagidx", LEGACY_ENC_VER, &tm);
  if (!LegacyTagIndexType) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
