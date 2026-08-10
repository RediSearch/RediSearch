/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "legacy_types.h"
#include "rmutil/rm_assert.h"
#include <stdbool.h>

#define LEGACY_ENC_VER 1
#define LEGACY_LEGACY_ENC_VER 0

// RDB load callback cannot return NULL, as it indicates an error
void *dummyNonNull = (void*)0xDEADBEEF;

// A legacy key that survived the upgrade sweep holds only `dummyNonNull` - the real
// index payload was consumed and discarded on load. These callbacks emit the smallest
// payload each matching `*_RdbLoad_Consume` accepts, so such a key round-trips instead
// of writing zero bytes and desyncing the RDB stream for every later reader.
//
// The assertions are `_ALWAYS`: writing an empty payload over a value that is *not* the
// sentinel would silently discard real data, so a release build must crash rather than
// corrupt. A disappearing debug-only assertion on this exact path is what produced the
// zero-byte save in the first place.
void InvertedIndex_RdbSave_Empty(RedisModuleIO *rdb, void *value) {
  RS_ASSERT_ALWAYS(value == dummyNonNull);
  RedisModule_SaveUnsigned(rdb, 0); // flags
  RedisModule_SaveUnsigned(rdb, 0); // lastId
  RedisModule_SaveUnsigned(rdb, 0); // numDocs
  RedisModule_SaveUnsigned(rdb, 0); // n_blocks
}

void NumericIndexType_RdbSave_Empty(RedisModuleIO *rdb, void *value) {
  RS_ASSERT_ALWAYS(value == dummyNonNull);
  RedisModule_SaveUnsigned(rdb, 0); // terminator for legacy v1 encoding
}

void TagIndex_RdbSave_Empty(RedisModuleIO *rdb, void *value) {
  RS_ASSERT_ALWAYS(value == dummyNonNull);
  RedisModule_SaveUnsigned(rdb, 0); // n_tags
}

// The shared handler aborts the server, which turns "AOF is enabled on a database that still
// holds a legacy key" into a crash on every rewrite. A legacy key carries no data, so emitting no
// commands is safe: nothing is lost, and the key does not come back when the AOF is replayed.
//
// Emitting nothing is also the only option here. Recreating the key would need a RESTORE of a full
// DUMP envelope (payload plus RDB version and CRC), which no module API can build -
// RedisModule_SaveDataTypeToString produces the bare module payload. Deleting the key instead is
// impossible too, because this callback runs in the forked AOF child and cannot touch the parent's
// keyspace.
//
// Consequence worth knowing: a command-only AOF drops these keys on replay, while an AOF with an
// RDB preamble goes through rdb_save and keeps them. That divergence follows from Redis having two
// AOF formats; the only way to make every path agree is for the keys not to exist, which is the
// cleanup work tracked in MOD-15685.
void LegacyType_AofRewrite_Skip(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  RS_ASSERT_ALWAYS(value == dummyNonNull);
}

void GenericType_DummyFree(void *value) {
  RS_ASSERT(value == dummyNonNull);
}

// Every count below is attacker-controlled: these loaders are reachable from RESTORE, so the
// payload is untrusted input. Once the stream is exhausted the Load* calls become no-ops that
// return 0 without consuming anything, so a counted loop must stop on the IO error - otherwise a
// truncated payload declaring UINT64_MAX entries spins the server. Returning NULL on error also
// stops a corrupt payload from materializing a key.

// Consume an inverted index type from RDB
void *InvertedIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
  if (encver > LEGACY_ENC_VER) {
    return NULL;
  }

  RedisModule_LoadUnsigned(rdb); // Consume the flags of the index
  RedisModule_LoadUnsigned(rdb); // Consume the lastId of the index
  RedisModule_LoadUnsigned(rdb); // Consume the number of documents in the index
  size_t n_blocks = RedisModule_LoadUnsigned(rdb); // Load the number of blocks in the index

  for (size_t i = 0; i < n_blocks && !RedisModule_IsIOError(rdb); i++) {
    RedisModule_LoadUnsigned(rdb); // Consume the firstId of the block
    RedisModule_LoadUnsigned(rdb); // Consume the lastId of the block
    RedisModule_LoadUnsigned(rdb); // Consume the number of entries in the block
    RedisModule_Free(RedisModule_LoadStringBuffer(rdb, NULL)); // Consume the buffer of the block
  }
  if (RedisModule_IsIOError(rdb)) {
    return NULL;
  }
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
    for (size_t ii = 0; ii < num && !RedisModule_IsIOError(rdb); ++ii) {
      RedisModule_LoadUnsigned(rdb); // Consume the document ID
      RedisModule_LoadDouble(rdb); // Consume the value
    }
  } else if (encver == LEGACY_ENC_VER) {
    // Version 1 stores (id,value) pairs, with a final 0 as a terminator. A failed read returns 0,
    // so this loop already terminates on an exhausted stream.
    while (RedisModule_LoadUnsigned(rdb)) { // Consume the document ID
      RedisModule_LoadDouble(rdb); // Consume the value
    }
  }

  if (RedisModule_IsIOError(rdb)) {
    return NULL;
  }
  return dummyNonNull;
}

// Consume a tag index type from RDB
void *TagIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
  // Redis matches a module type on its 54-bit signature and ignores the 10-bit encoding version,
  // so a crafted RESTORE can reach this loader with any encver. Guard it like the other two.
  if (encver > LEGACY_ENC_VER) {
    return NULL;
  }

  size_t n_tags = RedisModule_LoadUnsigned(rdb); // Consume the number of tags in the index

  for (size_t i = 0; i < n_tags && !RedisModule_IsIOError(rdb); i++) {
    RedisModule_Free(RedisModule_LoadStringBuffer(rdb, NULL)); // Consume the tag value
    // Propagate the nested failure: ignoring it would keep looping over a dead stream.
    if (InvertedIndex_RdbLoad_Consume(rdb, encver) == NULL) {
      return NULL;
    }
  }
  if (RedisModule_IsIOError(rdb)) {
    return NULL;
  }
  return dummyNonNull;
}

int RegisterLegacyTypes(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {
    .version = REDISMODULE_TYPE_METHOD_VERSION,
    .rdb_save = InvertedIndex_RdbSave_Empty,
    .aof_rewrite = LegacyType_AofRewrite_Skip,
    .free = GenericType_DummyFree,
  };

  // Register the inverted index type
  tm.rdb_load = InvertedIndex_RdbLoad_Consume;
  if (!RedisModule_CreateDataType(ctx, "ft_invidx", LEGACY_ENC_VER, &tm)) {
    return REDISMODULE_ERR;
  }

  // Register the numeric index type
  tm.rdb_load = NumericIndexType_RdbLoad_Consume;
  tm.rdb_save = NumericIndexType_RdbSave_Empty;
  if (!RedisModule_CreateDataType(ctx, "numericdx", LEGACY_ENC_VER, &tm)) {
    return REDISMODULE_ERR;
  }

  // Register the tag index type
  tm.rdb_load = TagIndex_RdbLoad_Consume;
  tm.rdb_save = TagIndex_RdbSave_Empty;
  if (!RedisModule_CreateDataType(ctx, "ft_tagidx", LEGACY_ENC_VER, &tm)) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
