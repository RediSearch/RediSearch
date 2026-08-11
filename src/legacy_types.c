/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "legacy_types.h"

#include <stddef.h>

#include "rmutil/rm_assert.h"

#define LEGACY_ENC_VER 1
#define LEGACY_LEGACY_ENC_VER 0
// Version we write for a legacy key that outlived the upgrade sweep. Such a key carries no data, so
// there is nothing to serialize - and stamping the record with a version the loader recognises is what
// makes an empty payload legal: the loader reads nothing, so Redis finds the module EOF marker exactly
// where it left it. Writing zero bytes under version 1 is what corrupted the RDB, because the loader
// then tried to read a payload that was not there and consumed the marker instead.
#define LEGACY_EMPTY_ENC_VER 2

// RDB load callback cannot return NULL, as it indicates an error
void *dummyNonNull = (void*)0xDEADBEEF;

// A legacy key that survived the upgrade sweep holds only `dummyNonNull` - the real index payload was
// consumed and discarded on load, so there is genuinely nothing to write. Emitting no payload is only
// safe because the record is stamped LEGACY_EMPTY_ENC_VER; see the loaders below.
//
// The assertion is `_ALWAYS`: a value that is not the sentinel would be silently dropped, so a release
// build must crash rather than lose data. A disappearing debug-only assertion on this exact path is
// what produced the original corruption.
void GenericType_EmptyRdbSave(RedisModuleIO *rdb, void *value) {
  RS_ASSERT_ALWAYS(value == dummyNonNull);
}

// The shared handler calls abort(), which kills the AOF rewrite child every time a database holding
// a legacy key rewrites - so the rewrite never completes and the AOF grows without bound.
//
// Emit the key's own serialization instead, so that a command-only AOF and an AOF with an RDB
// preamble agree: both keep the key. Asking Redis for the payload via DUMP avoids hand-building the
// envelope (RDB version plus CRC), and the rdb_save callbacks above are what make DUMP succeed. The
// rewrite child has a consistent snapshot, so the value cannot change underneath us.
void LegacyType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  RS_ASSERT_ALWAYS(value == dummyNonNull);

  // No ThreadSafeContextLock: the rewrite child inherits the module GIL already held, and it is
  // single-threaded. Locking here would deadlock.
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  // A detached context starts on DB 0, but the key lives in whichever database is being rewritten.
  // Failing to switch would dump a same-named, unrelated key from DB 0.
  RS_ASSERT_ALWAYS(RedisModule_SelectDb(ctx, RedisModule_GetDbIdFromIO(aof)) == REDISMODULE_OK);

  RedisModuleCallReply *rep = RedisModule_Call(ctx, "DUMP", "s", key);
  const int reply_type = rep ? RedisModule_CallReplyType(rep) : REDISMODULE_REPLY_UNKNOWN;

  if (reply_type == REDISMODULE_REPLY_STRING) {
    size_t n = 0;
    const char *payload = RedisModule_CallReplyStringPtr(rep, &n);
    // Redis emits the key's PEXPIREAT itself after this, so a TTL is preserved.
    // The TTL must be a long long: "l" consumes one, and a bare 0 is an int - undefined behaviour
    // wherever variadic int and long long are laid out differently.
    RedisModule_EmitAOF(aof, "RESTORE", "slb", key, 0LL, payload, n);
  } else if (reply_type == REDISMODULE_REPLY_NULL) {
    // The key expired between the fork snapshot and this DUMP. Nothing to recreate.
  } else {
    // Anything else would silently install an AOF base that is missing this key, which is exactly
    // the mode-dependent divergence this callback exists to prevent. Fail the rewrite instead: Redis
    // discards a rewrite whose child dies, so the previous base stays valid.
    RedisModule_Log(RedisModule_GetContextFromIO(aof), "warning",
                    "DUMP failed while rewriting a legacy index key into the AOF");
    RS_ABORT_ALWAYS("DUMP failed for a legacy index key during AOF rewrite");
  }

  if (rep != NULL) {
    RedisModule_FreeCallReply(rep);
  }
  RedisModule_FreeThreadSafeContext(ctx);
}

void GenericType_DummyFree(void *value) {
  RS_ASSERT(value == dummyNonNull);
}

// Consume an inverted index type from RDB
void *InvertedIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
  if (encver == LEGACY_EMPTY_ENC_VER) {
    // Written by GenericType_EmptyRdbSave: no payload to consume.
    return dummyNonNull;
  }

  if (encver > LEGACY_ENC_VER) {
    return NULL;
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
  return dummyNonNull;
}

// Consume a numeric index type from RDB
void *NumericIndexType_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
  if (encver == LEGACY_EMPTY_ENC_VER) {
    // Written by GenericType_EmptyRdbSave: no payload to consume.
    return dummyNonNull;
  }

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

  return dummyNonNull;
}

// Consume a tag index type from RDB
void *TagIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
  if (encver == LEGACY_EMPTY_ENC_VER) {
    // Written by GenericType_EmptyRdbSave: no payload to consume.
    return dummyNonNull;
  }

  size_t n_tags = RedisModule_LoadUnsigned(rdb); // Consume the number of tags in the index

  for (size_t i = 0; i < n_tags; i++) {
    RedisModule_Free(RedisModule_LoadStringBuffer(rdb, NULL)); // Consume the tag value
    InvertedIndex_RdbLoad_Consume(rdb, encver); // Consume the inverted index for the tag
  }
  return dummyNonNull;
}

int RegisterLegacyTypes(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {
    .version = REDISMODULE_TYPE_METHOD_VERSION,
    .rdb_save = GenericType_EmptyRdbSave,
    .aof_rewrite = LegacyType_AofRewrite,
    .free = GenericType_DummyFree,
  };

  // Register the inverted index type
  tm.rdb_load = InvertedIndex_RdbLoad_Consume;
  if (!RedisModule_CreateDataType(ctx, "ft_invidx", LEGACY_EMPTY_ENC_VER, &tm)) {
    return REDISMODULE_ERR;
  }

  // Register the numeric index type
  tm.rdb_load = NumericIndexType_RdbLoad_Consume;
  if (!RedisModule_CreateDataType(ctx, "numericdx", LEGACY_EMPTY_ENC_VER, &tm)) {
    return REDISMODULE_ERR;
  }

  // Register the tag index type
  tm.rdb_load = TagIndex_RdbLoad_Consume;
  if (!RedisModule_CreateDataType(ctx, "ft_tagidx", LEGACY_EMPTY_ENC_VER, &tm)) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
