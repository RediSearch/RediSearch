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

#include "util/misc.h"
#include "rmutil/rm_assert.h"

#define LEGACY_ENC_VER 1
#define LEGACY_LEGACY_ENC_VER 0
// Version we write for a legacy key that outlived the upgrade sweep. Such a key holds only the
// sentinel, so there is nothing to serialize - and stamping the record with a version the loader
// recognises is what makes an empty payload legal: the loader consumes no bytes, leaving Redis's module
// EOF marker exactly where it left it. Writing nothing under LEGACY_ENC_VER is what corrupted the RDB,
// because the loader then read a payload that was not there and ate the marker instead.
#define LEGACY_EMPTY_ENC_VER 2

// RDB load callback cannot return NULL, as it indicates an error
void *dummyNonNull = (void*)0xDEADBEEF;

// Dummy no-op functions for type methods
void GenericType_DummyRdbSave(RedisModuleIO *rdb, void *value) {
  // Writes nothing; the record's LEGACY_EMPTY_ENC_VER tells the loader not to expect a payload.
  // _ALWAYS because a value that is not the sentinel would be dropped silently, and RS_ABORT here
  // compiled to nothing in release builds - which is the bug this replaces.
  RS_ASSERT_ALWAYS(value == dummyNonNull);
}

void GenericType_DummyFree(void *value) {
  RS_ASSERT(value == dummyNonNull);
}

// Consume an inverted index type from RDB
void *InvertedIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
  if (encver == LEGACY_EMPTY_ENC_VER) {
    return dummyNonNull; // written by GenericType_DummyRdbSave: no payload to consume
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
    return dummyNonNull; // written by GenericType_DummyRdbSave: no payload to consume
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
    return dummyNonNull; // written by GenericType_DummyRdbSave: no payload to consume
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
    .rdb_save = GenericType_DummyRdbSave,
    .aof_rewrite = GenericAofRewrite_DisabledHandler,
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
