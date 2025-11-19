/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "legacy_types.h"
#include "util/misc.h"
#include "rmutil/rm_assert.h"
#include <stdbool.h>

#define LEGACY_ENC_VER 1
#define LEGACY_LEGACY_ENC_VER 0

// RDB load callback cannot return NULL, as it indicates an error
void *dummyNonNull = (void*)0xDEADBEEF;

// Dummy no-op functions for type methods
void GenericType_DummyRdbSave(RedisModuleIO *rdb, void *value) {
  RS_ABORT("Attempted to save a legacy type to RDB");
}

void GenericType_DummyFree(void *value) {
  RS_ASSERT(value == dummyNonNull);
}

// Consume an inverted index type from RDB
void *InvertedIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver) {
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
  if (!RedisModule_CreateDataType(ctx, "ft_invidx", LEGACY_ENC_VER, &tm)) {
    return REDISMODULE_ERR;
  }

  // Register the numeric index type
  tm.rdb_load = NumericIndexType_RdbLoad_Consume;
  if (!RedisModule_CreateDataType(ctx, "numericdx", LEGACY_ENC_VER, &tm)) {
    return REDISMODULE_ERR;
  }

  // Register the tag index type
  tm.rdb_load = TagIndex_RdbLoad_Consume;
  if (!RedisModule_CreateDataType(ctx, "ft_tagidx", LEGACY_ENC_VER, &tm)) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
