/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"

#include "spec.h"
#include "doc_id_meta.h"

#include <string>

// A flushed db and a fresh index name per test, with OPTIMIZE_PARTIAL_UPDATE forced on.
class PartialUpdateTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx = nullptr;
  IndexSpec *spec = nullptr;
  std::string indexName;
  bool previousOptimizePartialUpdate = false;

  explicit PartialUpdateTest(const char *indexPrefix) : indexPrefix(indexPrefix) {
  }

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    RMCK::flushdb(ctx);
    static int counter = 0;
    indexName = indexPrefix + std::to_string(++counter);
    // The partial-update fast paths are gated behind OPTIMIZE_PARTIAL_UPDATE (on by default).
    // Forced here so a config change elsewhere can't disable them out from under these tests;
    // restored in TearDown, which runs even when an assertion fails.
    previousOptimizePartialUpdate = RSGlobalConfig.optimizePartialUpdate;
    RSGlobalConfig.optimizePartialUpdate = true;
  }

  void TearDown() override {
    RSGlobalConfig.optimizePartialUpdate = previousOptimizePartialUpdate;
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }

  t_docId docIdOf(const char *key) {
    uint64_t docId = 0;
    if (DocIdMeta_Get(ctx, RMCK::RString(key), spec->specId, &docId) != REDISMODULE_OK) {
      return 0;
    }
    return (t_docId)docId;
  }

  // Deletes a single Hash field the same way HDEL does (RedisModule_HashSet with
  // REDISMODULE_HASH_DELETE); unlike RMCK::hset, there is no convenience wrapper for this.
  void hdel(const char *key, const char *field) {
    RedisModuleKey *k = RedisModule_OpenKey(ctx, RMCK::RString(key), REDISMODULE_WRITE);
    RedisModule_HashSet(k, REDISMODULE_HASH_CFIELDS, field, REDISMODULE_HASH_DELETE, nullptr);
    RedisModule_CloseKey(k);
  }

 private:
  const char *indexPrefix;
};
