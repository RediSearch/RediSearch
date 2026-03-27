/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "doc_id_meta.h"
#include "spec.h"
#include "common.h"
#include "index_utils.h"
#include "obfuscation/hidden.h"


class DocIdMetaTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Initialize redismock and clear any previous state
    RMCK::init();

    // Initialize the DocIdMeta module
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);

    // Initialize spec dictionary (creates specDict_g)
    Indexes_Init(ctx);

    DocIdMeta_Init(ctx);

    // Create a mock key name for testing
    testKeyName = RedisModule_CreateString(ctx, "testkey", 7);

    // Create the key in the database with an actual value (so it exists for metadata operations)
    RedisModuleKey *key = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_WRITE);
    RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
    RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
    RedisModule_HashSet(key, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
    RedisModule_FreeString(ctx, fieldName);
    RedisModule_FreeString(ctx, fieldValue);
    RedisModule_CloseKey(key);

    // Create RDB IO context
    rdbIO = RMCK_CreateRdbIO();
  }

  void TearDown() override {
    // Clean up specs from specDict_g
    for (auto *rm : createdSpecs) {
      freeSpec(rm);
    }
    createdSpecs.clear();

    // Clean up KeyMeta storage
    RMCK_ClearKeyMetaStorage();

    if (rdbIO) {
      RMCK_FreeRdbIO(rdbIO);
      rdbIO = nullptr;
    }

    if (testKeyName) {
      RedisModule_FreeString(ctx, testKeyName);
    }
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = NULL;
    }

    RMCK_Shutdown();
  }

  // Helper: creates a spec in specDict_g with the given name and specId.
  void addTestSpec(const char *name, uint64_t specId) {
    RSIndexOptions opts = {0};
    opts.gcPolicy = GC_POLICY_FORK;
    auto ism = RediSearch_CreateIndex(name, &opts);
    ASSERT_NE(ism, nullptr);

    IndexSpec *spec = get_spec(ism);
    spec->specId = specId;

    SchemaRuleArgs args = {0};
    args.type = "HASH";
    const char *empty_prefix = "";
    args.prefixes = &empty_prefix;
    args.nprefixes = 1;
    QueryError status = QueryError_Default();
    spec->rule = SchemaRule_Create(&args, {ism}, &status);

    Spec_AddToDict(ism);
    createdSpecs.push_back(ism);
  }

  // Helper: create a HiddenString from a const char*
  static HiddenString *makeHiddenString(const char *name) {
    return NewHiddenString(name, strlen(name), false);
  }

  RedisModuleCtx *ctx;
  RedisModuleString *testKeyName;
  RedisModuleIO *rdbIO;
  std::vector<RefManager*> createdSpecs;

  // Helper constants for spec IDs and names
  static constexpr uint64_t SPEC1_ID = 1;
  static constexpr uint64_t SPEC2_ID = 2;
  static constexpr uint64_t SPEC3_ID = 3;

  // Spec names matching the IDs (used in addTestSpec and DocIdMeta_Set)
  static constexpr const char *SPEC1_NAME = "spec1";
  static constexpr const char *SPEC2_NAME = "spec2";
  static constexpr const char *SPEC3_NAME = "spec3";
};

TEST_F(DocIdMetaTest, TestSetAndGetDocId) {
  uint64_t docId = 12345;
  HiddenString *hs = makeHiddenString(SPEC1_NAME);

  int result = DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, docId, hs);
  EXPECT_EQ(result, REDISMODULE_OK);

  uint64_t retrievedDocId;
  result = DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrievedDocId);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(retrievedDocId, docId);

  HiddenString_Free(hs, false);
}

TEST_F(DocIdMetaTest, TestGetNonExistentDocId) {
  // Test getting a docId that doesn't exist
  uint64_t docId;
  int result = DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &docId);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestSetMultipleDocIds) {
  uint64_t docId1 = 111;
  uint64_t docId2 = 222;
  uint64_t docId3 = 333;
  HiddenString *hs1 = makeHiddenString(SPEC1_NAME);
  HiddenString *hs2 = makeHiddenString(SPEC2_NAME);
  HiddenString *hs3 = makeHiddenString(SPEC3_NAME);

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, docId1, hs1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, docId2, hs2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, docId3, hs3), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId1);

  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC2_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId2);

  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC3_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId3);

  // Test that unset specs return error
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, 999, &retrieved), REDISMODULE_ERR);

  HiddenString_Free(hs1, false);
  HiddenString_Free(hs2, false);
  HiddenString_Free(hs3, false);
}

TEST_F(DocIdMetaTest, TestOverwriteDocId) {
  uint64_t originalDocId = 111;
  uint64_t newDocId = 222;
  HiddenString *hs = makeHiddenString(SPEC1_NAME);

  // Set original value
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, originalDocId, hs), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, originalDocId);

  // Overwrite with new value
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, newDocId, hs), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, newDocId);

  HiddenString_Free(hs, false);
}

TEST_F(DocIdMetaTest, TestSoftDeleteDocId) {
  uint64_t docId = 555;
  HiddenString *hs = makeHiddenString(SPEC1_NAME);

  // Set a value first
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, docId, hs), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId);

  // Soft-delete the value (invalidates but keeps entry for reuse)
  int result = DocIdMeta_SoftDelete(ctx, testKeyName, SPEC1_ID);
  EXPECT_EQ(result, REDISMODULE_OK);

  // Should now return error when trying to get
  result = DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved);
  EXPECT_EQ(result, REDISMODULE_ERR);

  HiddenString_Free(hs, false);
}

TEST_F(DocIdMetaTest, TestSoftDeleteNonExistentDocId) {
  // Try to soft-delete a docId that doesn't exist
  int result = DocIdMeta_SoftDelete(ctx, testKeyName, 999);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestMultipleKeys) {
  // Test that different keys maintain separate docId maps
  RedisModuleString *keyName1 = RedisModule_CreateString(ctx, "testkey1", 8);
  RedisModuleString *keyName2 = RedisModule_CreateString(ctx, "testkey2", 8);
  RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
  RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
  HiddenString *hs = makeHiddenString(SPEC1_NAME);

  // Create the keys in the database with actual values
  RedisModuleKey *key1 = RedisModule_OpenKey(ctx, keyName1, REDISMODULE_WRITE);
  RedisModule_HashSet(key1, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
  RedisModule_CloseKey(key1);

  RedisModuleKey *key2 = RedisModule_OpenKey(ctx, keyName2, REDISMODULE_WRITE);
  RedisModule_HashSet(key2, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
  RedisModule_CloseKey(key2);

  RedisModule_FreeString(ctx, fieldName);
  RedisModule_FreeString(ctx, fieldValue);

  uint64_t docId1 = 111;
  uint64_t docId2 = 222;

  // Set different values for the same spec on different keys
  EXPECT_EQ(DocIdMeta_Set(ctx, keyName1, SPEC1_ID, docId1, hs), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, keyName2, SPEC1_ID, docId2, hs), REDISMODULE_OK);

  // Verify they're independent
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, keyName1, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId1);

  EXPECT_EQ(DocIdMeta_Get(ctx, keyName2, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId2);

  HiddenString_Free(hs, false);
  RedisModule_FreeString(ctx, keyName1);
  RedisModule_FreeString(ctx, keyName2);
}

TEST_F(DocIdMetaTest, TestEdgeCases) {
  // Test with docId = 1 (minimum valid docId since 0 is DOCID_META_INVALID)
  uint64_t minValidDocId = 1;
  HiddenString *hs1 = makeHiddenString(SPEC1_NAME);
  HiddenString *hs2 = makeHiddenString(SPEC2_NAME);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, minValidDocId, hs1), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, minValidDocId);

  // Test with maximum uint64_t value
  uint64_t maxDocId = UINT64_MAX;
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, maxDocId, hs2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC2_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, maxDocId);

  HiddenString_Free(hs1, false);
  HiddenString_Free(hs2, false);
}

TEST_F(DocIdMetaTest, TestSoftDeleteAndReget) {
  // Test that getting from an unset spec returns ERR
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);

  // Set a valid docId and then soft-delete it to test soft-deletion behavior
  uint64_t validDocId = 42;
  HiddenString *hs = makeHiddenString(SPEC1_NAME);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, validDocId, hs), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, validDocId);

  // Soft-delete it and verify it's gone (should return ERR like uninitialized)
  EXPECT_EQ(DocIdMeta_SoftDelete(ctx, testKeyName, SPEC1_ID), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);

  HiddenString_Free(hs, false);
}

// Simple test to check if basic setup works
TEST_F(DocIdMetaTest, TestBasicSetup) {
  // Just verify that the test setup doesn't crash
  EXPECT_NE(ctx, nullptr);
  EXPECT_NE(testKeyName, nullptr);
}


TEST_F(DocIdMetaTest, TestBasicRdbSaveLoad) {
  // Create specs in specDict_g so RDB save/load doesn't filter them out
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec2", SPEC2_ID);
  addTestSpec("spec3", SPEC3_ID);

  // Set up some docId metadata
  uint64_t docId1 = 12345;
  uint64_t docId2 = 67890;
  uint64_t docId3 = 11111;
  HiddenString *hs1 = makeHiddenString(SPEC1_NAME);
  HiddenString *hs2 = makeHiddenString(SPEC2_NAME);
  HiddenString *hs3 = makeHiddenString(SPEC3_NAME);

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, docId1, hs1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, docId2, hs2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, docId3, hs3), REDISMODULE_OK);

  HiddenString_Free(hs1, false);
  HiddenString_Free(hs2, false);
  HiddenString_Free(hs3, false);

  // Get the metadata for RDB save
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  // Call the RDB save function through test wrapper
  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Reset read position
  rdbIO->read_pos = 0;

  // Call the RDB load function through test wrapper
  uint64_t loadedMeta = 0;
  int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);
  EXPECT_NE(loadedMeta, 0);

  // Create a new key with actual value and set the loaded metadata
  RedisModuleString *newKeyName = RedisModule_CreateString(ctx, "newkey", 6);
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, newKeyName, REDISMODULE_WRITE);
  RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
  RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
  RedisModule_HashSet(newKey, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
  RedisModule_FreeString(ctx, fieldName);
  RedisModule_FreeString(ctx, fieldValue);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);
  RedisModule_CloseKey(newKey);

  // Verify loaded data
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId1);

  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC2_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId2);

  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC3_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId3);

  // Verify nonexistent specs return error
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, 999, &retrieved), REDISMODULE_ERR);

  RedisModule_FreeString(ctx, newKeyName);
}

TEST_F(DocIdMetaTest, TestEmptyMetaRdbSaveLoad) {
  // Test saving/loading when there's no metadata
  uint64_t meta = 0;

  // Call RDB save with empty meta (should return early without writing anything)
  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Since nothing was saved, we can't test loading empty meta directly
  // Instead, test that we can handle the case where no data is available
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestMultipleSpecsRdbSaveLoad) {
  // Create specs in specDict_g
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec2", SPEC2_ID);
  addTestSpec("spec3", SPEC3_ID);
  addTestSpec("spec4", 4);

  // Test with multiple specs: (specName, specId, docId)
  struct SpecEntry { const char *specName; uint64_t specId; uint64_t docId; };
  std::vector<SpecEntry> specs = {
    {SPEC1_NAME, SPEC1_ID, 1001},
    {SPEC2_NAME, SPEC2_ID, 2002},
    {SPEC3_NAME, SPEC3_ID, 3003},
    {"spec4", 4, 4004},
  };

  // Set all the docIds
  for (const auto& spec : specs) {
    HiddenString *hs = makeHiddenString(spec.specName);
    EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, spec.specId, spec.docId, hs), REDISMODULE_OK);
    HiddenString_Free(hs, false);
  }

  // Get the metadata for RDB save
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  // Save to RDB
  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Reset read position
  rdbIO->read_pos = 0;

  // Load from RDB
  uint64_t loadedMeta = 0;
  int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);
  EXPECT_NE(loadedMeta, 0);

  // Create a new key with actual value and set the loaded metadata
  RedisModuleString *newKeyName = RedisModule_CreateString(ctx, "largekey", 8);
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, newKeyName, REDISMODULE_WRITE);
  RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
  RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
  RedisModule_HashSet(newKey, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
  RedisModule_FreeString(ctx, fieldName);
  RedisModule_FreeString(ctx, fieldValue);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);
  RedisModule_CloseKey(newKey);

  // Verify all loaded data
  for (const auto& spec : specs) {
    uint64_t retrieved;
    EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, spec.specId, &retrieved), REDISMODULE_OK);
    EXPECT_EQ(retrieved, spec.docId);
  }

  // Verify nonexistent specs return error
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, 999, &retrieved), REDISMODULE_ERR);

  RedisModule_FreeString(ctx, newKeyName);
}

TEST_F(DocIdMetaTest, TestMaxValueRdbSaveLoad) {
  // Create specs in specDict_g
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec2", SPEC2_ID);

  // Test with maximum uint64_t values
  uint64_t maxDocId = UINT64_MAX;
  uint64_t minValidDocId = 1;
  HiddenString *hs1 = makeHiddenString(SPEC1_NAME);
  HiddenString *hs2 = makeHiddenString(SPEC2_NAME);

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, maxDocId, hs1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, minValidDocId, hs2), REDISMODULE_OK);

  HiddenString_Free(hs1, false);
  HiddenString_Free(hs2, false);

  // Get the metadata for RDB save
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  // Save to RDB
  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Reset read position
  rdbIO->read_pos = 0;

  // Load from RDB
  uint64_t loadedMeta = 0;
  int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);
  EXPECT_NE(loadedMeta, 0);

  // Create a new key with actual value and set the loaded metadata
  RedisModuleString *newKeyName = RedisModule_CreateString(ctx, "maxkey", 6);
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, newKeyName, REDISMODULE_WRITE);
  RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
  RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
  RedisModule_HashSet(newKey, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
  RedisModule_FreeString(ctx, fieldName);
  RedisModule_FreeString(ctx, fieldValue);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);
  RedisModule_CloseKey(newKey);

  // Verify loaded data
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, maxDocId);

  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC2_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, minValidDocId);

  RedisModule_FreeString(ctx, newKeyName);
}

TEST_F(DocIdMetaTest, TestSingleElementRdbSaveLoad) {
  // Create spec in specDict_g
  addTestSpec("spec1", SPEC1_ID);

  // Test with just one spec
  uint64_t singleDocId = 99999;
  HiddenString *hs = makeHiddenString(SPEC1_NAME);

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, singleDocId, hs), REDISMODULE_OK);
  HiddenString_Free(hs, false);

  // Get the metadata for RDB save
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  // Save to RDB
  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Reset read position
  rdbIO->read_pos = 0;

  // Load from RDB
  uint64_t loadedMeta = 0;
  int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);
  EXPECT_NE(loadedMeta, 0);

  // Create a new key with actual value and set the loaded metadata
  RedisModuleString *newKeyName = RedisModule_CreateString(ctx, "singlekey", 9);
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, newKeyName, REDISMODULE_WRITE);
  RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
  RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
  RedisModule_HashSet(newKey, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
  RedisModule_FreeString(ctx, fieldName);
  RedisModule_FreeString(ctx, fieldValue);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);
  RedisModule_CloseKey(newKey);

  // Verify loaded data
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, singleDocId);

  // Verify other specs are not set
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC2_ID, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC3_ID, &retrieved), REDISMODULE_ERR);

  RedisModule_FreeString(ctx, newKeyName);
}


///////////////////////////////////////////////////////////////////////////////////////////////
// RDB save/load filtering tests (specs not in specDict_g are skipped)
///////////////////////////////////////////////////////////////////////////////////////////////

TEST_F(DocIdMetaTest, TestRdbLoadSkipsRemovedSpecEntries) {
  // Create all 3 specs in specDict_g
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec2", SPEC2_ID);
  addTestSpec("spec3", SPEC3_ID);
  HiddenString *hs1 = makeHiddenString(SPEC1_NAME);
  HiddenString *hs2 = makeHiddenString(SPEC2_NAME);
  HiddenString *hs3 = makeHiddenString(SPEC3_NAME);

  // Set up docId metadata for 3 specs on a key
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001, hs1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002, hs2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, 3003, hs3), REDISMODULE_OK);

  HiddenString_Free(hs1, false);
  HiddenString_Free(hs2, false);
  HiddenString_Free(hs3, false);

  // Get the metadata and save to RDB (all 3 specs are still live)
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Now remove SPEC2 from specDict_g (simulates index drop)
  freeSpec(createdSpecs[1]);
  createdSpecs.erase(createdSpecs.begin() + 1);

  // Load from RDB — SPEC2_ID entry should be skipped since it's not in specDict_g
  rdbIO->read_pos = 0;
  uint64_t loadedMeta = 0;
  int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_NE(loadedMeta, 0);

  // Create a new key and attach the loaded metadata
  RedisModuleString *newKeyName = RedisModule_CreateString(ctx, "stalekey", 8);
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, newKeyName, REDISMODULE_WRITE);
  RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
  RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
  RedisModule_HashSet(newKey, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
  RedisModule_FreeString(ctx, fieldName);
  RedisModule_FreeString(ctx, fieldValue);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);
  RedisModule_CloseKey(newKey);

  // SPEC1 and SPEC3 should be present, SPEC2 should be gone
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, 1001);

  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC2_ID, &retrieved), REDISMODULE_ERR);

  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC3_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, 3003);

  RedisModule_FreeString(ctx, newKeyName);
}

TEST_F(DocIdMetaTest, TestRdbSaveSkipsRemovedSpecEntries) {
  // Create only SPEC1 and SPEC3 in specDict_g (SPEC2 is "dropped")
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec3", SPEC3_ID);
  HiddenString *hs1 = makeHiddenString(SPEC1_NAME);
  HiddenString *hs2 = makeHiddenString(SPEC2_NAME);
  HiddenString *hs3 = makeHiddenString(SPEC3_NAME);

  // Set up docId metadata for 3 specs on a key
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001, hs1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002, hs2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, 3003, hs3), REDISMODULE_OK);

  HiddenString_Free(hs1, false);
  HiddenString_Free(hs2, false);
  HiddenString_Free(hs3, false);

  // Get the metadata and save to RDB — should skip SPEC2_ID (not in specDict_g)
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Load from RDB — should only have SPEC1 and SPEC3
  rdbIO->read_pos = 0;
  uint64_t loadedMeta = 0;
  int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_NE(loadedMeta, 0);

  // Create a new key and attach the loaded metadata
  RedisModuleString *newKeyName = RedisModule_CreateString(ctx, "savekey", 7);
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, newKeyName, REDISMODULE_WRITE);
  RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
  RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
  RedisModule_HashSet(newKey, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
  RedisModule_FreeString(ctx, fieldName);
  RedisModule_FreeString(ctx, fieldValue);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);
  RedisModule_CloseKey(newKey);

  // SPEC1 and SPEC3 should be present, SPEC2 should be gone
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, 1001);

  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC2_ID, &retrieved), REDISMODULE_ERR);

  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC3_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, 3003);

  RedisModule_FreeString(ctx, newKeyName);
}

TEST_F(DocIdMetaTest, TestRdbSaveAllRemoved_SavesNothing) {
  // No specs in specDict_g — all entries are considered stale
  HiddenString *hs = makeHiddenString(SPEC1_NAME);

  // Set up docId metadata for a single spec
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001, hs), REDISMODULE_OK);
  HiddenString_Free(hs, false);

  // Get the metadata and save to RDB — all entries are stale, should save nothing
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // The RDB buffer should have nothing to load (save returned early after
  // finding 0 valid entries). Since nothing was written, we can't call
  // docIdMetaRDBLoad — just verify no crash.
}

TEST_F(DocIdMetaTest, TestRdbSaveSkipsSoftDeletedEntries) {
  // Create all 3 specs in specDict_g
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec2", SPEC2_ID);
  addTestSpec("spec3", SPEC3_ID);
  HiddenString *hs1 = makeHiddenString(SPEC1_NAME);
  HiddenString *hs2 = makeHiddenString(SPEC2_NAME);
  HiddenString *hs3 = makeHiddenString(SPEC3_NAME);

  // Set up docId metadata for 3 specs
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001, hs1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002, hs2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, 3003, hs3), REDISMODULE_OK);

  HiddenString_Free(hs1, false);
  HiddenString_Free(hs2, false);
  HiddenString_Free(hs3, false);

  // Soft-delete SPEC2's entry (invalidates docId but keeps entry in hashmap)
  EXPECT_EQ(DocIdMeta_SoftDelete(ctx, testKeyName, SPEC2_ID), REDISMODULE_OK);

  // Verify SPEC2 is no longer retrievable
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC2_ID, &retrieved), REDISMODULE_ERR);

  // Get the metadata and save to RDB — should skip SPEC2 (soft-deleted)
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Load from RDB — should only have SPEC1 and SPEC3
  rdbIO->read_pos = 0;
  uint64_t loadedMeta = 0;
  int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_NE(loadedMeta, 0);

  // Create a new key and attach the loaded metadata
  RedisModuleString *newKeyName = RedisModule_CreateString(ctx, "softdelkey", 10);
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, newKeyName, REDISMODULE_WRITE);
  RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
  RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
  RedisModule_HashSet(newKey, REDISMODULE_HASH_NONE, fieldName, fieldValue, NULL);
  RedisModule_FreeString(ctx, fieldName);
  RedisModule_FreeString(ctx, fieldValue);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);
  RedisModule_CloseKey(newKey);

  // SPEC1 and SPEC3 should be present, SPEC2 should be gone (was soft-deleted)
  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, 1001);

  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC2_ID, &retrieved), REDISMODULE_ERR);

  EXPECT_EQ(DocIdMeta_Get(ctx, newKeyName, SPEC3_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, 3003);

  RedisModule_FreeString(ctx, newKeyName);
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Unlink callback tests
//
// Note: These tests don't add specs to specDict_g, so findSpecByNameAndId returns NULL
// and IndexSpec_DeleteDocById is not called. This tests the entry invalidation logic
// without requiring disk-based index support (which would trigger isSpecOnDisk assertions).
// The full unlink flow with IndexSpec_DeleteDocById is tested in integration/flow tests.
///////////////////////////////////////////////////////////////////////////////////////////////

TEST_F(DocIdMetaTest, TestUnlinkWithEmptyMeta) {
  // Test that unlink handles empty/zero meta gracefully
  uint64_t meta = 0;
  docIdMetaUnlink(nullptr, &meta);
  // Should not crash, just return early
  EXPECT_EQ(meta, 0);
}

TEST_F(DocIdMetaTest, TestUnlinkInvalidatesEntries) {
  // Don't add specs to specDict_g - this tests entry invalidation without triggering
  // IndexSpec_DeleteDocById (which requires disk-based indexes)
  HiddenString *hs1 = makeHiddenString(SPEC1_NAME);
  HiddenString *hs2 = makeHiddenString(SPEC2_NAME);

  // Set up docId metadata for 2 specs
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001, hs1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002, hs2), REDISMODULE_OK);

  HiddenString_Free(hs1, false);
  HiddenString_Free(hs2, false);

  // Verify entries exist
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, 1001);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC2_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, 2002);

  // Get the metadata pointer
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  // Call unlink - specs don't exist in specDict_g, so IndexSpec_DeleteDocById is skipped
  // but entries should still be invalidated
  docIdMetaUnlink(nullptr, &meta);

  // Verify entries are now invalid (Get should return ERR)
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC2_ID, &retrieved), REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestUnlinkSkipsSoftDeletedEntries) {
  // Don't add specs to specDict_g
  HiddenString *hs1 = makeHiddenString(SPEC1_NAME);
  HiddenString *hs2 = makeHiddenString(SPEC2_NAME);

  // Set up docId metadata for 2 specs
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001, hs1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002, hs2), REDISMODULE_OK);

  HiddenString_Free(hs1, false);
  HiddenString_Free(hs2, false);

  // Soft-delete SPEC1's entry
  EXPECT_EQ(DocIdMeta_SoftDelete(ctx, testKeyName, SPEC1_ID), REDISMODULE_OK);

  // Verify SPEC1 is already invalid, SPEC2 still valid
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC2_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, 2002);

  // Get the metadata pointer
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  // Call unlink - should skip SPEC1 (already soft-deleted) and process SPEC2
  // The test verifies unlink doesn't crash when encountering soft-deleted entries
  docIdMetaUnlink(nullptr, &meta);

  // Both entries should now be invalid
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC2_ID, &retrieved), REDISMODULE_ERR);
}
