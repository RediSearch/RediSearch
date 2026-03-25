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


class DocIdMetaTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Initialize redismock and clear any previous state
    RMCK::init();

    // Initialize the DocIdMeta module
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);
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
    // Clean up dropped specIds tracking state
    DocIdMeta_ClearDroppedSpecIds();

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
  }

  RedisModuleCtx *ctx;
  RedisModuleString *testKeyName;
  RedisModuleIO *rdbIO;

  // Helper constants for spec IDs
  static constexpr uint64_t SPEC1_ID = 1;
  static constexpr uint64_t SPEC2_ID = 2;
  static constexpr uint64_t SPEC3_ID = 3;
};

TEST_F(DocIdMetaTest, TestSetAndGetDocId) {
  uint64_t docId = 12345;

  int result = DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, docId);
  EXPECT_EQ(result, REDISMODULE_OK);

  uint64_t retrievedDocId;
  result = DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrievedDocId);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(retrievedDocId, docId);
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

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, docId1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, docId2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, docId3), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId1);

  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC2_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId2);

  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC3_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId3);

  // Test that unset specs return error
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, 999, &retrieved), REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestOverwriteDocId) {
  uint64_t originalDocId = 111;
  uint64_t newDocId = 222;

  // Set original value
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, originalDocId), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, originalDocId);

  // Overwrite with new value
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, newDocId), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, newDocId);
}

TEST_F(DocIdMetaTest, TestDeleteDocId) {
  uint64_t docId = 555;

  // Set a value first
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, docId), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId);

  // Delete the value
  int result = DocIdMeta_Delete(ctx, testKeyName, SPEC1_ID);
  EXPECT_EQ(result, REDISMODULE_OK);

  // Should now return error when trying to get
  result = DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestDeleteNonExistentDocId) {
  // Try to delete a docId that doesn't exist
  int result = DocIdMeta_Delete(ctx, testKeyName, 999);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestMultipleKeys) {
  // Test that different keys maintain separate docId maps
  RedisModuleString *keyName1 = RedisModule_CreateString(ctx, "testkey1", 8);
  RedisModuleString *keyName2 = RedisModule_CreateString(ctx, "testkey2", 8);
  RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
  RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);

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
  EXPECT_EQ(DocIdMeta_Set(ctx, keyName1, SPEC1_ID, docId1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, keyName2, SPEC1_ID, docId2), REDISMODULE_OK);

  // Verify they're independent
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, keyName1, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId1);

  EXPECT_EQ(DocIdMeta_Get(ctx, keyName2, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId2);

  RedisModule_FreeString(ctx, keyName1);
  RedisModule_FreeString(ctx, keyName2);
}

TEST_F(DocIdMetaTest, TestEdgeCases) {
  // Test with docId = 1 (minimum valid docId since 0 is DOCID_META_INVALID)
  uint64_t minValidDocId = 1;
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, minValidDocId), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, minValidDocId);

  // Test with maximum uint64_t value
  uint64_t maxDocId = UINT64_MAX;
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, maxDocId), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC2_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, maxDocId);
}

TEST_F(DocIdMetaTest, TestDeleteAndReget) {
  // Test that getting from an unset spec returns ERR
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);

  // Set a valid docId and then delete it to test deletion behavior
  uint64_t validDocId = 42;
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, validDocId), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, validDocId);

  // Delete it and verify it's gone (should return ERR like uninitialized)
  EXPECT_EQ(DocIdMeta_Delete(ctx, testKeyName, SPEC1_ID), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);
}

// Simple test to check if basic setup works
TEST_F(DocIdMetaTest, TestBasicSetup) {
  // Just verify that the test setup doesn't crash
  EXPECT_NE(ctx, nullptr);
  EXPECT_NE(testKeyName, nullptr);
}


TEST_F(DocIdMetaTest, TestBasicRdbSaveLoad) {
  // Set up some docId metadata
  uint64_t docId1 = 12345;
  uint64_t docId2 = 67890;
  uint64_t docId3 = 11111;

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, docId1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, docId2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, docId3), REDISMODULE_OK);

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
  // Test with multiple specs: (specId, docId)
  struct SpecEntry { uint64_t specId; uint64_t docId; };
  std::vector<SpecEntry> specs = {
    {SPEC1_ID, 1001},
    {SPEC2_ID, 2002},
    {SPEC3_ID, 3003},
    {4, 4004},
  };

  // Set all the docIds
  for (const auto& spec : specs) {
    EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, spec.specId, spec.docId), REDISMODULE_OK);
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
  // Test with maximum uint64_t values
  uint64_t maxDocId = UINT64_MAX;
  uint64_t minValidDocId = 1;

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, maxDocId), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, minValidDocId), REDISMODULE_OK);

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
  // Test with just one spec
  uint64_t singleDocId = 99999;

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, singleDocId), REDISMODULE_OK);

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
// DroppedSpecEntry tracking tests
///////////////////////////////////////////////////////////////////////////////////////////////

TEST_F(DocIdMetaTest, TestTrackDroppedSpecId_Basic) {
  // Track a dropped specId with refcount 3
  DocIdMeta_TrackDroppedSpecId(100, 3);

  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(100));
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(200));
}

TEST_F(DocIdMetaTest, TestTrackDroppedSpecId_ZeroRefcount) {
  // Tracking with refcount 0 should be a no-op
  DocIdMeta_TrackDroppedSpecId(100, 0);

  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(100));
}

TEST_F(DocIdMetaTest, TestDecrDroppedRefcount_RemovesAtZero) {
  DocIdMeta_TrackDroppedSpecId(100, 3);
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(100));

  DocIdMeta_DecrDroppedRefcount(100);
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(100));

  DocIdMeta_DecrDroppedRefcount(100);
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(100));

  // Third decrement should bring refcount to 0 and remove the entry
  DocIdMeta_DecrDroppedRefcount(100);
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(100));
}

TEST_F(DocIdMetaTest, TestDecrDroppedRefcount_NonExistent) {
  // Decrementing a non-existent specId should be a no-op (no crash)
  DocIdMeta_DecrDroppedRefcount(999);
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(999));
}

TEST_F(DocIdMetaTest, TestMultipleDroppedSpecs) {
  DocIdMeta_TrackDroppedSpecId(100, 2);
  DocIdMeta_TrackDroppedSpecId(200, 1);
  DocIdMeta_TrackDroppedSpecId(300, 5);

  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(100));
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(200));
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(300));
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(400));

  // Remove 200 (refcount was 1)
  DocIdMeta_DecrDroppedRefcount(200);
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(200));
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(100));
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(300));
}

TEST_F(DocIdMetaTest, TestClearDroppedSpecIds) {
  DocIdMeta_TrackDroppedSpecId(100, 5);
  DocIdMeta_TrackDroppedSpecId(200, 3);
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(100));
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(200));

  DocIdMeta_ClearDroppedSpecIds();

  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(100));
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(200));
}

TEST_F(DocIdMetaTest, TestDroppedSpecIdsRdbSaveLoad) {
  // Track some dropped specIds
  DocIdMeta_TrackDroppedSpecId(100, 5);
  DocIdMeta_TrackDroppedSpecId(200, 3);
  DocIdMeta_TrackDroppedSpecId(300, 1);

  // Save to RDB
  DocIdMeta_DroppedSpecIdsRdbSave(rdbIO);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Clear and verify they're gone
  DocIdMeta_ClearDroppedSpecIds();
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(100));
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(200));
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(300));

  // Load from RDB
  rdbIO->read_pos = 0;
  int result = DocIdMeta_DroppedSpecIdsRdbLoad(rdbIO);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Verify they're restored
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(100));
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(200));
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(300));
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(400));
}

TEST_F(DocIdMetaTest, TestDroppedSpecIdsRdbSaveLoad_Empty) {
  // Save with no dropped specs
  DocIdMeta_DroppedSpecIdsRdbSave(rdbIO);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Load from RDB
  rdbIO->read_pos = 0;
  int result = DocIdMeta_DroppedSpecIdsRdbLoad(rdbIO);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(100));
}

TEST_F(DocIdMetaTest, TestRdbLoadSkipsStaleEntries) {
  // Set up docId metadata for 3 specs on a key
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, 3003), REDISMODULE_OK);

  // Get the metadata and save to RDB
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Now mark SPEC2_ID as dropped with refcount 1
  DocIdMeta_TrackDroppedSpecId(SPEC2_ID, 1);
  EXPECT_TRUE(DocIdMeta_IsDroppedSpecId(SPEC2_ID));

  // Load from RDB — SPEC2_ID entry should be skipped
  rdbIO->read_pos = 0;
  uint64_t loadedMeta = 0;
  int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_NE(loadedMeta, 0);

  // SPEC2_ID should have been cleaned (refcount decremented to 0, entry removed)
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(SPEC2_ID));

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

TEST_F(DocIdMetaTest, TestRdbSaveSkipsStaleEntries) {
  // Set up docId metadata for 3 specs on a key
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, 3003), REDISMODULE_OK);

  // Mark SPEC2_ID as dropped with refcount 1
  DocIdMeta_TrackDroppedSpecId(SPEC2_ID, 1);

  // Get the metadata and save to RDB — should skip SPEC2_ID
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // SPEC2_ID refcount should have been decremented to 0 and removed
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(SPEC2_ID));

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

TEST_F(DocIdMetaTest, TestRdbSaveAllStale_SavesNothing) {
  // Set up docId metadata for a single spec
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001), REDISMODULE_OK);

  // Mark it as dropped
  DocIdMeta_TrackDroppedSpecId(SPEC1_ID, 1);

  // Get the metadata and save to RDB — all entries are stale, should save nothing
  RedisModuleKey *testKey = RedisModule_OpenKey(ctx, testKeyName, REDISMODULE_READ);
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);
  RedisModule_CloseKey(testKey);

  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  // Refcount should have been decremented to 0
  EXPECT_FALSE(DocIdMeta_IsDroppedSpecId(SPEC1_ID));

  // The RDB buffer should have nothing to load (save returned early after
  // finding 0 valid entries). Attempting to load should fail or produce empty.
  // Since nothing was written, we can't call docIdMetaRDBLoad.
  // Just verify the refcount cleanup happened.
}
