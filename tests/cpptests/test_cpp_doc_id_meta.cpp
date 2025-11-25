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

    // Create a mock key for testing
    testKey = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "testkey", 7), REDISMODULE_WRITE);

    // Create RDB IO context
    rdbIO = RMCK_CreateRdbIO();
  }

  void TearDown() override {
    // Clean up KeyMeta storage
    RMCK_ClearKeyMetaStorage();

    if (rdbIO) {
      RMCK_FreeRdbIO(rdbIO);
      rdbIO = nullptr;
    }

    if (testKey) {
      RedisModule_CloseKey(testKey);
    }
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = NULL;
    }
  }

  RedisModuleCtx *ctx;
  RedisModuleKey *testKey;
  RedisModuleIO *rdbIO;
};

TEST_F(DocIdMetaTest, TestSetAndGetDocId) {
  uint64_t docId = 12345;
  size_t idx = 0;

  int result = DocIdMeta_SetDocIdForIndex(testKey, idx, docId);
  EXPECT_EQ(result, REDISMODULE_OK);

  uint64_t retrievedDocId;
  result = DocIdMeta_GetDocIdForIndex(testKey, idx, &retrievedDocId);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(retrievedDocId, docId);
}

TEST_F(DocIdMetaTest, TestGetNonExistentDocId) {
  // Test getting a docId that doesn't exist
  uint64_t docId;
  int result = DocIdMeta_GetDocIdForIndex(testKey, 0, &docId);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestSetMultipleDocIds) {
  uint64_t docId1 = 111;
  uint64_t docId2 = 222;
  uint64_t docId3 = 333;

  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 0, docId1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 1, docId2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 5, docId3), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 0, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId1);

  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 1, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId2);

  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 5, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId3);

  // Test that unset indices return error
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 2, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 3, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 4, &retrieved), REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestArrayResize) {
  // Test that the array resizes correctly when we exceed initial capacity
  // INITIAL_DOCID_META_SIZE is 10, so setting index 15 should trigger resize
  uint64_t docId = 999;
  size_t largeIdx = 15;

  int result = DocIdMeta_SetDocIdForIndex(testKey, largeIdx, docId);
  EXPECT_EQ(result, REDISMODULE_OK);

  uint64_t retrieved;
  result = DocIdMeta_GetDocIdForIndex(testKey, largeIdx, &retrieved);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId);

  // Test that smaller indices still work (should be initialized to DOCID_META_INVALID)
  result = DocIdMeta_GetDocIdForIndex(testKey, 5, &retrieved);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestOverwriteDocId) {
  uint64_t originalDocId = 111;
  uint64_t newDocId = 222;
  size_t idx = 3;

  // Set original value
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, idx, originalDocId), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, idx, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, originalDocId);

  // Overwrite with new value
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, idx, newDocId), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, idx, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, newDocId);
}

TEST_F(DocIdMetaTest, TestDeleteDocId) {
  uint64_t docId = 555;
  size_t idx = 2;

  // Set a value first
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, idx, docId), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, idx, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId);

  // Delete the value
  int result = DocIdMeta_DeleteDocIdForIndex(testKey, idx);
  EXPECT_EQ(result, REDISMODULE_OK);

  // Should now return error when trying to get
  result = DocIdMeta_GetDocIdForIndex(testKey, idx, &retrieved);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestDeleteNonExistentDocId) {
  // Try to delete a docId that doesn't exist
  int result = DocIdMeta_DeleteDocIdForIndex(testKey, 10);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestDeleteOutOfBounds) {
  // Set a small array first
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 2, 123), REDISMODULE_OK);

  // Try to delete way beyond the array bounds
  int result = DocIdMeta_DeleteDocIdForIndex(testKey, 100);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestMultipleKeys) {
  // Test that different keys maintain separate docId arrays
  RedisModuleKey *key1 = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "testkey1", 8), REDISMODULE_WRITE);
  RedisModuleKey *key2 = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "testkey2", 8), REDISMODULE_WRITE);

  uint64_t docId1 = 111;
  uint64_t docId2 = 222;

  // Set different values for the same index on different keys
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(key1, 0, docId1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(key2, 0, docId2), REDISMODULE_OK);

  // Verify they're independent
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(key1, 0, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId1);

  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(key2, 0, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId2);

  RedisModule_CloseKey(key1);
  RedisModule_CloseKey(key2);
}

TEST_F(DocIdMetaTest, TestEdgeCases) {
  // Test with docId = 1 (minimum valid docId since 0 is DOCID_META_INVALID)
  uint64_t minValidDocId = 1;
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 0, minValidDocId), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 0, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, minValidDocId);

  // Test with maximum uint64_t value
  uint64_t maxDocId = UINT64_MAX;
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 1, maxDocId), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 1, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, maxDocId);
}

TEST_F(DocIdMetaTest, TestLargeIndex) {
  // Test with a very large index to ensure proper array growth
  size_t largeIdx = 1000;
  uint64_t docId = 12345;

  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, largeIdx, docId), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, largeIdx, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId);

  // Test exactly at initial size boundary (INITIAL_DOCID_META_SIZE = 10)
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 9, docId + 1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 9, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId + 1);

  // Test just beyond initial size (should trigger resize)
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 10, docId + 2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 10, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId + 2);
}

TEST_F(DocIdMetaTest, TestZeroDocId) {
  // Test that docId = 0 cannot be set (since it's DOCID_META_INVALID internally)
  // This should trigger an assertion failure in debug builds, but we can't easily test that
  // Instead, test that getting from an uninitialized slot returns ERR
  size_t idx = 5;

  uint64_t retrieved;
  // Getting from an uninitialized slot should return ERR
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, idx, &retrieved), REDISMODULE_ERR);

  // Set a valid docId and then delete it to test deletion behavior
  uint64_t validDocId = 42;
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, idx, validDocId), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, idx, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, validDocId);

  // Delete it and verify it's gone (should return ERR like uninitialized)
  EXPECT_EQ(DocIdMeta_DeleteDocIdForIndex(testKey, idx), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, idx, &retrieved), REDISMODULE_ERR);
}

// Simple test to check if basic setup works
TEST_F(DocIdMetaTest, TestBasicSetup) {
  // Just verify that the test setup doesn't crash
  EXPECT_NE(ctx, nullptr);
  EXPECT_NE(testKey, nullptr);
}


TEST_F(DocIdMetaTest, TestBasicRdbSaveLoad) {
  // Set up some docId metadata
  uint64_t docId1 = 12345;
  uint64_t docId2 = 67890;
  uint64_t docId3 = 11111;

  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 0, docId1), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 1, docId2), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 5, docId3), REDISMODULE_OK);

  // Get the metadata for RDB save
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);

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

  // Create a new key and set the loaded metadata
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "newkey", 6), REDISMODULE_WRITE);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);

  // Verify loaded data
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 0, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId1);

  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 1, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId2);

  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 5, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId3);

  // Verify empty slots return error
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 2, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 3, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 4, &retrieved), REDISMODULE_ERR);

  RedisModule_CloseKey(newKey);
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
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(testKey, 0, &retrieved), REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestLargeArrayRdbSaveLoad) {
  // Test with a large array that requires resizing
  std::vector<uint64_t> docIds;
  std::vector<size_t> indices;

  // Create a sparse array with some large indices
  docIds.push_back(1001);
  indices.push_back(0);

  docIds.push_back(2002);
  indices.push_back(15);  // Triggers resize from initial size 10

  docIds.push_back(3003);
  indices.push_back(50);  // Further resize

  docIds.push_back(4004);
  indices.push_back(100); // Even larger resize

  // Set all the docIds
  for (size_t i = 0; i < docIds.size(); i++) {
    EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, indices[i], docIds[i]), REDISMODULE_OK);
  }

  // Get the metadata for RDB save
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);

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

  // Create a new key and set the loaded metadata
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "largekey", 8), REDISMODULE_WRITE);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);

  // Verify all loaded data
  for (size_t i = 0; i < docIds.size(); i++) {
    uint64_t retrieved;
    EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, indices[i], &retrieved), REDISMODULE_OK);
    EXPECT_EQ(retrieved, docIds[i]);
  }

  // Verify empty slots return error
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 1, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 25, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 75, &retrieved), REDISMODULE_ERR);

  RedisModule_CloseKey(newKey);
}

TEST_F(DocIdMetaTest, TestMaxValueRdbSaveLoad) {
  // Test with maximum uint64_t values
  uint64_t maxDocId = UINT64_MAX;
  uint64_t minValidDocId = 1;

  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 0, maxDocId), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 1, minValidDocId), REDISMODULE_OK);

  // Get the metadata for RDB save
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);

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

  // Create a new key and set the loaded metadata
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "maxkey", 6), REDISMODULE_WRITE);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);

  // Verify loaded data
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 0, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, maxDocId);

  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 1, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, minValidDocId);

  RedisModule_CloseKey(newKey);
}

TEST_F(DocIdMetaTest, TestMultipleRoundTripRdbSaveLoad) {
  // Test multiple save/load cycles to ensure data integrity
  uint64_t originalDocId = 55555;

  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 3, originalDocId), REDISMODULE_OK);

  // First save/load cycle
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);

  docIdMetaRDBSave(rdbIO, nullptr, &meta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  rdbIO->read_pos = 0;

  uint64_t loadedMeta = 0;
  int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);
  EXPECT_NE(loadedMeta, 0);

  RedisModuleKey *firstKey = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "first", 5), REDISMODULE_WRITE);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), firstKey, loadedMeta), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(firstKey, 3, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, originalDocId);

  // Modify the loaded data and save again
  uint64_t newDocId = 77777;
  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(firstKey, 7, newDocId), REDISMODULE_OK);

  // Reset RDB IO for second cycle
  RMCK_ResetRdbIO(rdbIO);

  // Get metadata from first key for second save
  uint64_t secondMeta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), firstKey, &secondMeta), REDISMODULE_OK);
  EXPECT_NE(secondMeta, 0);

  docIdMetaRDBSave(rdbIO, nullptr, &secondMeta);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);

  rdbIO->read_pos = 0;

  uint64_t secondLoadedMeta = 0;
  result = docIdMetaRDBLoad(rdbIO, &secondLoadedMeta, 1);
  EXPECT_EQ(result, REDISMODULE_OK);
  EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);
  EXPECT_NE(secondLoadedMeta, 0);

  RedisModuleKey *secondKey = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "second", 6), REDISMODULE_WRITE);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), secondKey, secondLoadedMeta), REDISMODULE_OK);

  // Verify both original and new data
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(secondKey, 3, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, originalDocId);

  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(secondKey, 7, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, newDocId);

  RedisModule_CloseKey(firstKey);
  RedisModule_CloseKey(secondKey);
}

TEST_F(DocIdMetaTest, TestSingleElementRdbSaveLoad) {
  // Test with just one element at index 0
  uint64_t singleDocId = 99999;

  EXPECT_EQ(DocIdMeta_SetDocIdForIndex(testKey, 0, singleDocId), REDISMODULE_OK);

  // Get the metadata for RDB save
  uint64_t meta = 0;
  EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), testKey, &meta), REDISMODULE_OK);
  EXPECT_NE(meta, 0);

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

  // Create a new key and set the loaded metadata
  RedisModuleKey *newKey = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, "singlekey", 9), REDISMODULE_WRITE);
  EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), newKey, loadedMeta), REDISMODULE_OK);

  // Verify loaded data
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 0, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, singleDocId);

  // Verify other indices are empty
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 1, &retrieved), REDISMODULE_ERR);
  EXPECT_EQ(DocIdMeta_GetDocIdForIndex(newKey, 5, &retrieved), REDISMODULE_ERR);

  RedisModule_CloseKey(newKey);
}
