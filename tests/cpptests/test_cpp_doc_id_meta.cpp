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


class DocIdMetaTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Get context - MyEnvironment already initialized redismock
    ctx = RedisModule_GetThreadSafeContext(nullptr);
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
    RedisModule_HashSet(key, REDISMODULE_HASH_NONE, fieldName, fieldValue, nullptr);
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
      ctx = nullptr;
    }
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

  // Helper: create a Redis hash key with a dummy field/value.
  // Returns the RedisModuleString key name (caller must free).
  RedisModuleString *createHashKey(const char *name) {
    RedisModuleString *keyName = RedisModule_CreateStringPrintf(ctx, "%s", name);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_WRITE);
    RedisModuleString *fieldName = RedisModule_CreateString(ctx, "field", 5);
    RedisModuleString *fieldValue = RedisModule_CreateString(ctx, "value", 5);
    RedisModule_HashSet(key, REDISMODULE_HASH_NONE, fieldName, fieldValue, nullptr);
    RedisModule_FreeString(ctx, fieldName);
    RedisModule_FreeString(ctx, fieldValue);
    RedisModule_CloseKey(key);
    return keyName;
  }

  // Helper: get the raw metadata uint64 for a key.
  uint64_t getKeyMeta(RedisModuleString *keyName) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);
    uint64_t meta = 0;
    EXPECT_EQ(RedisModule_GetKeyMeta(DocIdMeta_GetClassId(), key, &meta), REDISMODULE_OK);
    EXPECT_NE(meta, 0);
    RedisModule_CloseKey(key);
    return meta;
  }

  // Helper: RDB save. Writes meta to rdbIO buffer.
  void rdbSave(uint64_t meta) {
    docIdMetaRDBSave(rdbIO, nullptr, &meta);
    EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);
  }

  // Helper: RDB load from the current rdbIO buffer. Returns the loaded metadata.
  uint64_t rdbLoad() {
    rdbIO->read_pos = 0;
    uint64_t loadedMeta = 0;
    int result = docIdMetaRDBLoad(rdbIO, &loadedMeta, 1);
    EXPECT_EQ(result, REDISMODULE_OK);
    EXPECT_EQ(RMCK_IsIOError(rdbIO), 0);
    EXPECT_NE(loadedMeta, 0);
    return loadedMeta;
  }

  // Helper: RDB save, reset, load. Returns the loaded metadata.
  uint64_t rdbSaveAndLoad(uint64_t meta) {
    rdbSave(meta);
    return rdbLoad();
  }

  // Helper: create a new hash key and attach metadata to it. Returns the key name.
  RedisModuleString *createKeyWithMeta(const char *name, uint64_t meta) {
    RedisModuleString *keyName = createHashKey(name);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_WRITE);
    EXPECT_EQ(RedisModule_SetKeyMeta(DocIdMeta_GetClassId(), key, meta), REDISMODULE_OK);
    RedisModule_CloseKey(key);
    return keyName;
  }

  // Helper: verify a docId can be retrieved for a spec on a key.
  void verifyDocId(RedisModuleString *keyName, uint64_t specId, uint64_t expectedDocId) {
    uint64_t retrieved;
    EXPECT_EQ(DocIdMeta_Get(ctx, keyName, specId, &retrieved), REDISMODULE_OK);
    EXPECT_EQ(retrieved, expectedDocId);
  }

  // Helper: verify a docId is missing for a spec on a key.
  void verifyDocIdMissing(RedisModuleString *keyName, uint64_t specId) {
    uint64_t retrieved;
    EXPECT_EQ(DocIdMeta_Get(ctx, keyName, specId, &retrieved), REDISMODULE_ERR);
  }

  RedisModuleCtx *ctx;
  RedisModuleString *testKeyName;
  RedisModuleIO *rdbIO;
  std::vector<RefManager*> createdSpecs;

  // Helper constants for spec IDs and names
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

TEST_F(DocIdMetaTest, TestSoftDeleteDocId) {
  uint64_t docId = 555;

  // Set a value first
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, docId), REDISMODULE_OK);

  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, docId);

  // Soft-delete the value (invalidates but keeps entry for reuse)
  int result = DocIdMeta_SoftDelete(ctx, testKeyName, SPEC1_ID);
  EXPECT_EQ(result, REDISMODULE_OK);

  // Should now return error when trying to get
  result = DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestSoftDeleteNonExistentDocId) {
  // Try to soft-delete a docId that doesn't exist
  int result = DocIdMeta_SoftDelete(ctx, testKeyName, 999);
  EXPECT_EQ(result, REDISMODULE_ERR);
}

TEST_F(DocIdMetaTest, TestMultipleKeys) {
  // Test that different keys maintain separate docId maps
  RedisModuleString *keyName1 = createHashKey("testkey1");
  RedisModuleString *keyName2 = createHashKey("testkey2");

  // Set different values for the same spec on different keys
  EXPECT_EQ(DocIdMeta_Set(ctx, keyName1, SPEC1_ID, 111), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, keyName2, SPEC1_ID, 222), REDISMODULE_OK);

  // Verify they're independent
  verifyDocId(keyName1, SPEC1_ID, 111);
  verifyDocId(keyName2, SPEC1_ID, 222);

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

TEST_F(DocIdMetaTest, TestSoftDeleteAndReget) {
  // Test that getting from an unset spec returns ERR
  uint64_t retrieved;
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);

  // Set a valid docId and then soft-delete it to test soft-deletion behavior
  uint64_t validDocId = 42;
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, validDocId), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_OK);
  EXPECT_EQ(retrieved, validDocId);

  // Soft-delete it and verify it's gone (should return ERR like uninitialized)
  EXPECT_EQ(DocIdMeta_SoftDelete(ctx, testKeyName, SPEC1_ID), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Get(ctx, testKeyName, SPEC1_ID, &retrieved), REDISMODULE_ERR);
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
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 12345), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 67890), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, 11111), REDISMODULE_OK);

  // RDB round-trip
  uint64_t meta = getKeyMeta(testKeyName);
  uint64_t loadedMeta = rdbSaveAndLoad(meta);
  RedisModuleString *newKeyName = createKeyWithMeta("newkey", loadedMeta);

  // Verify loaded data
  verifyDocId(newKeyName, SPEC1_ID, 12345);
  verifyDocId(newKeyName, SPEC2_ID, 67890);
  verifyDocId(newKeyName, SPEC3_ID, 11111);
  verifyDocIdMissing(newKeyName, 999);

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

  // Test with multiple specs: (specId, docId)
  struct SpecEntry { uint64_t specId; uint64_t docId; };
  std::vector<SpecEntry> specs = {
    {SPEC1_ID, 1001}, {SPEC2_ID, 2002}, {SPEC3_ID, 3003}, {4, 4004},
  };

  for (const auto& spec : specs) {
    EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, spec.specId, spec.docId), REDISMODULE_OK);
  }

  // RDB round-trip
  uint64_t meta = getKeyMeta(testKeyName);
  uint64_t loadedMeta = rdbSaveAndLoad(meta);
  RedisModuleString *newKeyName = createKeyWithMeta("largekey", loadedMeta);

  for (const auto& spec : specs) {
    verifyDocId(newKeyName, spec.specId, spec.docId);
  }
  verifyDocIdMissing(newKeyName, 999);

  RedisModule_FreeString(ctx, newKeyName);
}

TEST_F(DocIdMetaTest, TestMaxValueRdbSaveLoad) {
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec2", SPEC2_ID);

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, UINT64_MAX), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 1), REDISMODULE_OK);

  // RDB round-trip
  uint64_t meta = getKeyMeta(testKeyName);
  uint64_t loadedMeta = rdbSaveAndLoad(meta);
  RedisModuleString *newKeyName = createKeyWithMeta("maxkey", loadedMeta);

  verifyDocId(newKeyName, SPEC1_ID, UINT64_MAX);
  verifyDocId(newKeyName, SPEC2_ID, 1);

  RedisModule_FreeString(ctx, newKeyName);
}

TEST_F(DocIdMetaTest, TestSingleElementRdbSaveLoad) {
  addTestSpec("spec1", SPEC1_ID);

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 99999), REDISMODULE_OK);

  // RDB round-trip
  uint64_t meta = getKeyMeta(testKeyName);
  uint64_t loadedMeta = rdbSaveAndLoad(meta);
  RedisModuleString *newKeyName = createKeyWithMeta("singlekey", loadedMeta);

  verifyDocId(newKeyName, SPEC1_ID, 99999);
  verifyDocIdMissing(newKeyName, SPEC2_ID);
  verifyDocIdMissing(newKeyName, SPEC3_ID);

  RedisModule_FreeString(ctx, newKeyName);
}


///////////////////////////////////////////////////////////////////////////////////////////////
// RDB save/load filtering tests (specs not in specDict_g are skipped)
///////////////////////////////////////////////////////////////////////////////////////////////

TEST_F(DocIdMetaTest, TestRdbLoadSkipsRemovedSpecEntries) {
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec2", SPEC2_ID);
  addTestSpec("spec3", SPEC3_ID);

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, 3003), REDISMODULE_OK);

  // Save to RDB while all 3 specs are live
  rdbSave(getKeyMeta(testKeyName));

  // Remove SPEC2 from specDict_g (simulates index drop)
  freeSpec(createdSpecs[1]);
  createdSpecs.erase(createdSpecs.begin() + 1);

  // Load from RDB — SPEC2_ID entry should be skipped
  uint64_t loadedMeta = rdbLoad();
  RedisModuleString *newKeyName = createKeyWithMeta("stalekey", loadedMeta);

  verifyDocId(newKeyName, SPEC1_ID, 1001);
  verifyDocIdMissing(newKeyName, SPEC2_ID);
  verifyDocId(newKeyName, SPEC3_ID, 3003);

  RedisModule_FreeString(ctx, newKeyName);
}

TEST_F(DocIdMetaTest, TestRdbSaveSkipsRemovedSpecEntries) {
  // Create only SPEC1 and SPEC3 in specDict_g (SPEC2 is "dropped")
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec3", SPEC3_ID);

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, 3003), REDISMODULE_OK);

  // RDB round-trip — should skip SPEC2_ID (not in specIdDict_g)
  uint64_t loadedMeta = rdbSaveAndLoad(getKeyMeta(testKeyName));
  RedisModuleString *newKeyName = createKeyWithMeta("savekey", loadedMeta);

  verifyDocId(newKeyName, SPEC1_ID, 1001);
  verifyDocIdMissing(newKeyName, SPEC2_ID);
  verifyDocId(newKeyName, SPEC3_ID, 3003);

  RedisModule_FreeString(ctx, newKeyName);
}

TEST_F(DocIdMetaTest, TestRdbSaveAllRemoved_SavesNothing) {
  // No specs in specIdDict_g — all entries are considered stale
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001), REDISMODULE_OK);

  // Save to RDB — all entries are stale, should save nothing
  rdbSave(getKeyMeta(testKeyName));
  // Nothing was written, so we can't call rdbLoad — just verify no crash.
}

TEST_F(DocIdMetaTest, TestRdbSaveSkipsSoftDeletedEntries) {
  addTestSpec("spec1", SPEC1_ID);
  addTestSpec("spec2", SPEC2_ID);
  addTestSpec("spec3", SPEC3_ID);

  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC3_ID, 3003), REDISMODULE_OK);

  // Soft-delete SPEC2's entry
  EXPECT_EQ(DocIdMeta_SoftDelete(ctx, testKeyName, SPEC2_ID), REDISMODULE_OK);
  verifyDocIdMissing(testKeyName, SPEC2_ID);

  // RDB round-trip — should skip SPEC2 (soft-deleted)
  uint64_t loadedMeta = rdbSaveAndLoad(getKeyMeta(testKeyName));
  RedisModuleString *newKeyName = createKeyWithMeta("softdelkey", loadedMeta);

  verifyDocId(newKeyName, SPEC1_ID, 1001);
  verifyDocIdMissing(newKeyName, SPEC2_ID);
  verifyDocId(newKeyName, SPEC3_ID, 3003);

  RedisModule_FreeString(ctx, newKeyName);
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Unlink callback tests
//
// Note: These tests don't add specs to specIdDict_g, so findSpecBySpecId returns NULL
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
  // Don't add specs to specIdDict_g - this tests entry invalidation without triggering
  // IndexSpec_DeleteDocById (which requires disk-based indexes)
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002), REDISMODULE_OK);

  verifyDocId(testKeyName, SPEC1_ID, 1001);
  verifyDocId(testKeyName, SPEC2_ID, 2002);

  // Call unlink - specs don't exist in specIdDict_g, so IndexSpec_DeleteDocById is skipped
  // but entries should still be invalidated
  uint64_t meta = getKeyMeta(testKeyName);
  docIdMetaUnlink(nullptr, &meta);

  verifyDocIdMissing(testKeyName, SPEC1_ID);
  verifyDocIdMissing(testKeyName, SPEC2_ID);
}

TEST_F(DocIdMetaTest, TestUnlinkSkipsSoftDeletedEntries) {
  // Don't add specs to specIdDict_g
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC1_ID, 1001), REDISMODULE_OK);
  EXPECT_EQ(DocIdMeta_Set(ctx, testKeyName, SPEC2_ID, 2002), REDISMODULE_OK);

  // Soft-delete SPEC1's entry
  EXPECT_EQ(DocIdMeta_SoftDelete(ctx, testKeyName, SPEC1_ID), REDISMODULE_OK);
  verifyDocIdMissing(testKeyName, SPEC1_ID);
  verifyDocId(testKeyName, SPEC2_ID, 2002);

  // Call unlink - should skip SPEC1 (already soft-deleted) and process SPEC2
  uint64_t meta = getKeyMeta(testKeyName);
  docIdMetaUnlink(nullptr, &meta);

  // Both entries should now be invalid
  verifyDocIdMissing(testKeyName, SPEC1_ID);
  verifyDocIdMissing(testKeyName, SPEC2_ID);
}
