/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "common.h"
#include "redismock/redismock.h"

extern "C" {
#include "spec.h"
#include "query_error.h"

// Forward declarations for RDB functions
extern void Indexes_RdbSave(RedisModuleIO *rdb, int when);
extern int Indexes_RdbLoad(RedisModuleIO *rdb, int encver, int when);
extern void Spec_AddToDict(RefManager *rm);  // Helper to add spec to global dict
}

class RdbMockTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize Redis mock
        ctx = RedisModule_GetThreadSafeContext(NULL);
        ASSERT_TRUE(ctx != nullptr);
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
            ctx = nullptr;
        }
    }

    RedisModuleCtx *ctx = nullptr;
};

TEST_F(RdbMockTest, testBasicRdbOperations) {
    // Test basic RDB save/load operations
    RedisModuleIO *io = RMCK_CreateRdbIO();
    ASSERT_TRUE(io != nullptr);

    // Test unsigned integer
    uint64_t original_uint = 0x123456789ABCDEF0ULL;
    RMCK_SaveUnsigned(io, original_uint);

    // Test signed integer
    int64_t original_int = -0x123456789ABCDEF0LL;
    RMCK_SaveSigned(io, original_int);

    // Test double
    double original_double = 3.14159265359;
    RMCK_SaveDouble(io, original_double);

    // Test string
    const char *original_str = "Hello, RediSearch!";
    RMCK_SaveStringBuffer(io, original_str, strlen(original_str));

    // Reset read position
    io->read_pos = 0;

    // Load and verify
    uint64_t loaded_uint = RMCK_LoadUnsigned(io);
    EXPECT_EQ(original_uint, loaded_uint);

    int64_t loaded_int = RMCK_LoadSigned(io);
    EXPECT_EQ(original_int, loaded_int);

    double loaded_double = RMCK_LoadDouble(io);
    EXPECT_DOUBLE_EQ(original_double, loaded_double);

    size_t loaded_str_len;
    char *loaded_str = RMCK_LoadStringBuffer(io, &loaded_str_len);
    ASSERT_TRUE(loaded_str != nullptr);
    EXPECT_EQ(strlen(original_str), loaded_str_len);
    EXPECT_STREQ(original_str, loaded_str);
    free(loaded_str);

    // Verify no errors
    EXPECT_EQ(0, RMCK_IsIOError(io));

    RMCK_FreeRdbIO(io);
}

TEST_F(RdbMockTest, testCreateIndexSpec) {
    // Test creating a simple IndexSpec using IndexSpec_ParseC
    const char *args[] = {"SCHEMA", "title", "TEXT", "WEIGHT", "1.0", "body", "TEXT", "price", "NUMERIC"};
    QueryError err = QueryError_Default();

    StrongRef spec_ref = IndexSpec_ParseC("test_idx", args, sizeof(args) / sizeof(const char *), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

    IndexSpec *spec = (IndexSpec *)StrongRef_Get(spec_ref);
    ASSERT_TRUE(spec != nullptr);

    // Verify basic properties
    EXPECT_EQ(3, spec->numFields);
    EXPECT_TRUE(spec->fields != nullptr);

    // Verify the rwlock is properly initialized
    // We can't directly test the lock state, but we can verify it's initialized
    // by trying to acquire and release it
    int lock_result = pthread_rwlock_tryrdlock(&spec->rwlock);
    if (lock_result == 0) {
        pthread_rwlock_unlock(&spec->rwlock);
    }
    // If tryrdlock failed, it means the lock is either already locked or there's an error
    // For a newly created spec, it should be unlocked, so we expect success (0)
    EXPECT_EQ(0, lock_result);

    // Clean up
    IndexSpec_RemoveFromGlobals(spec_ref, false);
}

// Helper function to test lock state
bool testLockState(IndexSpec *spec) {
    int lock_result = pthread_rwlock_tryrdlock(&spec->rwlock);
    if (lock_result == 0) {
        pthread_rwlock_unlock(&spec->rwlock);
        return true;  // Lock is properly initialized and unlocked
    }
    return false;  // Lock failed - either not initialized or locked
}

// Second function - IndexSpec RDB serialization test
TEST_F(RdbMockTest, testIndexSpecRdbSerialization) {

    // Create an IndexSpec
    const char *args[] = {"SCHEMA", "title", "TEXT", "WEIGHT", "2.0", "body", "TEXT", "price", "NUMERIC"};
    QueryError err = QueryError_Default();

    StrongRef original_spec_ref = IndexSpec_ParseC("test_rdb_idx", args, sizeof(args) / sizeof(const char *), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

    IndexSpec *spec = (IndexSpec *)StrongRef_Get(original_spec_ref);
    ASSERT_TRUE(spec != nullptr);
    std::unique_ptr<IndexSpec, std::function<void(IndexSpec *)>> specPtr(spec, [](IndexSpec *spec) {
        StrongRef_Release(spec->own_ref);
    });

    // Verify original lock state
    EXPECT_TRUE(testLockState(spec)) << "Original IndexSpec should have properly initialized rwlock";

    // Create RDB IO context
    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Save all indexes to RDB using existing function (while spec is still in globals)
    IndexSpec_RdbSave(io, spec);
    EXPECT_EQ(0, RMCK_IsIOError(io));

    // Reset read position to load it back
    io->read_pos = 0;

    QueryError status = QueryError_Default();
    IndexSpec *loadedSpec = IndexSpec_RdbLoad(io, INDEX_CURRENT_VERSION, &status);
    EXPECT_TRUE(loadedSpec != nullptr);
    std::unique_ptr<IndexSpec, std::function<void(IndexSpec *)>> loadedSpecPtr(loadedSpec, [](IndexSpec *spec) {
        StrongRef_Release(spec->own_ref);
    });
    EXPECT_FALSE(QueryError_HasError(&status)) << QueryError_GetUserError(&status);
    EXPECT_EQ(0, RMCK_IsIOError(io));

    // Compare the original and loaded specs
    EXPECT_EQ(spec->numFields, loadedSpec->numFields);
    EXPECT_EQ(spec->flags, loadedSpec->flags);
    EXPECT_EQ(spec->timeout, loadedSpec->timeout);
    EXPECT_EQ(spec->isTimerSet, loadedSpec->isTimerSet);
    EXPECT_EQ(spec->timerId, loadedSpec->timerId);
    EXPECT_EQ(spec->monitorDocumentExpiration, loadedSpec->monitorDocumentExpiration);
    EXPECT_EQ(spec->monitorFieldExpiration, loadedSpec->monitorFieldExpiration);
    EXPECT_EQ(spec->isDuplicate, loadedSpec->isDuplicate);
    EXPECT_EQ(spec->scan_in_progress, loadedSpec->scan_in_progress);
    EXPECT_EQ(spec->scan_failed_OOM, loadedSpec->scan_failed_OOM);
    EXPECT_EQ(spec->used_dialects, loadedSpec->used_dialects);
    EXPECT_EQ(spec->counter, loadedSpec->counter);
    EXPECT_EQ(spec->activeCursors, loadedSpec->activeCursors);
    // verify read locks can be taken
    int lockResult = pthread_rwlock_tryrdlock(&spec->rwlock);
    EXPECT_EQ(0, lockResult);
    if (lockResult == 0) {
        pthread_rwlock_unlock(&spec->rwlock);
    }
    lockResult = pthread_rwlock_tryrdlock(&loadedSpec->rwlock);
    EXPECT_EQ(0, lockResult);
    if (lockResult == 0) {
        pthread_rwlock_unlock(&loadedSpec->rwlock);
    }

    // verify write locks can be taken
    lockResult = pthread_rwlock_trywrlock(&spec->rwlock);
    EXPECT_EQ(0, lockResult);
    if (lockResult == 0) {
        pthread_rwlock_unlock(&spec->rwlock);
    }
    lockResult = pthread_rwlock_trywrlock(&loadedSpec->rwlock);
    EXPECT_EQ(0, lockResult);
    if (lockResult == 0) {
        pthread_rwlock_unlock(&loadedSpec->rwlock);
    }

    // Verify field specifications are preserved
    for (int i = 0; i < loadedSpec->numFields; i++) {
        FieldSpec *field = &spec->fields[i];
        FieldSpec *loadedField = &loadedSpec->fields[i];
        EXPECT_NE(loadedField->types, 0);
        EXPECT_GE(loadedField->index, 0);
        EXPECT_NE(loadedField->fieldName, nullptr);
    }
}

TEST_F(RdbMockTest, testIndexSpecStringSerialize) {

    // Create an IndexSpec
    const char *args[] = {"SCHEMA", "title", "TEXT", "WEIGHT", "2.0", "body", "TEXT", "price", "NUMERIC"};
    QueryError err = QueryError_Default();

    StrongRef original_spec_ref = IndexSpec_ParseC("test_rdb_idx", args, sizeof(args) / sizeof(const char *), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

    IndexSpec *spec = (IndexSpec *)StrongRef_Get(original_spec_ref);

    ASSERT_TRUE(spec != nullptr);

    // Create serialized string
    RedisModuleString *serialized = IndexSpec_Serialize(spec);
    int encver = INDEX_CURRENT_VERSION;
    ASSERT_TRUE(serialized != nullptr);

    // Drop the original spec from globals
    IndexSpec_RemoveFromGlobals(original_spec_ref, false);
    ASSERT_TRUE(IndexSpec_LoadUnsafe("test_rdb_idx").rm == NULL);

    // Deserialize
    int res = IndexSpec_Deserialize(serialized, encver);
    ASSERT_EQ(REDISMODULE_OK, res);
    StrongRef loaded_spec_ref = IndexSpec_LoadUnsafe("test_rdb_idx");
    spec = (IndexSpec *)StrongRef_Get(loaded_spec_ref);

    // Sanity checks that the spec is loaded correctly
    // This test verifies that the serialization and deserialization to string work correctly,
    // and isn't focused on deep equality of all fields. That's covered in other RDB tests.
    ASSERT_TRUE(spec != nullptr);
    ASSERT_STREQ(HiddenString_GetUnsafe(spec->specName, NULL), "test_rdb_idx");
    ASSERT_EQ(spec->numFields, 3);
    ASSERT_STREQ(HiddenString_GetUnsafe(spec->fields[0].fieldName, NULL), "title");
    ASSERT_STREQ(HiddenString_GetUnsafe(spec->fields[1].fieldName, NULL), "body");
    ASSERT_STREQ(HiddenString_GetUnsafe(spec->fields[2].fieldName, NULL), "price");

    // Clean up
    IndexSpec_RemoveFromGlobals(loaded_spec_ref, false);
    RedisModule_FreeString(NULL, serialized);
}

TEST_F(RdbMockTest, testDuplicateIndexRdbLoad) {
    // Create an index with a single text field
    const char *args[] = {"ON", "HASH", "SCHEMA", "title", "TEXT"};
    QueryError err = QueryError_Default();

    StrongRef spec_ref = IndexSpec_ParseC("test_duplicate_idx", args, sizeof(args) / sizeof(const char *), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

    IndexSpec *spec = (IndexSpec *)StrongRef_Get(spec_ref);
    ASSERT_TRUE(spec != nullptr);

    // Create RDB IO context
    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Write the same index 30 times to RDB
    // First write the count (30)
    RMCK_SaveUnsigned(io, 30);

    // Then write the index 30 times
    for (int i = 0; i < 30; i++) {
        IndexSpec_RdbSave(io, spec);
    }
    EXPECT_EQ(0, RMCK_IsIOError(io));

    // Remove the original spec from globals before loading from RDB
    IndexSpec_RemoveFromGlobals(spec_ref, false);
    ASSERT_TRUE(IndexSpec_LoadUnsafe("test_duplicate_idx").rm == NULL);

    // Reset read position to load from RDB
    io->read_pos = 0;

    // Load from RDB - this should load 30 copies but only store one
    int result = Indexes_RdbLoad(io, INDEX_CURRENT_VERSION, REDISMODULE_AUX_BEFORE_RDB);
    EXPECT_EQ(REDISMODULE_OK, result);
    EXPECT_EQ(0, RMCK_IsIOError(io));


    // Verify the loaded index exists and has the correct name
    StrongRef loaded_spec_ref = IndexSpec_LoadUnsafe("test_duplicate_idx");
    IndexSpec *loaded_spec = (IndexSpec *)StrongRef_Get(loaded_spec_ref);
    ASSERT_TRUE(loaded_spec != nullptr);
    ASSERT_STREQ(HiddenString_GetUnsafe(loaded_spec->specName, NULL), "test_duplicate_idx");
    ASSERT_EQ(loaded_spec->numFields, 1);

    // Clean up
    IndexSpec_RemoveFromGlobals(loaded_spec_ref, false);
}
