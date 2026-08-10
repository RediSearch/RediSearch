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

    StrongRef spec_ref = IndexSpec_ParseC(NULL, "test_idx", args, sizeof(args) / sizeof(const char *), &err);
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

    StrongRef original_spec_ref = IndexSpec_ParseC(NULL, "test_rdb_idx", args, sizeof(args) / sizeof(const char *), &err);
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
    IndexSpec_RdbSave(io, spec, 0);
    EXPECT_EQ(0, RMCK_IsIOError(io));

    // Reset read position to load it back
    io->read_pos = 0;

    QueryError status = QueryError_Default();
    IndexSpec *loadedSpec = IndexSpec_RdbLoad(io, INDEX_CURRENT_VERSION, false, &status);
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

TEST_F(RdbMockTest, testIndexSpecRdbLoadNormalizesInvalidStorageFlags) {

    const char *args[] = {"NOFIELDS", "SCHEMA", "title", "TEXT"};
    QueryError err = QueryError_Default();

    StrongRef original_spec_ref = IndexSpec_ParseC(NULL, "test_rdb_invalid_idx", args, sizeof(args) / sizeof(const char *), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

    IndexSpec *spec = (IndexSpec *)StrongRef_Get(original_spec_ref);
    ASSERT_TRUE(spec != nullptr);
    std::unique_ptr<IndexSpec, std::function<void(IndexSpec *)>> specPtr(spec, [](IndexSpec *spec) {
        StrongRef_Release(spec->own_ref);
    });

    // Make sure Index_StoreFieldFlags is not set, and turn on Index_WideSchema which is invalid without it, to simulate an invalid RDB state that we want to normalize on load
    ASSERT_FALSE(spec->flags & Index_StoreFieldFlags) << "Original IndexSpec should not have storage flags set";
    uint64_t flags = spec->flags;
    flags |= Index_WideSchema;
    spec->flags = (IndexFlags)flags;

    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    IndexSpec_RdbSave(io, spec, 0);
    EXPECT_EQ(0, RMCK_IsIOError(io));

    io->read_pos = 0;

    QueryError status = QueryError_Default();
    IndexSpec *loadedSpec = IndexSpec_RdbLoad(io, INDEX_CURRENT_VERSION, false, &status);
    ASSERT_NE(nullptr, loadedSpec);
    std::unique_ptr<IndexSpec, std::function<void(IndexSpec *)>> loadedSpecPtr(loadedSpec, [](IndexSpec *spec) {
        StrongRef_Release(spec->own_ref);
    });
    // We expect no error, and the invalid storage flags to be normalized (i.e., Index_WideSchema should be turned off because Index_StoreFieldFlags is not set)
    EXPECT_FALSE(QueryError_HasError(&status)) << QueryError_GetUserError(&status);
    EXPECT_FALSE(loadedSpec->flags & Index_WideSchema);
    EXPECT_FALSE(loadedSpec->flags & Index_StoreFieldFlags);
}

TEST_F(RdbMockTest, testIndexSpecStringSerialize) {

    // Create an IndexSpec
    const char *args[] = {"SCHEMA", "title", "TEXT", "WEIGHT", "2.0", "body", "TEXT", "price", "NUMERIC"};
    QueryError err = QueryError_Default();

    StrongRef original_spec_ref = IndexSpec_ParseC(NULL, "test_rdb_idx", args, sizeof(args) / sizeof(const char *), &err);
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

    StrongRef spec_ref = IndexSpec_ParseC(NULL, "test_duplicate_idx", args, sizeof(args) / sizeof(const char *), &err);
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
        IndexSpec_RdbSave(io, spec, 0);
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

// Legacy pre-2.0 module types (ft_invidx / numericdx / ft_tagidx) are registered only so
// that an old RDB can be read and discarded during an upgrade. Their loaders return the
// `dummyNonNull` sentinel rather than NULL, so a key can outlive the upgrade sweep holding
// nothing but that sentinel. Such a key must still serialize to a payload its own loader
// accepts: emitting zero bytes leaves the module payload unterminated, and Redis then
// rejects the whole RDB with "not terminated by the proper module value EOF marker",
// breaking replication, backups and exports. See MOD-15685.
extern "C" {
extern void *dummyNonNull;

void InvertedIndex_RdbSave_Empty(RedisModuleIO *rdb, void *value);
void NumericIndexType_RdbSave_Empty(RedisModuleIO *rdb, void *value);
void TagIndex_RdbSave_Empty(RedisModuleIO *rdb, void *value);

void *InvertedIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver);
void *NumericIndexType_RdbLoad_Consume(RedisModuleIO *rdb, int encver);
void *TagIndex_RdbLoad_Consume(RedisModuleIO *rdb, int encver);
}

namespace {
// Matches LEGACY_ENC_VER / LEGACY_LEGACY_ENC_VER in src/legacy_types.c, which are private
// to that translation unit.
constexpr int kLegacyEncVer = 1;
constexpr int kLegacyLegacyEncVer = 0;

struct LegacyTypeCase {
  const char *name;
  void (*save)(RedisModuleIO *, void *);
  void *(*load)(RedisModuleIO *, int);
};
}  // namespace

TEST_F(RdbMockTest, testLegacyTypeDummyValueRoundTrip) {
  const LegacyTypeCase cases[] = {
      {"ft_invidx", InvertedIndex_RdbSave_Empty, InvertedIndex_RdbLoad_Consume},
      {"numericdx", NumericIndexType_RdbSave_Empty, NumericIndexType_RdbLoad_Consume},
      {"ft_tagidx", TagIndex_RdbSave_Empty, TagIndex_RdbLoad_Consume},
  };

  for (const LegacyTypeCase &c : cases) {
    RedisModuleIO *io = RMCK_CreateRdbIO();
    ASSERT_TRUE(io != nullptr) << c.name;

    c.save(io, dummyNonNull);

    // A zero-byte save is the defect this guards against: the loader below would read
    // past the end of the payload and desync every later record in the RDB.
    EXPECT_GT(io->buffer.size(), 0u) << c.name << " wrote an empty payload";
    EXPECT_EQ(0, RMCK_IsIOError(io)) << c.name << " errored while saving";

    io->read_pos = 0;
    EXPECT_EQ(dummyNonNull, c.load(io, kLegacyEncVer)) << c.name;
    EXPECT_EQ(0, RMCK_IsIOError(io)) << c.name << " errored while loading";
    // The loader must consume exactly what was written, otherwise the module payload is
    // left unterminated and Redis reports the RDB as corrupt.
    EXPECT_EQ(io->buffer.size(), io->read_pos) << c.name << " did not consume its payload";

    RMCK_FreeRdbIO(io);
  }
}

// The numeric loader picks its wire format from encver, so the single terminator written by
// the save callback has to be valid under the v0 framing (a zero entry count) as well.
TEST_F(RdbMockTest, testLegacyNumericDummyValueRoundTripEncver0) {
  RedisModuleIO *io = RMCK_CreateRdbIO();
  ASSERT_TRUE(io != nullptr);

  NumericIndexType_RdbSave_Empty(io, dummyNonNull);
  EXPECT_GT(io->buffer.size(), 0u);

  io->read_pos = 0;
  EXPECT_EQ(dummyNonNull, NumericIndexType_RdbLoad_Consume(io, kLegacyLegacyEncVer));
  EXPECT_EQ(0, RMCK_IsIOError(io));
  EXPECT_EQ(io->buffer.size(), io->read_pos);

  RMCK_FreeRdbIO(io);
}

// The legacy loaders are reachable from RESTORE, so every count they read is untrusted input.
// A truncated payload that declares a huge count must abort on the IO error instead of iterating
// the declared number of times: once the stream is exhausted the Load* calls return 0 without
// consuming anything, so a counted loop would otherwise spin. See MOD-15685.
//
// Note the failure mode if a guard is ever removed: this test does not fail, it hangs, because the
// loop then really does run UINT64_MAX times. A CI timeout pointing here means a missing
// RedisModule_IsIOError check in one of the loaders below.
TEST_F(RdbMockTest, testLegacyLoadersRejectTruncatedHugeCounts) {
  const uint64_t huge = UINT64_MAX;

  {
    // ft_invidx: flags, lastId, numDocs, then a block count with no blocks behind it.
    RedisModuleIO *io = RMCK_CreateRdbIO();
    ASSERT_TRUE(io != nullptr);
    RMCK_SaveUnsigned(io, 0);
    RMCK_SaveUnsigned(io, 0);
    RMCK_SaveUnsigned(io, 0);
    RMCK_SaveUnsigned(io, huge);
    io->read_pos = 0;
    EXPECT_EQ(nullptr, InvertedIndex_RdbLoad_Consume(io, kLegacyEncVer));
    EXPECT_NE(0, RMCK_IsIOError(io));
    RMCK_FreeRdbIO(io);
  }

  {
    // numericdx v0: an entry count with no entries behind it. The v1 framing is terminator-based,
    // so a failed read ends its loop on its own.
    RedisModuleIO *io = RMCK_CreateRdbIO();
    ASSERT_TRUE(io != nullptr);
    RMCK_SaveUnsigned(io, huge);
    io->read_pos = 0;
    EXPECT_EQ(nullptr, NumericIndexType_RdbLoad_Consume(io, kLegacyLegacyEncVer));
    EXPECT_NE(0, RMCK_IsIOError(io));
    RMCK_FreeRdbIO(io);
  }

  {
    // ft_tagidx: a tag count with no tags behind it.
    RedisModuleIO *io = RMCK_CreateRdbIO();
    ASSERT_TRUE(io != nullptr);
    RMCK_SaveUnsigned(io, huge);
    io->read_pos = 0;
    EXPECT_EQ(nullptr, TagIndex_RdbLoad_Consume(io, kLegacyEncVer));
    EXPECT_NE(0, RMCK_IsIOError(io));
    RMCK_FreeRdbIO(io);
  }
}

// Redis matches a module type on its 54-bit signature and ignores the 10-bit encoding version, so
// a crafted RESTORE can reach these loaders with any encver. All three must refuse anything they
// do not understand rather than parsing it as a version they do.
TEST_F(RdbMockTest, testLegacyLoadersRejectUnknownEncver) {
  const int unsupported[] = {kLegacyEncVer + 1, 7, 1023};

  for (int encver : unsupported) {
    RedisModuleIO *io = RMCK_CreateRdbIO();
    ASSERT_TRUE(io != nullptr) << encver;
    // A well-formed empty ft_invidx payload, so a refusal cannot be mistaken for a short read.
    InvertedIndex_RdbSave_Empty(io, dummyNonNull);
    io->read_pos = 0;

    EXPECT_EQ(nullptr, InvertedIndex_RdbLoad_Consume(io, encver)) << encver;
    io->read_pos = 0;
    EXPECT_EQ(nullptr, NumericIndexType_RdbLoad_Consume(io, encver)) << encver;
    io->read_pos = 0;
    EXPECT_EQ(nullptr, TagIndex_RdbLoad_Consume(io, encver)) << encver;

    RMCK_FreeRdbIO(io);
  }
}
