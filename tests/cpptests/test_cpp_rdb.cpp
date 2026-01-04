/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "gtest/internal/gtest-port.h"  // For CaptureStderr/GetCapturedStderr
#include "common.h"
#include "redismock/redismock.h"
#include "synonym_map.h"
#include "trie/trie_type.h"
#include <cstdint>  // For SIZE_MAX, UINT32_MAX

extern "C" {
#include "spec.h"
#include "query_error.h"
#include "rules.h"
#include "stopwords.h"
#include "doc_table.h"

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
    QueryError err = {QUERY_OK};

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
    QueryError err = {QUERY_OK};

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

    QueryError status = {QUERY_OK, 0};
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

TEST_F(RdbMockTest, testDuplicateIndexRdbLoad) {
    // Create an index with a single text field
    const char *args[] = {"ON", "HASH", "SCHEMA", "title", "TEXT"};
    QueryError err = {QUERY_OK};

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

TEST_F(RdbMockTest, testSynonymMapRdbSerialization) {
    // Create a SynonymMap and add some terms
    SynonymMap *smap = SynonymMap_New(false);
    ASSERT_TRUE(smap != nullptr);
    std::unique_ptr<SynonymMap, std::function<void(SynonymMap *)>> smapPtr(smap, [](SynonymMap *s) {
        SynonymMap_Free(s);
    });

    // Add terms to synonym groups
    const char *group1_terms[] = {"hello", "hi", "greetings"};
    const char *group2_terms[] = {"bye", "goodbye", "farewell"};
    // hello and bye belong to multiple groups
    const char *group3_terms[] = {"hello", "bye"};

    ASSERT_EQ(SYNONYM_MAP_OK, SynonymMap_Add(smap, "greet", group1_terms, 3));
    ASSERT_EQ(SYNONYM_MAP_OK, SynonymMap_Add(smap, "part", group2_terms, 3));
    ASSERT_EQ(SYNONYM_MAP_OK, SynonymMap_Add(smap, "common", group3_terms, 2));

    // Verify the synonym map has the expected terms
    TermData *td = SynonymMap_GetIdsBySynonym_cstr(smap, "hello");
    ASSERT_TRUE(td != nullptr);
    // hello belongs to "greet" and "common"
    EXPECT_EQ(2, array_len(td->groupIds));

    // Create RDB IO context
    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Save synonym map to RDB
    SynonymMap_RdbSave(io, smap);
    EXPECT_EQ(0, RMCK_IsIOError(io));

    // Reset read position to load it back
    io->read_pos = 0;

    // Load synonym map from RDB
    SynonymMap *loadedSmap = (SynonymMap *)SynonymMap_RdbLoad(io, INDEX_CURRENT_VERSION);
    ASSERT_TRUE(loadedSmap != nullptr);
    std::unique_ptr<SynonymMap, std::function<void(SynonymMap *)>> loadedSmapPtr(loadedSmap, [](SynonymMap *s) {
        SynonymMap_Free(s);
    });
    EXPECT_EQ(0, RMCK_IsIOError(io));

    // Verify loaded synonym map has the same terms
    td = SynonymMap_GetIdsBySynonym_cstr(loadedSmap, "hello");
    ASSERT_TRUE(td != nullptr);
    EXPECT_EQ(2, array_len(td->groupIds));

    td = SynonymMap_GetIdsBySynonym_cstr(loadedSmap, "hi");
    ASSERT_TRUE(td != nullptr);
    EXPECT_EQ(1, array_len(td->groupIds));

    td = SynonymMap_GetIdsBySynonym_cstr(loadedSmap, "bye");
    ASSERT_TRUE(td != nullptr);
    EXPECT_EQ(2, array_len(td->groupIds));

    td = SynonymMap_GetIdsBySynonym_cstr(loadedSmap, "farewell");
    ASSERT_TRUE(td != nullptr);
    EXPECT_EQ(1, array_len(td->groupIds));
}

TEST_F(RdbMockTest, testSynonymMapRdbLoadExceedsTermsLimit) {
    // Test that loading a synonym map with more terms than MAX_SYNONYM_TERMS fails
    // The check is in SynonymMap_RdbLoad at the dict_size level

    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Write dict_size that exceeds MAX_SYNONYM_TERMS
    uint64_t dict_size = MAX_SYNONYM_TERMS + 1;
    RMCK_SaveUnsigned(io, dict_size);

    // Write some dummy data to prove failure is from limit check, not EOF
    const char *term = "test_term";
    RMCK_SaveStringBuffer(io, term, strlen(term) + 1);

    // Reset read position
    io->read_pos = 0;

    // Capture stderr to verify the error message
    testing::internal::CaptureStderr();

    // Try to load - should fail due to exceeding MAX_SYNONYM_TERMS
    SynonymMap *loadedSmap = (SynonymMap *)SynonymMap_RdbLoad(io, INDEX_CURRENT_VERSION);
    EXPECT_TRUE(loadedSmap == nullptr) << "Expected RDB load to fail when terms exceed limit";

    // Verify the error message was logged to stderr
    std::string stderr_output = testing::internal::GetCapturedStderr();
    std::string expected = "RDB Load: Synonym map size (" +
        std::to_string(dict_size) +
        ") exceeds maximum allowed (" +
        std::to_string(MAX_SYNONYM_TERMS) + ")";
    EXPECT_TRUE(stderr_output.find(expected) != std::string::npos)
        << "Expected: " << expected << ", got: " << stderr_output;
}

TEST_F(RdbMockTest, testTermDataRdbLoadExceedsGroupIdsLimit) {
    // Test that loading term data with more group IDs than MAX_SYNONYM_GROUP_IDS fails
    // The check is in TermData_RdbLoad at the group IDs level

    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Write term string
    const char *term = "test_term";
    RMCK_SaveStringBuffer(io, term, strlen(term) + 1);

    // Write number of group IDs - exceeds the limit
    uint64_t num_group_ids = MAX_SYNONYM_GROUP_IDS + 1;
    RMCK_SaveUnsigned(io, num_group_ids);

    // Write some dummy data to prove failure is from limit check, not EOF
    const char *dummy = "dummy_group";
    RMCK_SaveStringBuffer(io, dummy, strlen(dummy) + 1);

    // Reset read position
    io->read_pos = 0;

    // Capture stderr to verify the error message
    testing::internal::CaptureStderr();

    // Try to load - should fail due to exceeding MAX_SYNONYM_GROUP_IDS
    TermData *td = TermData_RdbLoad(io, INDEX_CURRENT_VERSION);
    EXPECT_TRUE(td == nullptr) << "Expected RDB load to fail when synonym groups exceed limit";

    // Verify the error message was logged to stderr
    std::string stderr_output = testing::internal::GetCapturedStderr();
    std::string expected = "RDB Load: Synonym group IDs (" +
        std::to_string(num_group_ids) +
        ") exceeds maximum allowed (" +
        std::to_string(MAX_SYNONYM_GROUP_IDS) + ")";
    EXPECT_TRUE(stderr_output.find(expected) != std::string::npos)
        << "Expected: " << expected << ", got: " << stderr_output;
}

TEST_F(RdbMockTest, testSynonymMapRdbLoadValidGroupIds) {
    // Test that loading a synonym map with valid group IDs succeeds
    // We manually craft an RDB payload with a valid number of group IDs

    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Write RDB data: dict_size, then for each term: term_string, num_group_ids, group_id_strings...
    uint64_t dict_size = 1;  // One term
    RMCK_SaveUnsigned(io, dict_size);

    // Write term data
    const char *term = "test_term";
    RMCK_SaveStringBuffer(io, term, strlen(term) + 1);  // +1 for null terminator

    // Write number of group IDs
    uint64_t num_group_ids = 2;
    RMCK_SaveUnsigned(io, num_group_ids);

    // Write the actual group ID strings (without the ~ prefix, as that's added during load)
    const char *group1 = "group1";
    const char *group2 = "group2";
    RMCK_SaveStringBuffer(io, group1, strlen(group1) + 1);
    RMCK_SaveStringBuffer(io, group2, strlen(group2) + 1);

    // Reset read position
    io->read_pos = 0;

    // Load should succeed
    SynonymMap *loadedSmap = (SynonymMap *)SynonymMap_RdbLoad(io, INDEX_CURRENT_VERSION);
    ASSERT_TRUE(loadedSmap != nullptr) << "Expected RDB load to succeed with valid group IDs";
    std::unique_ptr<SynonymMap, std::function<void(SynonymMap *)>> loadedSmapPtr(loadedSmap, [](SynonymMap *s) {
        SynonymMap_Free(s);
    });
    EXPECT_EQ(0, RMCK_IsIOError(io));

    // Verify the loaded data
    TermData *td = SynonymMap_GetIdsBySynonym_cstr(loadedSmap, "test_term");
    ASSERT_TRUE(td != nullptr);
    EXPECT_EQ(2, array_len(td->groupIds));
}

TEST_F(RdbMockTest, testTrieRdbLoadMoreThan65535Elements) {
    // Test that a trie with more than 65535 (UINT16_MAX) elements can be
    // saved and loaded correctly from RDB.

    const size_t NUM_ELEMENTS = 65536 + 100;  // Just over UINT16_MAX
    const char *MIDDLE = "_dictionary_entry_with_a_long_string_to_stress_test_the_trie_node_allocation_";

    // Create a trie and populate it with many elements
    Trie *originalTrie = NewTrie(NULL, Trie_Sort_Lex);
    ASSERT_TRUE(originalTrie != nullptr);
    std::unique_ptr<Trie, std::function<void(Trie *)>> originalTriePtr(originalTrie, [](Trie *t) {
        TrieType_Free(t);
    });

    // Insert NUM_ELEMENTS entries using longer strings with number as prefix
    // AND suffix. This creates more unique trie paths instead of one prefix
    // node with many children
    for (size_t i = 0; i < NUM_ELEMENTS; i++) {
        char buf[160];
        snprintf(buf, sizeof(buf), "%zu%s%zu", i, MIDDLE, i);
        Trie_InsertStringBuffer(originalTrie, buf, strlen(buf), 1.0, 0, NULL);
    }

    ASSERT_EQ(NUM_ELEMENTS, originalTrie->size);

    // Create RDB IO context
    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Save the trie to RDB (without payloads, like dictionary)
    TrieType_GenericSave(io, originalTrie, 0);
    EXPECT_EQ(0, RMCK_IsIOError(io));

    // Reset read position to load it back
    io->read_pos = 0;

    // Load the trie from RDB
    Trie *loadedTrie = (Trie *)TrieType_GenericLoad(io, 0);
    ASSERT_TRUE(loadedTrie != nullptr) << "Failed to load trie with more than 65535 elements";
    std::unique_ptr<Trie, std::function<void(Trie *)>> loadedTriePtr(loadedTrie, [](Trie *t) {
        TrieType_Free(t);
    });
    EXPECT_EQ(0, RMCK_IsIOError(io));

    // Verify the loaded trie has the correct size
    EXPECT_EQ(NUM_ELEMENTS, loadedTrie->size);
}

TEST_F(RdbMockTest, testTriePayloadNoOverflow) {
    // Test that triePayload_New correctly handles large payloads without
    // overflow

    // Create RDB IO context
    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Test 1: Normal payload - should succeed
    {
        const char *normal_payload = "This is a normal payload";
        size_t normal_len = strlen(normal_payload);

        // Save a trie entry with normal payload
        RMCK_SaveUnsigned(io, 1);  // 1 element
        RMCK_SaveStringBuffer(io, "test", 5);  // key with null terminator
        RMCK_SaveDouble(io, 1.0);  // score
        RMCK_SaveStringBuffer(io, normal_payload, normal_len + 1);  // payload with null terminator

        io->read_pos = 0;
        Trie *trie = (Trie *)TrieType_GenericLoad(io, 1);  // loadPayloads = 1
        ASSERT_TRUE(trie != nullptr) << "Failed to load trie with normal payload";
        EXPECT_EQ(1, trie->size);
        TrieType_Free(trie);
    }

    // Test 2: Large but valid payload (1MB) - should succeed
    RMCK_ResetRdbIO(io);
    {
        const size_t large_size = 1024 * 1024;  // 1MB
        std::vector<char> large_payload(large_size, 'A');
        large_payload[large_size - 1] = '\0';

        // Save a trie entry with large payload
        RMCK_SaveUnsigned(io, 1);  // 1 element
        RMCK_SaveStringBuffer(io, "large", 6);  // key with null terminator
        RMCK_SaveDouble(io, 1.0);  // score
        RMCK_SaveStringBuffer(io, large_payload.data(), large_size);  // payload with null terminator

        io->read_pos = 0;
        Trie *trie = (Trie *)TrieType_GenericLoad(io, 1);  // loadPayloads = 1
        ASSERT_TRUE(trie != nullptr) << "Failed to load trie with 1MB payload";
        EXPECT_EQ(1, trie->size);
        TrieType_Free(trie);
    }

    // Test 3: Maximum valid uint32_t payload length - should handle gracefully
    // Note: We can't actually allocate UINT32_MAX bytes in a test, but we can
    // verify the RDB load handles the size correctly
    RMCK_ResetRdbIO(io);
    {
        // Create a malicious RDB with a payload length that would cause overflow
        // if not properly checked: UINT32_MAX
        RMCK_SaveUnsigned(io, 1);  // 1 element
        RMCK_SaveStringBuffer(io, "overflow", 9);  // key with null terminator
        RMCK_SaveDouble(io, 1.0);  // score

        // Save a payload length of UINT32_MAX (this would overflow when adding 1)
        RMCK_SaveUnsigned(io, UINT32_MAX);

        io->read_pos = 0;
        Trie *trie = (Trie *)TrieType_GenericLoad(io, 1);  // loadPayloads = 1

        // The load should fail gracefully (return NULL) rather than crash
        // due to integer overflow in the allocation
        EXPECT_TRUE(trie == nullptr) << "Expected RDB load to fail with UINT32_MAX payload length";
    }
}

TEST_F(RdbMockTest, testTriePayloadOverflowDirectInsert) {
    // Test that Trie_InsertRune correctly detects payload overflow
    // when payload length would cause uint32_t overflow in allocation.
    //
    // LIMITATION: This overflow check is impractical to trigger via FT.SUGADD
    // command:
    // - Redis defaults to 512MB max string size (proto-max-bulk-len), though
    //   it is configurable
    // - The overflow requires payload size > ~4GB (UINT32_MAX - sizeof(TriePayload) - 1)
    // - Even if proto-max-bulk-len is increased, allocating ~4GB for a single
    //   suggestion payload would likely fail due to memory constraints before
    //   reaching the overflow check
    //
    // The overflow check exists to protect against:
    // 1. Malicious/corrupted RDB files with crafted payload lengths
    // 2. Internal API misuse where payload.len could be set to an invalid value
    //
    // This test exercises the overflow check directly via
    // Trie_InsertStringBuffer with artificially large payload.len values
    // (without actually allocating memory).

    // Create a trie
    Trie *trie = NewTrie(NULL, Trie_Sort_Score);
    ASSERT_TRUE(trie != nullptr);
    std::unique_ptr<Trie, std::function<void(Trie *)>> triePtr(trie, [](Trie *t) {
        TrieType_Free(t);
    });

    // Test 1: Normal payload insertion should succeed
    {
        const char *key = "normal_key";
        const char *payload_data = "normal_payload";
        RSPayload payload = {
            .data = const_cast<char*>(payload_data),
            .len = strlen(payload_data)
        };

        int rc = Trie_InsertStringBuffer(trie, key, strlen(key), 1.0, 0, &payload);
        EXPECT_EQ(TRIE_OK_NEW, rc) << "Normal payload insertion should succeed";
        EXPECT_EQ(1, trie->size);
    }

    // Test 2: Payload with length that would overflow uint32_t should fail
    // The overflow check is: sizeof(TriePayload) + 1 + plen > UINT32_MAX
    // sizeof(TriePayload) is 4 (for the uint32_t len field)
    // So overflow occurs when plen > UINT32_MAX - 5
    {
        const char *key = "overflow_key";
        char dummy_data = 'X';  // Non-NULL pointer to trigger the overflow check
        RSPayload payload = {
            .data = &dummy_data,
            // This will overflow: 4 + 1 + (UINT32_MAX - 4) > UINT32_MAX
            .len = static_cast<size_t>(UINT32_MAX) - 4
        };

        int rc = Trie_InsertStringBuffer(trie, key, strlen(key), 1.0, 0, &payload);
        EXPECT_EQ(TRIE_ERR_PAYLOAD_OVERFLOW, rc) << "Payload overflow should be detected";
        EXPECT_EQ(1, trie->size) << "Trie size should not change on failed insert";
    }

    // Test 3: Payload with SIZE_MAX length should also fail (extreme case)
    {
        const char *key = "extreme_key";
        char dummy_data = 'Y';
        RSPayload payload = {
            .data = &dummy_data,
            .len = SIZE_MAX
        };

        int rc = Trie_InsertStringBuffer(trie, key, strlen(key), 1.0, 0, &payload);
        EXPECT_EQ(TRIE_ERR_PAYLOAD_OVERFLOW, rc) << "SIZE_MAX payload should trigger overflow";
        EXPECT_EQ(1, trie->size) << "Trie size should not change on failed insert";
    }

}

TEST_F(RdbMockTest, testSchemaPrefixesRdbLoadExceedsLimit) {
    // Test that loading schema rules with more prefixes than
    // MAX_SCHEMA_PREFIXES fails.
    // This exercises the RDB load path in SchemaRule_RdbLoad (src/rules.c).

    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Craft RDB data with prefixes exceeding MAX_SCHEMA_PREFIXES (1024)
    // RDB format for SchemaRule:
    //   1. type string (e.g., "HASH")
    //   2. number of prefixes (uint64)
    //   3. prefix strings...

    // Write type string
    const char *type = "HASH";
    RMCK_SaveStringBuffer(io, type, strlen(type) + 1);

    // Write number of prefixes exceeding the limit
    uint64_t nprefixes = MAX_SCHEMA_PREFIXES + 1;
    RMCK_SaveUnsigned(io, nprefixes);

    // Write some dummy prefix data to prove failure is from limit check, not EOF
    const char *prefix = "prefix:";
    RMCK_SaveStringBuffer(io, prefix, strlen(prefix) + 1);

    // Reset read position
    io->read_pos = 0;

    // Create QueryError to capture the error message
    QueryError status = {QueryErrorCode(0)};

    // Try to load - should fail due to exceeding MAX_SCHEMA_PREFIXES
    // We pass an invalid StrongRef since we expect failure before it's used
    StrongRef dummy_ref = INVALID_STRONG_REF;
    int rc = SchemaRule_RdbLoad(dummy_ref, io, INDEX_CURRENT_VERSION, &status);

    EXPECT_EQ(REDISMODULE_ERR, rc) << "Expected RDB load to fail when prefixes exceed limit";
    EXPECT_TRUE(QueryError_HasError(&status)) << "Expected QueryError to be set";

    // Verify the error message mentions the limit
    const char *err_msg = QueryError_GetUserError(&status);
    EXPECT_TRUE(err_msg != nullptr);
    if (err_msg) {
        std::string expected = "RDB Load: Number of prefixes (" +
            std::to_string(MAX_SCHEMA_PREFIXES + 1) +
            ") exceeds maximum allowed (" +
            std::to_string(MAX_SCHEMA_PREFIXES) + ")";
        EXPECT_TRUE(strstr(err_msg, expected.c_str()) != nullptr)
            << "Expected: " << expected << ", got: " << err_msg;
    }

    QueryError_ClearError(&status);
}

TEST_F(RdbMockTest, testStopWordListRdbLoadExceedsLimit) {
    // Test that loading a stopword list with more elements than
    // MAX_STOPWORDLIST_SIZE fails.
    // This exercises the RDB load path in StopWordList_RdbLoad (src/stopwords.c).

    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Craft RDB data with elements exceeding MAX_STOPWORDLIST_SIZE
    // RDB format for StopWordList:
    //   1. number of elements (uint64)
    //   2. element strings...

    // Write number of elements exceeding the limit
    uint64_t num_elements = MAX_STOPWORDLIST_SIZE + 1;
    RMCK_SaveUnsigned(io, num_elements);

    // Write some dummy stopword data to prove failure is from limit check, not EOF
    const char *stopword = "the";
    RMCK_SaveStringBuffer(io, stopword, strlen(stopword));

    // Reset read position
    io->read_pos = 0;

    // Capture stderr to verify the error message
    testing::internal::CaptureStderr();

    // Try to load - should fail due to exceeding MAX_STOPWORDLIST_SIZE
    StopWordList *loadedList = StopWordList_RdbLoad(io, INDEX_CURRENT_VERSION);
    EXPECT_TRUE(loadedList == nullptr) << "Expected RDB load to fail when stopwords exceed limit";

    // Verify the error message was logged to stderr
    std::string stderr_output = testing::internal::GetCapturedStderr();
    std::string expected = "RDB Load: Stopword list size (" +
        std::to_string(num_elements) +
        ") exceeds maximum allowed (" +
        std::to_string(MAX_STOPWORDLIST_SIZE) + ")";
    EXPECT_TRUE(stderr_output.find(expected) != std::string::npos)
        << "Expected: " << expected << ", got: " << stderr_output;
}


TEST_F(RdbMockTest, testDocTableLegacyRdbLoadOverflow) {
    // Test that DocTable_LegacyRdbLoad correctly handles allocation overflow
    // when maxDocId > maxSize and the allocation would overflow.
    //
    // The overflow check is: t->cap * sizeof(DMDChain) > SIZE_MAX
    // We craft RDB data with a maxSize that would cause this overflow.

    RedisModuleIO *io = RMCK_CreateRdbIO();
    std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
        RMCK_FreeRdbIO(io);
    });
    ASSERT_TRUE(io != nullptr);

    // Create a DocTable with initial small capacity
    // Use NewDocTable directly to avoid RSGlobalConfig dependency
    DocTable t = NewDocTable(16, 1000000);
    std::unique_ptr<DocTable, std::function<void(DocTable *)>> tablePtr(&t, [](DocTable *t) {
        DocTable_Free(t);
    });

    // Craft RDB data that will trigger the overflow check:
    // - size: 2 (at least 1 document to load)
    // - maxDocId: SIZE_MAX (very large, will be > maxSize)
    // - maxSize: (SIZE_MAX / sizeof(DMDChain)) + 1 (will cause overflow in allocation)
    //
    // RDB format for legacy DocTable (encver >= INDEX_MIN_COMPACTED_DOCTABLE_VERSION):
    //   1. size (uint64)
    //   2. maxDocId (uint64)
    //   3. maxSize (uint64)
    //   4. document entries...

    uint64_t size = 2;
    uint64_t maxDocId = SIZE_MAX;
    // Calculate a maxSize that will cause overflow: cap * sizeof(DMDChain) > SIZE_MAX
    // sizeof(DMDChain) is typically 8 bytes (pointer to DLLIST2)
    uint64_t maxSize = (SIZE_MAX / sizeof(DMDChain)) + 1;

    RMCK_SaveUnsigned(io, size);
    RMCK_SaveUnsigned(io, maxDocId);
    RMCK_SaveUnsigned(io, maxSize);

    // Write some dummy document data (won't be reached due to early return)
    const char *docKey = "doc:1";
    RMCK_SaveStringBuffer(io, docKey, strlen(docKey));

    // Reset read position
    io->read_pos = 0;

    // Capture stderr to verify the error message
    testing::internal::CaptureStderr();

    // Try to load - should fail due to allocation overflow
    int rc = DocTable_LegacyRdbLoad(&t, io, INDEX_MIN_COMPACTED_DOCTABLE_VERSION);
    EXPECT_EQ(REDISMODULE_ERR, rc) << "Expected DocTable_LegacyRdbLoad to fail on allocation overflow";

    // Verify the error message was logged
    std::string stderr_output = testing::internal::GetCapturedStderr();
    EXPECT_TRUE(stderr_output.find("DocTable_LegacyRdbLoad: allocation overflow") != std::string::npos)
        << "Expected overflow error message, got: " << stderr_output;

    // Verify the DocTable is in a safe state after failure
    // buckets should be NULL and cap should be 0
    EXPECT_TRUE(t.buckets == nullptr) << "buckets should be NULL after overflow error";
    EXPECT_EQ(0, t.cap) << "cap should be 0 after overflow error";
}
