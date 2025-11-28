/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "aggregate/aggregate.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "slot_ranges.h"
#include "query_error.h"
#include "rmalloc.h"
#include "module.h"
#include "redismodule.h"
#include "asm_state_machine.h"

#include <vector>
#include <cstring>

class AREQTest : public ::testing::Test {
protected:
    RedisModuleCtx* ctx = nullptr;
    RedisModuleSlotRangeArray* local_slots = nullptr;

    void SetUp() override {
        ctx = RedisModule_GetThreadSafeContext(NULL);
        ASM_StateMachine_Init();
        local_slots = createSlotRangeArray(0, 16383);
        ASM_StateMachine_SetLocalSlots(local_slots);
        // Just assume all slots are local for testing
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
            ASM_StateMachine_End();
        }
        if (local_slots) {
            rm_free(local_slots);
        }
    }

    // Helper function to create a RedisModuleSlotRangeArray for testing
    RedisModuleSlotRangeArray* createSlotRangeArray(uint16_t start, uint16_t end) {
        size_t array_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange);
        RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)rm_malloc(array_size);
        array->num_ranges = 1;
        array->ranges[0].start = start;
        array->ranges[0].end = end;
        return array;
    }

    // Helper function to create binary slot range data using the Serialization API
    std::vector<uint8_t> createBinarySlotRangeData(const std::vector<std::pair<uint16_t, uint16_t>>& ranges) {
        // Create a RedisModuleSlotRangeArray
        size_t total_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * ranges.size();
        RedisModuleSlotRangeArray* slot_array = (RedisModuleSlotRangeArray*)rm_malloc(total_size);
        slot_array->num_ranges = ranges.size();

        // Fill in the ranges
        for (size_t i = 0; i < ranges.size(); i++) {
            slot_array->ranges[i].start = ranges[i].first;
            slot_array->ranges[i].end = ranges[i].second;
        }

        // Serialize using the API
        char *serialized = SlotRangesArray_Serialize(slot_array);

        size_t buffer_size = SlotRangeArray_SizeOf(ranges.size());
        std::vector<uint8_t> data(serialized, serialized + buffer_size);

        // Clean up
        rm_free(slot_array);
        rm_free(serialized);
        return data;
    }

    // Helper function to create a RedisModuleString from binary data
    RedisModuleString* createBinaryString(const std::vector<uint8_t>& data) {
        return RedisModule_CreateString(ctx, reinterpret_cast<const char*>(data.data()), data.size());
    }

    // Helper function to compare two slot range arrays exactly (borrowed from test_slot_ranges)
    bool compareExactly(const RedisModuleSlotRangeArray* a, const RedisModuleSlotRangeArray* b) {
        if (a->num_ranges != b->num_ranges) {
            return false;
        }
        for (int i = 0; i < a->num_ranges; i++) {
            if (a->ranges[i].start != b->ranges[i].start || a->ranges[i].end != b->ranges[i].end) {
                return false;
            }
        }
        return true;
    }
};

// Parameterized test data structure
struct SlotRangeTestData {
    std::vector<std::pair<uint16_t, uint16_t>> ranges;
    std::string description;
};

// Parameterized test class
class AREQBinarySlotRangeTest : public AREQTest, public ::testing::WithParamInterface<SlotRangeTestData> {};

// Test binary slot range parsing with different ranges (parameterized)
TEST_P(AREQBinarySlotRangeTest, testBinarySlotRangeParsing) {
    const auto& test_data = GetParam();

    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    // Mark req as internal to bypass checks
    req->reqflags = QEXEC_F_INTERNAL;

    QueryError status = QueryError_Default();

    // Create test slot ranges from parameter
    auto binary_data = createBinarySlotRangeData(test_data.ranges);
    ASSERT_FALSE(binary_data.empty()) << "Binary data creation should succeed for: " << test_data.description;

    // Create argument list
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, SLOTS_STR, strlen(SLOTS_STR)));
    argv.push_back(createBinaryString(binary_data));

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_OK) << "AREQ_Compile should succeed for: " << test_data.description;
    EXPECT_FALSE(QueryError_HasError(&status)) << "Should not have query error for: " << test_data.description;

    // Verify that querySlots was set correctly
    EXPECT_NE(req->querySlots, nullptr) << "querySlots should be set for: " << test_data.description;
    EXPECT_EQ(req->querySlots->num_ranges, test_data.ranges.size()) << "Should have " << test_data.ranges.size() << " ranges for: " << test_data.description;

    // Create expected slot range array for comparison
    size_t total_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * test_data.ranges.size();
    RedisModuleSlotRangeArray* expected = (RedisModuleSlotRangeArray*)rm_malloc(total_size);
    expected->num_ranges = test_data.ranges.size();
    for (size_t i = 0; i < test_data.ranges.size(); i++) {
        expected->ranges[i].start = test_data.ranges[i].first;
        expected->ranges[i].end = test_data.ranges[i].second;
    }

    // Use exact comparison from test_slot_ranges
    EXPECT_TRUE(compareExactly(expected, req->querySlots)) << "Slot ranges should match exactly for: " << test_data.description;

    // Clean up
    rm_free(expected);
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}

// Test data for parameterized tests - includes ranges that create null bytes in binary data
INSTANTIATE_TEST_SUITE_P(
    BinarySlotRangeVariations,
    AREQBinarySlotRangeTest,
    ::testing::Values(
        // Original test case - single_full_range
        SlotRangeTestData{{{0, 16383}}, "single_full_range"},

        // Original test case - standard cluster ranges
        SlotRangeTestData{{{0, 5460}, {5462, 10922}, {10924, 16383}}, "almost_full_range"},

        // Single range
        SlotRangeTestData{{{0, 5460}}, "single_partial_range"},


        // Range 0-255 creates 0x0000 0x00FF in binary (2 null bytes at start)
        SlotRangeTestData{{{0, 254}, {256, 511}}, "ranges_starting_with_zero"},

        // Range 256-256 creates 0x0100 0x0100 in binary (null byte in middle)
        SlotRangeTestData{{{256, 256}, {512, 512}}, "ranges_with_embedded_nulls"},

        // Multiple ranges with various null byte patterns
        SlotRangeTestData{{{0, 0}, {256, 256}, {512, 768}, {1024, 1024}}, "mixed_null_byte_patterns"},

        // Edge case: maximum slot values
        SlotRangeTestData{{{16383, 16383}}, "max_slot_range"},

        // Ranges that create 0x0000 patterns in different positions
        SlotRangeTestData{{{0, 256}, {512, 768}, {1024, 1280}}, "ranges_creating_null_sequences"},

        // Small ranges with potential null bytes
        SlotRangeTestData{{{0, 1}, {3, 4}, {6, 7}}, "small_ranges_with_null_potential"},

        // Ranges where end values create null bytes (e.g., 256 = 0x0100)
        SlotRangeTestData{{{100, 256}, {300, 512}, {600, 768}}, "ranges_ending_with_null_patterns"}
    )
);

// Test binary slot range parsing with single range
TEST_F(AREQTest, testBinarySlotRangeParsingSingleRange) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    // Mark req as internal to bypass checks
    req->reqflags = QEXEC_F_INTERNAL;

    QueryError status = QueryError_Default();

    // Create test slot range - single range covering all slots
    std::vector<std::pair<uint16_t, uint16_t>> ranges = {{0, 16383}};
    auto binary_data = createBinarySlotRangeData(ranges);
    ASSERT_FALSE(binary_data.empty()) << "Binary data creation should succeed";

    // Create argument list
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, SLOTS_STR, strlen(SLOTS_STR)));
    argv.push_back(createBinaryString(binary_data));

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_OK) << "AREQ_Compile should succeed";
    EXPECT_FALSE(QueryError_HasError(&status)) << "Should not have query error";

    // Verify that querySlots was set correctly
    EXPECT_NE(req->querySlots, nullptr) << "querySlots should be set";
    EXPECT_EQ(req->querySlots->num_ranges, 1) << "Should have 1 range";
    EXPECT_EQ(req->querySlots->ranges[0].start, 0) << "Range start should be 0";
    EXPECT_EQ(req->querySlots->ranges[0].end, 16383) << "Range end should be 16383";

    // Clean up
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}

// Test error handling for insufficient arguments
TEST_F(AREQTest, testBinarySlotRangeInsufficientArgs) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    // Mark req as internal to bypass checks
    req->reqflags = QEXEC_F_INTERNAL;

    QueryError status = QueryError_Default();

    // Create argument list with missing binary data
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, SLOTS_STR, strlen(SLOTS_STR)));

    // Test AREQ_Compile - should fail due to insufficient arguments
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_ERR) << "AREQ_Compile should fail with insufficient arguments";
    EXPECT_TRUE(QueryError_HasError(&status)) << "Should have query error";

    // Clean up
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}

// Test complex aggregate query with cursor, scorer, and slot ranges
TEST_F(AREQTest, testComplexAggregateWithCursorAndSlotRanges) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    // Mark req as internal to bypass checks
    req->reqflags = QEXEC_F_INTERNAL;

    QueryError status = QueryError_Default();

    // Create argument list matching the MRCommand
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello world", 11));        // query
    argv.push_back(RedisModule_CreateString(ctx, "WITHCURSOR", 10));
    argv.push_back(RedisModule_CreateString(ctx, "_NUM_SSTRING", 12));
    argv.push_back(RedisModule_CreateString(ctx, "_INDEX_PREFIXES", 15));
    argv.push_back(RedisModule_CreateString(ctx, "1", 1));
    argv.push_back(RedisModule_CreateString(ctx, "", 0));                    // empty prefix
    argv.push_back(RedisModule_CreateString(ctx, SLOTS_STR, strlen(SLOTS_STR)));
    argv.push_back(createBinaryString(createBinarySlotRangeData({{5462, 10923}}))); // slot ranges
    argv.push_back(RedisModule_CreateString(ctx, "SCORER", 6));
    argv.push_back(RedisModule_CreateString(ctx, "BM25STD", 7));
    argv.push_back(RedisModule_CreateString(ctx, "ADDSCORES", 9));
    argv.push_back(RedisModule_CreateString(ctx, "LOAD", 4));
    argv.push_back(RedisModule_CreateString(ctx, "2", 1));
    argv.push_back(RedisModule_CreateString(ctx, "@__key", 6));
    argv.push_back(RedisModule_CreateString(ctx, "@__score", 8));

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_OK) << "AREQ_Compile should succeed";
    EXPECT_FALSE(QueryError_HasError(&status)) << "Should not have query error";

    // Verify slot ranges were set
    EXPECT_NE(req->querySlots, nullptr) << "querySlots should be set";
    EXPECT_EQ(req->querySlots->num_ranges, 1) << "Should have 1 range";
    EXPECT_EQ(req->querySlots->ranges[0].start, 5462) << "Range start should be 5462";
    EXPECT_EQ(req->querySlots->ranges[0].end, 10923) << "Range end should be 10923";

    // Clean up
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}
