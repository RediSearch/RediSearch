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

#include <vector>
#include <cstring>

class AREQTest : public ::testing::Test {
protected:
    RedisModuleCtx* ctx = nullptr;

    void SetUp() override {
        ctx = RedisModule_GetThreadSafeContext(NULL);
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
        }
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

        // Calculate required buffer size using the API
        size_t buffer_size = RedisModuleSlotRangeArray_SerializedSize_Binary(ranges.size());
        std::vector<uint8_t> data(buffer_size);

        // Serialize using the API
        bool success = RedisModuleSlotRangeArray_SerializeBinary(slot_array, data.data(), buffer_size);

        // Clean up
        rm_free(slot_array);

        if (!success) {
            data.clear();
        }

        return data;
    }

    // Helper function to create a RedisModuleString from binary data
    RedisModuleString* createBinaryString(const std::vector<uint8_t>& data) {
        return RedisModule_CreateString(ctx, reinterpret_cast<const char*>(data.data()), data.size());
    }
};

// Test binary slot range parsing with multiple ranges
TEST_F(AREQTest, testBinarySlotRangeParsing) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    QueryError status = QueryError_Default();

    // Create test slot ranges
    std::vector<std::pair<uint16_t, uint16_t>> ranges = {{0, 5460}, {5461, 10922}, {10923, 16383}};
    auto binary_data = createBinarySlotRangeData(ranges);
    ASSERT_FALSE(binary_data.empty()) << "Binary data creation should succeed";

    // Create argument list
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_BINARY", strlen("_RANGE_SLOTS_BINARY")));
    argv.push_back(createBinaryString(binary_data));

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_OK) << "AREQ_Compile should succeed";
    EXPECT_FALSE(QueryError_HasError(&status)) << "Should not have query error";

    // Verify that coordSlotRanges was set correctly
    EXPECT_NE(req->coordSlotRanges, nullptr) << "coordSlotRanges should be set";
    EXPECT_EQ(req->coordSlotRanges->num_ranges, 3) << "Should have 3 ranges";
    EXPECT_EQ(req->coordSlotRanges->ranges[0].start, 0) << "First range start should be 0";
    EXPECT_EQ(req->coordSlotRanges->ranges[0].end, 5460) << "First range end should be 5460";
    EXPECT_EQ(req->coordSlotRanges->ranges[1].start, 5461) << "Second range start should be 5461";
    EXPECT_EQ(req->coordSlotRanges->ranges[1].end, 10922) << "Second range end should be 10922";
    EXPECT_EQ(req->coordSlotRanges->ranges[2].start, 10923) << "Third range start should be 10923";
    EXPECT_EQ(req->coordSlotRanges->ranges[2].end, 16383) << "Third range end should be 16383";

    // Clean up
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}

// Test binary slot range parsing with single range
TEST_F(AREQTest, testBinarySlotRangeParsingSingleRange) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    QueryError status = QueryError_Default();

    // Create test slot range - single range covering all slots
    std::vector<std::pair<uint16_t, uint16_t>> ranges = {{0, 16383}};
    auto binary_data = createBinarySlotRangeData(ranges);
    ASSERT_FALSE(binary_data.empty()) << "Binary data creation should succeed";

    // Create argument list
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_BINARY", strlen("_RANGE_SLOTS_BINARY")));
    argv.push_back(createBinaryString(binary_data));

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_OK) << "AREQ_Compile should succeed";
    EXPECT_FALSE(QueryError_HasError(&status)) << "Should not have query error";

    // Verify that coordSlotRanges was set correctly
    EXPECT_NE(req->coordSlotRanges, nullptr) << "coordSlotRanges should be set";
    EXPECT_EQ(req->coordSlotRanges->num_ranges, 1) << "Should have 1 range";
    EXPECT_EQ(req->coordSlotRanges->ranges[0].start, 0) << "Range start should be 0";
    EXPECT_EQ(req->coordSlotRanges->ranges[0].end, 16383) << "Range end should be 16383";

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

    QueryError status = QueryError_Default();

    // Create argument list with missing binary data
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_BINARY", strlen("_RANGE_SLOTS_BINARY")));

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

// Test human-readable slot range parsing with multiple ranges
TEST_F(AREQTest, testHumanReadableSlotRangeParsing) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    QueryError status = QueryError_Default();

    // Create argument list with human-readable slot ranges
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_HR", strlen("_RANGE_SLOTS_HR")));
    argv.push_back(RedisModule_CreateString(ctx, "3", 1));      // num_ranges
    argv.push_back(RedisModule_CreateString(ctx, "0", 1));      // start1
    argv.push_back(RedisModule_CreateString(ctx, "5460", 4));   // end1
    argv.push_back(RedisModule_CreateString(ctx, "5461", 4));   // start2
    argv.push_back(RedisModule_CreateString(ctx, "10922", 5));  // end2
    argv.push_back(RedisModule_CreateString(ctx, "10923", 5));  // start3
    argv.push_back(RedisModule_CreateString(ctx, "16383", 5));  // end3

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_OK) << "AREQ_Compile should succeed";
    EXPECT_FALSE(QueryError_HasError(&status)) << "Should not have query error";
    EXPECT_NE(req->coordSlotRanges, nullptr) << "coordSlotRanges should be set";
    EXPECT_EQ(req->coordSlotRanges->num_ranges, 3) << "Should have 3 ranges";

    // Verify first range
    EXPECT_EQ(req->coordSlotRanges->ranges[0].start, 0) << "First range start should be 0";
    EXPECT_EQ(req->coordSlotRanges->ranges[0].end, 5460) << "First range end should be 5460";

    // Verify second range
    EXPECT_EQ(req->coordSlotRanges->ranges[1].start, 5461) << "Second range start should be 5461";
    EXPECT_EQ(req->coordSlotRanges->ranges[1].end, 10922) << "Second range end should be 10922";

    // Verify third range
    EXPECT_EQ(req->coordSlotRanges->ranges[2].start, 10923) << "Third range start should be 10923";
    EXPECT_EQ(req->coordSlotRanges->ranges[2].end, 16383) << "Third range end should be 16383";

    // Clean up
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}

// Test human-readable slot range parsing with single range
TEST_F(AREQTest, testHumanReadableSlotRangeParsingSingleRange) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    QueryError status = QueryError_Default();

    // Create argument list with single human-readable slot range
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_HR", strlen("_RANGE_SLOTS_HR")));
    argv.push_back(RedisModule_CreateString(ctx, "1", 1));      // num_ranges
    argv.push_back(RedisModule_CreateString(ctx, "0", 1));      // start1
    argv.push_back(RedisModule_CreateString(ctx, "16383", 5));  // end1

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_OK) << "AREQ_Compile should succeed";
    EXPECT_FALSE(QueryError_HasError(&status)) << "Should not have query error";
    EXPECT_NE(req->coordSlotRanges, nullptr) << "coordSlotRanges should be set";
    EXPECT_EQ(req->coordSlotRanges->num_ranges, 1) << "Should have 1 range";
    EXPECT_EQ(req->coordSlotRanges->ranges[0].start, 0) << "Range start should be 0";
    EXPECT_EQ(req->coordSlotRanges->ranges[0].end, 16383) << "Range end should be 16383";

    // Clean up
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}

// Test human-readable slot range parsing with invalid range (start > end)
TEST_F(AREQTest, testHumanReadableSlotRangeInvalidRange) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    QueryError status = QueryError_Default();

    // Create argument list with invalid range (start > end)
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_HR", strlen("_RANGE_SLOTS_HR")));
    argv.push_back(RedisModule_CreateString(ctx, "1", 1));      // num_ranges
    argv.push_back(RedisModule_CreateString(ctx, "200", 3));    // start (invalid - greater than end)
    argv.push_back(RedisModule_CreateString(ctx, "100", 3));    // end

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_ERR) << "AREQ_Compile should fail with invalid range";
    EXPECT_TRUE(QueryError_HasError(&status)) << "Should have query error";

    const char* error_msg = QueryError_GetUserError(&status);
    EXPECT_NE(error_msg, nullptr) << "Error message should be set";
    EXPECT_TRUE(strstr(error_msg, "start slot must be <= end slot") != nullptr)
        << "Error message should mention invalid range";

    // Clean up
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}

// Test that using both _RANGE_SLOTS_BINARY and _RANGE_SLOTS_HR together fails
TEST_F(AREQTest, testConflictingSlotRangeFormats) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    QueryError status = QueryError_Default();

    // Create binary slot range data
    std::vector<std::pair<uint16_t, uint16_t>> ranges = {{100, 200}};
    auto binary_data = createBinarySlotRangeData(ranges);
    ASSERT_FALSE(binary_data.empty()) << "Binary data creation should succeed";

    // Create argument list with both binary and HR formats
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query

    // First add binary format
    argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_BINARY", strlen("_RANGE_SLOTS_BINARY")));
    argv.push_back(createBinaryString(binary_data));

    // Then add HR format (this should cause conflict)
    argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_HR", strlen("_RANGE_SLOTS_HR")));
    argv.push_back(RedisModule_CreateString(ctx, "1", 1));      // num_ranges
    argv.push_back(RedisModule_CreateString(ctx, "300", 3));    // start
    argv.push_back(RedisModule_CreateString(ctx, "400", 3));    // end

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_ERR) << "AREQ_Compile should fail with conflicting formats";
    EXPECT_TRUE(QueryError_HasError(&status)) << "Should have query error";

    const char* error_msg = QueryError_GetUserError(&status);
    EXPECT_NE(error_msg, nullptr) << "Error message should be set";
    EXPECT_TRUE(strstr(error_msg, "Cannot specify both _RANGE_SLOTS_BINARY and _RANGE_SLOTS_HR") != nullptr)
        << "Error message should mention conflicting formats";

    // Clean up
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}


// Test that using both _RANGE_SLOTS_BINARY and _RANGE_SLOTS_HR together fails
TEST_F(AREQTest, testConflictingSlotRangeFormatsReversed) {
  AREQ* req = AREQ_New();
  ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

  QueryError status = QueryError_Default();

  // Create binary slot range data
  std::vector<std::pair<uint16_t, uint16_t>> ranges = {{100, 200}};
  auto binary_data = createBinarySlotRangeData(ranges);
  ASSERT_FALSE(binary_data.empty()) << "Binary data creation should succeed";

  // Create argument list with both binary and HR formats
  std::vector<RedisModuleString*> argv;
  argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query

  // First add HR format (this should cause conflict)
  argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_HR", strlen("_RANGE_SLOTS_HR")));
  argv.push_back(RedisModule_CreateString(ctx, "1", 1));      // num_ranges
  argv.push_back(RedisModule_CreateString(ctx, "300", 3));    // start
  argv.push_back(RedisModule_CreateString(ctx, "400", 3));    // end

  // Then add binary format
  argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_BINARY", strlen("_RANGE_SLOTS_BINARY")));
  argv.push_back(createBinaryString(binary_data));

  // Test AREQ_Compile
  int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

  EXPECT_EQ(result, REDISMODULE_ERR) << "AREQ_Compile should fail with conflicting formats";
  EXPECT_TRUE(QueryError_HasError(&status)) << "Should have query error";

  const char* error_msg = QueryError_GetUserError(&status);
  EXPECT_NE(error_msg, nullptr) << "Error message should be set";
  EXPECT_TRUE(strstr(error_msg, "Cannot specify both _RANGE_SLOTS_BINARY and _RANGE_SLOTS_HR") != nullptr)
      << "Error message should mention conflicting formats";

  // Clean up
  for (auto* str : argv) {
      RedisModule_FreeString(ctx, str);
  }
  QueryError_ClearError(&status);
  AREQ_Free(req);
}

// Test human-readable slot range parsing with insufficient arguments
TEST_F(AREQTest, testHumanReadableSlotRangeInsufficientArgs) {
    AREQ* req = AREQ_New();
    ASSERT_NE(req, nullptr) << "AREQ_New should return a valid pointer";

    QueryError status = QueryError_Default();

    // Create argument list with insufficient arguments (missing range values)
    std::vector<RedisModuleString*> argv;
    argv.push_back(RedisModule_CreateString(ctx, "hello", 5));  // query
    argv.push_back(RedisModule_CreateString(ctx, "_RANGE_SLOTS_HR", strlen("_RANGE_SLOTS_HR")));
    argv.push_back(RedisModule_CreateString(ctx, "2", 1));      // num_ranges (expects 4 more args)
    argv.push_back(RedisModule_CreateString(ctx, "100", 3));    // start1
    // Missing end1, start2, end2

    // Test AREQ_Compile
    int result = AREQ_Compile(req, argv.data(), argv.size(), &status);

    EXPECT_EQ(result, REDISMODULE_ERR) << "AREQ_Compile should fail with insufficient arguments";
    EXPECT_TRUE(QueryError_HasError(&status)) << "Should have query error";

    const char* error_msg = QueryError_GetUserError(&status);
    EXPECT_NE(error_msg, nullptr) << "Error message should be set";
    EXPECT_TRUE(strstr(error_msg, "insufficient arguments for ranges") != nullptr)
        << "Error message should mention insufficient arguments";

    // Clean up
    for (auto* str : argv) {
        RedisModule_FreeString(ctx, str);
    }
    QueryError_ClearError(&status);
    AREQ_Free(req);
}
