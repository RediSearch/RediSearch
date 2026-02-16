/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include <cstdlib>
#include <cstring>
#include <vector>

#include "rmalloc.h"
#include "src/slot_ranges.h"
#include "redismock/redismock.h"
#include "redismock/internal.h"

class SlotRangesTest : public ::testing::Test {
protected:
    // Helper function to create a RedisModuleSlotRangeArray for testing
    RedisModuleSlotRangeArray* createSlotRangeArray(const std::vector<std::pair<uint16_t, uint16_t>>& ranges) {
        // Allocate memory for the struct plus the flexible array member
        size_t total_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * ranges.size();
        RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)rm_malloc(total_size);
        array->num_ranges = ranges.size();

        for (size_t i = 0; i < ranges.size(); i++) {
            array->ranges[i].start = ranges[i].first;
            array->ranges[i].end = ranges[i].second;
        }

        return array;
    }

    void freeSlotRangeArray(RedisModuleSlotRangeArray* array) {
        rm_free(array);
    }

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

// Test basic binary serialization and deserialization
TEST_F(SlotRangesTest, testBinarySerializationBasic) {
    // Test with a simple single range
    auto original = createSlotRangeArray({{100, 200}});

    // Calculate required buffer size
    size_t size = SlotRangeArray_SizeOf(1);
    EXPECT_EQ(size, sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange)); // header + one range

    // Serialize
    char *result = SlotRangesArray_Serialize(original);
    ASSERT_NE(result, nullptr);

    // Deserialize
    auto deserialized = SlotRangesArray_Deserialize(result, size);
    ASSERT_NE(deserialized, nullptr);

    EXPECT_TRUE(compareExactly(original, deserialized));

    rm_free(result);
    freeSlotRangeArray(original);
    freeSlotRangeArray(deserialized);
}

// Test binary serialization with multiple ranges
TEST_F(SlotRangesTest, testBinarySerializationMultipleRanges) {
    // Test with multiple ranges
    auto* original = createSlotRangeArray({{0, 100}, {500, 600}, {1000, 1500}});

    // Calculate required buffer size
    size_t size = SlotRangeArray_SizeOf(3);

    // Serialize
    char *result = SlotRangesArray_Serialize(original);
    ASSERT_NE(result, nullptr);

    // Deserialize
    auto* deserialized = SlotRangesArray_Deserialize(result, size);
    ASSERT_NE(deserialized, nullptr);

    EXPECT_TRUE(compareExactly(original, deserialized));

    rm_free(result);
    freeSlotRangeArray(original);
    freeSlotRangeArray(deserialized);
}

// Test binary deserialization with invalid data
TEST_F(SlotRangesTest, testBinaryDeserializationInvalidData) {
    struct {
        uint32_t num_ranges;
        RedisModuleSlotRange ranges[5]; // enough for 5 ranges for this test
    } array_buf;
    RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)&array_buf;

    // Test with corrupted/invalid serialized data

    // Case 1: Buffer too small to contain header
    auto result = SlotRangesArray_Deserialize((char*)array, sizeof(array->num_ranges) - 1);
    EXPECT_EQ(result, nullptr);

    // Case 2: Buffer too small to contain declared ranges
    array->num_ranges = 3; // Declare 3 ranges
    result = SlotRangesArray_Deserialize((char*)array, SlotRangeArray_SizeOf(2)); // Only enough for 2 ranges
    EXPECT_FALSE(result);

    // Case 3: Buffer too large (corrupted)
    array->num_ranges = 2; // Declare 2 ranges
    result = SlotRangesArray_Deserialize((char*)array, SlotRangeArray_SizeOf(3)); // Buffer size for 3 ranges
    EXPECT_FALSE(result);
}

// Test binary serialization with many ranges
TEST_F(SlotRangesTest, testBinarySerializationManyRanges) {
    // Test with a large number of ranges
    std::vector<std::pair<uint16_t, uint16_t>> ranges;
    for (int i = 0; i < 100; i++) {
        ranges.push_back({static_cast<uint16_t>(i * 100), static_cast<uint16_t>(i * 100 + 50)});
    }

    auto* original = createSlotRangeArray(ranges);

    // Calculate required buffer size
    size_t size = SlotRangeArray_SizeOf(100);
    EXPECT_EQ(size, 404); // 4 bytes for header + 400 bytes for 100 ranges (4 bytes each)

    // Serialize
    char *result = SlotRangesArray_Serialize(original);
    ASSERT_NE(result, nullptr);

    // Deserialize
    auto* deserialized = SlotRangesArray_Deserialize(result, size);
    ASSERT_NE(deserialized, nullptr);

    EXPECT_TRUE(compareExactly(original, deserialized));

    rm_free(result);
    freeSlotRangeArray(original);
    freeSlotRangeArray(deserialized);
}

// Test binary serialization with extreme values
TEST_F(SlotRangesTest, testBinarySerializationExtremeValues) {
    // Test with extreme uint16_t values
    auto* original = createSlotRangeArray({
        {0, 0},                    // Minimum values
        {0, 65535},                // Full range
        {65535, 65535},            // Maximum values
        {32767, 32768},            // Around middle
        {1, 2},                    // Small consecutive values
        {65534, 65535}             // Maximum consecutive values
    });

    // Calculate required buffer size
    size_t size = SlotRangeArray_SizeOf(6);

    // Serialize
    char *result = SlotRangesArray_Serialize(original);
    ASSERT_NE(result, nullptr);

    // Deserialize
    auto* deserialized = SlotRangesArray_Deserialize(result, size);
    ASSERT_NE(deserialized, nullptr);

    EXPECT_TRUE(compareExactly(original, deserialized));

    rm_free(result);
    freeSlotRangeArray(original);
    freeSlotRangeArray(deserialized);
}

// Test binary serialization with very large number of ranges
TEST_F(SlotRangesTest, testBinarySerializationVeryManyRanges) {
    // Test with 1000 ranges to stress test the serialization
    std::vector<std::pair<uint16_t, uint16_t>> ranges;
    for (int i = 0; i < 1000; i++) {
        // Create non-overlapping ranges across the full uint16_t space
        uint16_t start = static_cast<uint16_t>((i * 65) % 65536);
        uint16_t end = static_cast<uint16_t>(start + (i % 10) + 1);
        if (end < start) end = 65535; // Handle overflow
        ranges.push_back({start, end});
    }

    auto* original = createSlotRangeArray(ranges);

    // Calculate required buffer size
    size_t size = SlotRangeArray_SizeOf(1000);

    // Serialize
    char *result = SlotRangesArray_Serialize(original);
    ASSERT_NE(result, nullptr);

    // Deserialize
    auto* deserialized = SlotRangesArray_Deserialize(result, size);
    ASSERT_NE(deserialized, nullptr);

    EXPECT_TRUE(compareExactly(original, deserialized));

    rm_free(result);
    freeSlotRangeArray(original);
    freeSlotRangeArray(deserialized);
}

// Test with Redis cluster slot ranges (0-16383)
TEST_F(SlotRangesTest, testRedisClusterSlotRanges) {
    // Test with typical Redis cluster slot assignments
    auto* original = createSlotRangeArray({
        {0, 5460},        // Node 1: slots 0-5460
        {5461, 10922},    // Node 2: slots 5461-10922
        {10923, 16383}    // Node 3: slots 10923-16383
    });

    // Calculate required buffer size
    size_t size = SlotRangeArray_SizeOf(3);

    // Serialize
    char *result = SlotRangesArray_Serialize(original);
    ASSERT_NE(result, nullptr);

    // Deserialize
    auto* deserialized = SlotRangesArray_Deserialize(result, size);
    ASSERT_NE(deserialized, nullptr);

    EXPECT_TRUE(compareExactly(original, deserialized));

    rm_free(result);
    freeSlotRangeArray(original);
    freeSlotRangeArray(deserialized);
}

// Test Slots_CanAccessKeysInSlot function
TEST_F(SlotRangesTest, testSlotsCanAccessKeysInSlot) {
    // Test with single range
    auto* singleRange = createSlotRangeArray({{100, 200}});

    // Test slots within range
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(singleRange, 100));  // Start boundary
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(singleRange, 150));  // Middle
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(singleRange, 200));  // End boundary

    // Test slots outside range
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(singleRange, 99));   // Just before start
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(singleRange, 201));  // Just after end
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(singleRange, 0));    // Far before
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(singleRange, 65535)); // Far after

    freeSlotRangeArray(singleRange);

    // Test with multiple ranges
    auto* multipleRanges = createSlotRangeArray({
        {0, 100},      // Range 1: 0-100
        {500, 600},    // Range 2: 500-600
        {1000, 1500}   // Range 3: 1000-1500
    });

    // Test slots within each range
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(multipleRanges, 0));     // Range 1 start
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(multipleRanges, 50));    // Range 1 middle
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(multipleRanges, 100));   // Range 1 end
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(multipleRanges, 500));   // Range 2 start
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(multipleRanges, 550));   // Range 2 middle
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(multipleRanges, 600));   // Range 2 end
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(multipleRanges, 1000));  // Range 3 start
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(multipleRanges, 1250));  // Range 3 middle
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(multipleRanges, 1500));  // Range 3 end

    // Test slots in gaps between ranges
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(multipleRanges, 101));  // Between range 1 and 2
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(multipleRanges, 300));  // Between range 1 and 2
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(multipleRanges, 499));  // Between range 1 and 2
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(multipleRanges, 601));  // Between range 2 and 3
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(multipleRanges, 800));  // Between range 2 and 3
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(multipleRanges, 999));  // Between range 2 and 3
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(multipleRanges, 1501)); // After range 3

    freeSlotRangeArray(multipleRanges);

    // Test with single slot ranges
    auto* singleSlotRanges = createSlotRangeArray({
        {42, 42},      // Single slot 42
        {100, 100},    // Single slot 100
        {65535, 65535} // Single slot at max value
    });

    EXPECT_TRUE(SlotRangeArray_ContainsSlot(singleSlotRanges, 42));
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(singleSlotRanges, 100));
    EXPECT_TRUE(SlotRangeArray_ContainsSlot(singleSlotRanges, 65535));

    EXPECT_FALSE(SlotRangeArray_ContainsSlot(singleSlotRanges, 41));
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(singleSlotRanges, 43));
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(singleSlotRanges, 99));
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(singleSlotRanges, 101));
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(singleSlotRanges, 65534));

    freeSlotRangeArray(singleSlotRanges);

    // Test with empty ranges array
    auto* emptyRanges = createSlotRangeArray({});

    EXPECT_FALSE(SlotRangeArray_ContainsSlot(emptyRanges, 0));
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(emptyRanges, 100));
    EXPECT_FALSE(SlotRangeArray_ContainsSlot(emptyRanges, 65535));

    freeSlotRangeArray(emptyRanges);
}
