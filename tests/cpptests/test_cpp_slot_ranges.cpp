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
    auto* original = createSlotRangeArray({{100, 200}});

    // Calculate required buffer size
    size_t size = RedisModuleSlotRangeArray_SerializedSize_Binary(1);
    EXPECT_EQ(size, 4); // 4 bytes for one range (2*uint16_t)

    // Serialize
    std::vector<uint8_t> buffer(size);
    bool result = RedisModuleSlotRangeArray_SerializeBinary(original, buffer.data(), buffer.size());

    EXPECT_TRUE(result);
    // Deserialize
    auto* deserialized = createSlotRangeArray({{0, 0}}); // Allocate space for one range
    result = RedisModuleSlotRangeArray_DeserializeBinary(buffer.data(), buffer.size(), deserialized);

    EXPECT_TRUE(result);
    EXPECT_TRUE(compareExactly(original, deserialized));
    freeSlotRangeArray(original);
    freeSlotRangeArray(deserialized);
}

// Test binary serialization with multiple ranges
TEST_F(SlotRangesTest, testBinarySerializationMultipleRanges) {
    // Test with multiple ranges
    auto* original = createSlotRangeArray({{0, 100}, {500, 600}, {1000, 1500}});

    // Calculate required buffer size
    size_t size = RedisModuleSlotRangeArray_SerializedSize_Binary(3);
    EXPECT_EQ(size, 12); // 12 bytes for three ranges

    // Serialize
    std::vector<uint8_t> buffer(size);
    bool result = RedisModuleSlotRangeArray_SerializeBinary(original, buffer.data(), buffer.size());

    EXPECT_TRUE(result);

    // Deserialize
    auto* deserialized = createSlotRangeArray({{0, 0}, {0, 0}, {0, 0}}); // Allocate space for three ranges
    result = RedisModuleSlotRangeArray_DeserializeBinary(buffer.data(), buffer.size(), deserialized);

    EXPECT_TRUE(result);
    EXPECT_TRUE(compareExactly(original, deserialized));
    freeSlotRangeArray(original);
    freeSlotRangeArray(deserialized);
}

// Test binary serialization with insufficient buffer
TEST_F(SlotRangesTest, testBinarySerializationInsufficientBuffer) {
    // Test with a range that requires more buffer than provided
    auto* original = createSlotRangeArray({{100, 200}});

    size_t size = RedisModuleSlotRangeArray_SerializedSize_Binary(1);
    // Try to serialize with insufficient buffer
    std::vector<uint8_t> buffer(size);
    bool result = RedisModuleSlotRangeArray_SerializeBinary(original, buffer.data(), buffer.size() - 1);

    EXPECT_FALSE(result); // Should fail due to insufficient buffer

    freeSlotRangeArray(original);
}

// Test binary deserialization with invalid data
TEST_F(SlotRangesTest, testBinaryDeserializationInvalidData) {
    // Test with corrupted/invalid serialized data
    uint8_t invalid_buffer[] = {0x00, 0x00, 0x00, 0x01, 0xFF}; // Claims 1 range but incomplete data

    auto* deserialized = createSlotRangeArray({{0, 0}}); // Allocate space for one range
    bool result = RedisModuleSlotRangeArray_DeserializeBinary(invalid_buffer, sizeof(invalid_buffer), deserialized);

    EXPECT_FALSE(result); // Should fail due to incomplete data

    freeSlotRangeArray(deserialized);
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
    size_t size = RedisModuleSlotRangeArray_SerializedSize_Binary(100);
    EXPECT_EQ(size, 400); // 400 bytes for 100 ranges (4 bytes each)

    // Serialize
    std::vector<uint8_t> buffer(size);
    bool result = RedisModuleSlotRangeArray_SerializeBinary(original, buffer.data(), buffer.size());

    EXPECT_TRUE(result);

    // Deserialize
    std::vector<std::pair<uint16_t, uint16_t>> empty_ranges(100, {0, 0});
    auto* deserialized = createSlotRangeArray(empty_ranges);
    result = RedisModuleSlotRangeArray_DeserializeBinary(buffer.data(), size, deserialized);

    EXPECT_TRUE(result);
    EXPECT_TRUE(compareExactly(original, deserialized));

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
    size_t size = RedisModuleSlotRangeArray_SerializedSize_Binary(6);
    EXPECT_EQ(size, 24); // 4 bytes for count + 24 bytes for 6 ranges

    // Serialize
    std::vector<uint8_t> buffer(size);
    bool result = RedisModuleSlotRangeArray_SerializeBinary(original, buffer.data(), buffer.size());

    EXPECT_TRUE(result);

    // Deserialize
    auto* deserialized = createSlotRangeArray({{0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}});
    result = RedisModuleSlotRangeArray_DeserializeBinary(buffer.data(), buffer.size(), deserialized);

    EXPECT_TRUE(result);
    EXPECT_TRUE(compareExactly(original, deserialized));

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
    size_t size = RedisModuleSlotRangeArray_SerializedSize_Binary(1000);
    EXPECT_EQ(size, 4000); // 4000 bytes for 1000 ranges

    // Serialize
    std::vector<uint8_t> buffer(size);
    bool result = RedisModuleSlotRangeArray_SerializeBinary(original, buffer.data(), buffer.size());

    EXPECT_TRUE(result);

    // Deserialize
    std::vector<std::pair<uint16_t, uint16_t>> empty_ranges(1000, {0, 0});
    auto* deserialized = createSlotRangeArray(empty_ranges);
    result = RedisModuleSlotRangeArray_DeserializeBinary(buffer.data(), buffer.size(), deserialized);

    EXPECT_TRUE(result);
    EXPECT_TRUE(compareExactly(original, deserialized));

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
    size_t size = RedisModuleSlotRangeArray_SerializedSize_Binary(3);

    // Serialize
    std::vector<uint8_t> buffer(size);
    bool result = RedisModuleSlotRangeArray_SerializeBinary(original, buffer.data(), buffer.size());

    EXPECT_TRUE(result);

    // Deserialize
    auto* deserialized = createSlotRangeArray({{0, 0}, {0, 0}, {0, 0}});
    result = RedisModuleSlotRangeArray_DeserializeBinary(buffer.data(), buffer.size(), deserialized);

    EXPECT_TRUE(result);
    EXPECT_TRUE(compareExactly(original, deserialized));

    freeSlotRangeArray(original);
    freeSlotRangeArray(deserialized);
};

// Test exact match - identical ranges
TEST_F(SlotRangesTest, testExactMatch) {
    auto* ranges1 = createSlotRangeArray({{0, 100}, {200, 300}});
    auto* ranges2 = createSlotRangeArray({{0, 100}, {200, 300}});

    SlotRangesComparisonResult result = CompareSlotRanges(ranges1, ranges2);
    EXPECT_EQ(result, SLOT_RANGES_MATCH);

    freeSlotRangeArray(ranges1);
    freeSlotRangeArray(ranges2);
}

// Test subset - expected is subset of actual
TEST_F(SlotRangesTest, testSubset) {
    auto* expected = createSlotRangeArray({{50, 75}});
    auto* actual = createSlotRangeArray({{0, 100}, {200, 300}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_SUBSET);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test subset with multiple ranges
TEST_F(SlotRangesTest, testSubsetMultipleRanges) {
    auto* expected = createSlotRangeArray({{10, 20}, {250, 280}});
    auto* actual = createSlotRangeArray({{0, 100}, {200, 300}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_SUBSET);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test does not include - expected has slots not in actual
TEST_F(SlotRangesTest, testDoesNotInclude) {
    auto* expected = createSlotRangeArray({{0, 100}, {400, 500}});
    auto* actual = createSlotRangeArray({{0, 100}, {200, 300}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_DOES_NOT_INCLUDE);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test partial overlap - some slots match, some don't
TEST_F(SlotRangesTest, testPartialOverlap) {
    auto* expected = createSlotRangeArray({{50, 150}});  // 50-100 overlap, 101-150 don't
    auto* actual = createSlotRangeArray({{0, 100}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_DOES_NOT_INCLUDE);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test single slot ranges
TEST_F(SlotRangesTest, testSingleSlotRanges) {
    auto* expected = createSlotRangeArray({{42, 42}});
    auto* actual = createSlotRangeArray({{42, 42}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_MATCH);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test single slot subset
TEST_F(SlotRangesTest, testSingleSlotSubset) {
    auto* expected = createSlotRangeArray({{42, 42}});
    auto* actual = createSlotRangeArray({{40, 50}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_SUBSET);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test edge case: overlapping ranges in actual
TEST_F(SlotRangesTest, testOverlappingRangesInActual) {
    auto* expected = createSlotRangeArray({{25, 75}});
    auto* actual = createSlotRangeArray({{0, 50}, {25, 100}});  // Overlapping ranges

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_SUBSET);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test maximum slot values (Redis has 16384 slots: 0-16383)
TEST_F(SlotRangesTest, testMaxSlotValues) {
    auto* expected = createSlotRangeArray({{16380, 16383}});
    auto* actual = createSlotRangeArray({{16380, 16383}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_MATCH);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test complex scenario with multiple ranges
TEST_F(SlotRangesTest, testComplexMultipleRanges) {
    auto* expected = createSlotRangeArray({{0, 100}, {500, 600}, {1000, 1100}});
    auto* actual = createSlotRangeArray({{0, 200}, {450, 700}, {900, 1200}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_SUBSET);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test when actual has extra ranges
TEST_F(SlotRangesTest, testActualHasExtraRanges) {
    auto* expected = createSlotRangeArray({{0, 100}});
    auto* actual = createSlotRangeArray({{0, 100}, {200, 300}, {400, 500}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_SUBSET);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test gap in expected ranges
TEST_F(SlotRangesTest, testGapInExpectedRanges) {
    auto* expected = createSlotRangeArray({{0, 50}, {100, 150}});  // Gap from 51-99
    auto* actual = createSlotRangeArray({{0, 150}});  // Covers the gap

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_SUBSET);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test gap in actual ranges
TEST_F(SlotRangesTest, testGapInActualRanges) {
    auto* expected = createSlotRangeArray({{0, 150}});  // Needs slots 0-150
    auto* actual = createSlotRangeArray({{0, 50}, {100, 150}});  // Gap from 51-99

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_DOES_NOT_INCLUDE);  // Missing slots 51-99

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test empty intersection
TEST_F(SlotRangesTest, testEmptyIntersection) {
    auto* expected = createSlotRangeArray({{0, 100}});
    auto* actual = createSlotRangeArray({{200, 300}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_DOES_NOT_INCLUDE);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test boundary conditions
TEST_F(SlotRangesTest, testBoundaryConditions) {
    auto* expected = createSlotRangeArray({{0, 0}, {16383, 16383}});  // First and last slots
    auto* actual = createSlotRangeArray({{0, 16383}});  // All slots

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_SUBSET);

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}

// Test reverse comparison (actual subset of expected)
TEST_F(SlotRangesTest, testReverseSubset) {
    auto* expected = createSlotRangeArray({{0, 1000}});  // Large range
    auto* actual = createSlotRangeArray({{100, 200}});   // Small range within

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_DOES_NOT_INCLUDE);  // Expected has more slots than actual

    freeSlotRangeArray(expected);
    freeSlotRangeArray(actual);
}
