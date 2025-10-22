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
#include "src/redismodule.h"
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

// Test exact match with different order
TEST_F(SlotRangesTest, testExactMatchDifferentOrder) {
    auto* ranges1 = createSlotRangeArray({{0, 100}, {200, 300}});
    auto* ranges2 = createSlotRangeArray({{200, 300}, {0, 100}});

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

// Test edge case: adjacent ranges (should be considered a match)
TEST_F(SlotRangesTest, testAdjacentRanges) {
    auto* expected = createSlotRangeArray({{0, 50}, {51, 100}});
    auto* actual = createSlotRangeArray({{0, 100}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_MATCH);  // Adjacent ranges cover same slots as single range

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

// Test fragmented vs consolidated ranges
TEST_F(SlotRangesTest, testFragmentedVsConsolidated) {
    auto* expected = createSlotRangeArray({{0, 10}, {11, 20}, {21, 30}, {31, 40}});
    auto* actual = createSlotRangeArray({{0, 40}});

    SlotRangesComparisonResult result = CompareSlotRanges(expected, actual);
    EXPECT_EQ(result, SLOT_RANGES_MATCH);  // Same slots, different representation

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
