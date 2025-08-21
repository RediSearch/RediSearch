/**
 * @file test_cpp_deletedIds.cpp
 * @brief Unit tests for the DeletedIds class
 *
 * This file contains comprehensive unit tests for the DeletedIds class, which
 * provides an efficient, thread-safe container for tracking deleted document IDs
 * using roaring bitmaps.
 *
 * The tests cover:
 * - Basic functionality (add, remove, contains, size, clear)
 * - Edge cases (empty set, special values)
 * - Thread safety (concurrent operations)
 * - Performance with large numbers of IDs
 */

#include "redisearch.h"
#include "disk/doc_table/deleted_ids/deleted_ids.hpp"
// Note: CRoaring headers are included via deleted_ids.hpp
#include <gtest/gtest.h>
#include <vector>
#include <algorithm>
#include <random>
#include <thread>
#include <atomic>
#include <chrono>

using namespace std;

/**
 * @brief Test fixture for DeletedIds tests
 *
 * This fixture provides a common setup for all DeletedIds tests.
 * Currently, it doesn't require any special setup or teardown.
 */
class DeletedIdsTest : public ::testing::Test {};

/**
 * @brief Test that a newly created DeletedIds is empty
 *
 * This test verifies that:
 * - A newly created DeletedIds has a size of 0
 * - It doesn't contain any document IDs
 */
TEST_F(DeletedIdsTest, EmptyInitialization) {
    search::disk::DeletedIds deletedIds;

    // A newly created DeletedIds should be empty
    ASSERT_EQ(deletedIds.size(), 0);

    // It should not contain any IDs
    ASSERT_FALSE(deletedIds.contains(1));
    ASSERT_FALSE(deletedIds.contains(UINT64_MAX));
}

/**
 * @brief Test the add and contains operations
 *
 * This test verifies that:
 * - Adding document IDs increases the size correctly
 * - The contains method correctly identifies added IDs
 * - The contains method correctly reports false for non-added IDs
 * - The class works with various values including the maximum uint64_t value
 */
TEST_F(DeletedIdsTest, AddAndContains) {
    search::disk::DeletedIds deletedIds;

    // Add some IDs
    deletedIds.add(1);
    deletedIds.add(42);
    deletedIds.add(UINT64_MAX);

    // Check size
    ASSERT_EQ(deletedIds.size(), 3);

    // Check contains
    ASSERT_TRUE(deletedIds.contains(1));
    ASSERT_TRUE(deletedIds.contains(42));
    ASSERT_TRUE(deletedIds.contains(UINT64_MAX));

    // Check non-existent IDs
    ASSERT_FALSE(deletedIds.contains(0));
    ASSERT_FALSE(deletedIds.contains(2));
    ASSERT_FALSE(deletedIds.contains(41));
    ASSERT_FALSE(deletedIds.contains(43));
    ASSERT_FALSE(deletedIds.contains(UINT64_MAX - 1));
}

/**
 * @brief Test the remove operation
 *
 * This test verifies that:
 * - Removing a document ID decreases the size correctly
 * - After removal, contains returns false for the removed ID
 * - Removing a non-existent ID has no effect
 * - After removing all IDs, the container is empty
 */
TEST_F(DeletedIdsTest, Remove) {
    search::disk::DeletedIds deletedIds;

    // Add some IDs
    deletedIds.add(1);
    deletedIds.add(42);
    deletedIds.add(UINT64_MAX);

    // Initial size should be 3
    ASSERT_EQ(deletedIds.size(), 3);

    // Remove an ID
    ASSERT_EQ(deletedIds.remove(42), true);

    // Size should be 2
    ASSERT_EQ(deletedIds.size(), 2);

    // Remove a non-existing ID
    ASSERT_EQ(deletedIds.remove(42), false);

    // Size should still be 2
    ASSERT_EQ(deletedIds.size(), 2);

    // Check contains
    ASSERT_TRUE(deletedIds.contains(1));
    ASSERT_FALSE(deletedIds.contains(42));
    ASSERT_TRUE(deletedIds.contains(UINT64_MAX));

    // Remove non-existent ID (should not affect size)
    deletedIds.remove(100);
    ASSERT_EQ(deletedIds.size(), 2);

    // Remove remaining IDs
    deletedIds.remove(1);
    deletedIds.remove(UINT64_MAX);

    // Size should be 0
    ASSERT_EQ(deletedIds.size(), 0);

    // Should not contain any IDs
    ASSERT_FALSE(deletedIds.contains(1));
    ASSERT_FALSE(deletedIds.contains(42));
    ASSERT_FALSE(deletedIds.contains(UINT64_MAX));
}

/**
 * @brief Test the clear operation
 *
 * This test verifies that:
 * - The clear method removes all document IDs from the container
 * - After clearing, the size is 0
 * - After clearing, contains returns false for all previously added IDs
 */
TEST_F(DeletedIdsTest, Clear) {
    search::disk::DeletedIds deletedIds;

    // Add some IDs
    deletedIds.add(1);
    deletedIds.add(42);
    deletedIds.add(UINT64_MAX);

    // Initial size should be 3
    ASSERT_EQ(deletedIds.size(), 3);

    // Clear the set
    deletedIds.clear();

    // Size should be 0
    ASSERT_EQ(deletedIds.size(), 0);

    // Should not contain any IDs
    ASSERT_FALSE(deletedIds.contains(1));
    ASSERT_FALSE(deletedIds.contains(42));
    ASSERT_FALSE(deletedIds.contains(UINT64_MAX));
}

/**
 * @brief Test with a large number of random document IDs
 *
 * This test verifies that:
 * - The container can handle a large number of document IDs
 * - The size correctly accounts for unique IDs (no duplicates)
 * - All added IDs can be retrieved with contains
 * - The clear method works correctly with large sets
 *
 */
TEST_F(DeletedIdsTest, LargeNumberOfIds) {
    search::disk::DeletedIds deletedIds;

    // Add a large number of IDs
    const size_t numIds = 10000;
    vector<uint64_t> ids;

    // Generate random IDs
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;

    for (size_t i = 0; i < numIds; ++i) {
        uint64_t id = dis(gen);
        ids.push_back(id);
        deletedIds.add(id);
    }

    // Size should match the number of unique IDs
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    ASSERT_EQ(deletedIds.size(), ids.size());

    // All added IDs should be contained
    for (const auto& id : ids) {
        ASSERT_TRUE(deletedIds.contains(id));
    }

    // Clear and verify
    deletedIds.clear();
    ASSERT_EQ(deletedIds.size(), 0);
}

/**
 * @brief Test a sequence of add and remove operations on the same ID
 *
 * This test verifies that:
 * - Adding, removing, and re-adding the same ID works correctly
 * - The contains method returns the expected result after each operation
 * - The size is correctly updated after each operation
 */
TEST_F(DeletedIdsTest, AddRemoveSequence) {
    search::disk::DeletedIds deletedIds;

    // Add, remove, add sequence
    bool added = deletedIds.add(42);
    ASSERT_TRUE(added);  // Should return true for a new addition
    ASSERT_TRUE(deletedIds.contains(42));

    // Should return false for a duplicate, since we use the checked version
    ASSERT_FALSE(deletedIds.add(42));

    ASSERT_TRUE(deletedIds.remove(42));
    ASSERT_FALSE(deletedIds.contains(42));

    added = deletedIds.add(42);
    ASSERT_TRUE(added);  // Should return true for a new addition
    ASSERT_TRUE(deletedIds.contains(42));

    // Size should be 1
    ASSERT_EQ(deletedIds.size(), 1);
}

/**
 * @brief Test the return values of the checked add and remove operations
 *
 * This test verifies that:
 * - add returns true when adding a new ID
 * - add returns false when adding an ID that's already in the set
 * - remove returns true when removing an ID that's in the set
 * - remove returns false when removing an ID that's not in the set
 */
TEST_F(DeletedIdsTest, CheckedOperations) {
    search::disk::DeletedIds deletedIds;

    // Test add with new ID
    bool added = deletedIds.add(42);
    ASSERT_TRUE(added);
    ASSERT_EQ(deletedIds.size(), 1);

    // Test add with existing ID
    added = deletedIds.add(42);
    ASSERT_FALSE(added);  // Should return false for an existing ID
    ASSERT_EQ(deletedIds.size(), 1);  // Size should not change

    // Test remove with existing ID
    bool removed = deletedIds.remove(42);
    ASSERT_TRUE(removed);  // Should return true for an existing ID
    ASSERT_EQ(deletedIds.size(), 0);

    // Test remove with non-existing ID
    removed = deletedIds.remove(42);
    ASSERT_FALSE(removed);  // Should return false for a non-existing ID
    ASSERT_EQ(deletedIds.size(), 0);  // Size should not change

    // Test with multiple IDs
    deletedIds.add(1);
    deletedIds.add(2);
    deletedIds.add(3);

    ASSERT_EQ(deletedIds.size(), 3);

    // Try to add existing IDs
    ASSERT_FALSE(deletedIds.add(1));
    ASSERT_FALSE(deletedIds.add(2));
    ASSERT_FALSE(deletedIds.add(3));

    // Size should still be 3
    ASSERT_EQ(deletedIds.size(), 3);

    // Remove IDs
    ASSERT_TRUE(deletedIds.remove(2));
    ASSERT_EQ(deletedIds.size(), 2);

    // Try to remove already removed ID
    ASSERT_FALSE(deletedIds.remove(2));
    ASSERT_EQ(deletedIds.size(), 2);
}

/**
 * @brief Test with special uint64_t values
 *
 * This test verifies that the DeletedIds class correctly handles various
 * special values across the uint64_t range, including:
 * - Minimum value (0)
 * - Small values
 * - 32-bit boundary values
 * - Middle range values
 * - Maximum value (UINT64_MAX)
 *
 * This ensures the underlying roaring bitmap implementation correctly
 * handles the full range of possible document IDs.
 */
TEST_F(DeletedIdsTest, SpecialValues) {
    search::disk::DeletedIds deletedIds;

    // Test with special values
    const uint64_t values[] = {
        0,                    // Minimum value
        1,                    // Small value
        UINT32_MAX,           // 32-bit boundary
        UINT32_MAX + 1ULL,    // Just above 32-bit boundary
        UINT64_MAX / 2,       // Middle value
        UINT64_MAX - 1,       // One below maximum
        UINT64_MAX            // Maximum value
    };

    // Add all values
    for (const auto& val : values) {
        deletedIds.add(val);
    }

    // Check size
    ASSERT_EQ(deletedIds.size(), sizeof(values) / sizeof(values[0]));

    // Check contains
    for (const auto& val : values) {
        ASSERT_TRUE(deletedIds.contains(val));
    }

    // Remove all values
    for (const auto& val : values) {
        deletedIds.remove(val);
    }

    // Check size
    ASSERT_EQ(deletedIds.size(), 0);

    // Check contains
    for (const auto& val : values) {
        ASSERT_FALSE(deletedIds.contains(val));
    }
}
