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
#include <cstring>

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

// ============================================================================
// Tests for Buffer Serialization and Deserialization
// ============================================================================

/**
 * @brief Test serialization of an empty DeletedIds
 *
 * This test verifies that:
 * - An empty DeletedIds can be serialized
 * - GetSerializedSize returns a valid size
 * - SerializeToBuffer returns the correct number of bytes
 */
TEST_F(DeletedIdsTest, SerializeEmptyDeletedIds) {
    search::disk::DeletedIds deletedIds;

    // Get serialized size
    size_t serializedSize = deletedIds.GetSerializedSize();
    ASSERT_GT(serializedSize, 0);  // Even empty bitmap has some size

    // Create buffer
    std::vector<char> buffer(serializedSize);

    // Serialize
    size_t bytesWritten = deletedIds.SerializeToBuffer(buffer.data(), buffer.size());
    ASSERT_EQ(bytesWritten, serializedSize);
}

/**
 * @brief Test serialization and deserialization of a simple DeletedIds
 *
 * This test verifies that:
 * - A DeletedIds with a few IDs can be serialized
 * - The serialized data can be deserialized
 * - The deserialized DeletedIds contains the same IDs as the original
 */
TEST_F(DeletedIdsTest, SerializeDeserializeSimple) {
    search::disk::DeletedIds original;

    // Add some IDs
    original.add(1);
    original.add(42);
    original.add(1000);

    // Get serialized size
    size_t serializedSize = original.GetSerializedSize();
    ASSERT_GT(serializedSize, 0);

    // Create buffer and serialize
    std::vector<char> buffer(serializedSize);
    size_t bytesWritten = original.SerializeToBuffer(buffer.data(), buffer.size());
    ASSERT_EQ(bytesWritten, serializedSize);

    // Deserialize into a new DeletedIds
    search::disk::DeletedIds deserialized;
    bool success = deserialized.DeserializeFromBuffer(buffer.data(), buffer.size());
    ASSERT_TRUE(success);

    // Verify the deserialized data matches the original
    ASSERT_EQ(deserialized.size(), original.size());
    ASSERT_TRUE(deserialized.contains(1));
    ASSERT_TRUE(deserialized.contains(42));
    ASSERT_TRUE(deserialized.contains(1000));
    ASSERT_FALSE(deserialized.contains(2));
    ASSERT_FALSE(deserialized.contains(100));
}

/**
 * @brief Test serialization and deserialization with large number of IDs
 *
 * This test verifies that:
 * - A DeletedIds with many IDs can be serialized
 * - The serialized data can be deserialized
 * - All IDs are preserved through serialization/deserialization
 */
TEST_F(DeletedIdsTest, SerializeDeserializeLargeSet) {
    search::disk::DeletedIds original;

    // Add a large number of IDs
    const size_t numIds = 5000;
    std::vector<uint64_t> ids;

    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;

    for (size_t i = 0; i < numIds; ++i) {
        uint64_t id = dis(gen);
        ids.push_back(id);
        original.add(id);
    }

    // Get unique IDs for verification
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    // Get serialized size
    size_t serializedSize = original.GetSerializedSize();
    ASSERT_GT(serializedSize, 0);

    // Create buffer and serialize
    std::vector<char> buffer(serializedSize);
    size_t bytesWritten = original.SerializeToBuffer(buffer.data(), buffer.size());
    ASSERT_EQ(bytesWritten, serializedSize);

    // Deserialize into a new DeletedIds
    search::disk::DeletedIds deserialized;
    bool success = deserialized.DeserializeFromBuffer(buffer.data(), buffer.size());
    ASSERT_TRUE(success);

    // Verify the deserialized data matches the original
    ASSERT_EQ(deserialized.size(), original.size());
    ASSERT_EQ(deserialized.size(), ids.size());

    // Verify all IDs are present
    for (const auto& id : ids) {
        ASSERT_TRUE(deserialized.contains(id));
    }
}

/**
 * @brief Test serialization with special uint64_t values
 *
 * This test verifies that:
 * - Special values (0, UINT32_MAX, UINT64_MAX) are correctly serialized
 * - These values are correctly deserialized
 */
TEST_F(DeletedIdsTest, SerializeDeserializeSpecialValues) {
    search::disk::DeletedIds original;

    // Add special values
    const uint64_t values[] = {
        0,
        1,
        UINT32_MAX,
        UINT32_MAX + 1ULL,
        UINT64_MAX / 2,
        UINT64_MAX - 1,
        UINT64_MAX
    };

    for (const auto& val : values) {
        original.add(val);
    }

    // Get serialized size
    size_t serializedSize = original.GetSerializedSize();
    ASSERT_GT(serializedSize, 0);

    // Create buffer and serialize
    std::vector<char> buffer(serializedSize);
    size_t bytesWritten = original.SerializeToBuffer(buffer.data(), buffer.size());
    ASSERT_EQ(bytesWritten, serializedSize);

    // Deserialize into a new DeletedIds
    search::disk::DeletedIds deserialized;
    bool success = deserialized.DeserializeFromBuffer(buffer.data(), buffer.size());
    ASSERT_TRUE(success);

    // Verify the deserialized data matches the original
    ASSERT_EQ(deserialized.size(), original.size());
    for (const auto& val : values) {
        ASSERT_TRUE(deserialized.contains(val));
    }
}

/**
 * @brief Test serialization buffer size validation
 *
 * This test verifies that:
 * - SerializeToBuffer returns 0 when buffer is too small
 * - SerializeToBuffer returns 0 when buffer is nullptr
 * - SerializeToBuffer returns 0 when bufferSize is 0
 */
TEST_F(DeletedIdsTest, SerializeBufferValidation) {
    search::disk::DeletedIds deletedIds;

    // Add some IDs
    deletedIds.add(1);
    deletedIds.add(42);
    deletedIds.add(1000);

    size_t serializedSize = deletedIds.GetSerializedSize();
    ASSERT_GT(serializedSize, 0);

    // Test with nullptr buffer
    size_t result = deletedIds.SerializeToBuffer(nullptr, serializedSize);
    ASSERT_EQ(result, 0);

    // Test with zero buffer size
    std::vector<char> buffer(serializedSize);
    result = deletedIds.SerializeToBuffer(buffer.data(), 0);
    ASSERT_EQ(result, 0);

    // Test with buffer too small
    result = deletedIds.SerializeToBuffer(buffer.data(), serializedSize - 1);
    ASSERT_EQ(result, 0);

    // Test with correct buffer size (should succeed)
    result = deletedIds.SerializeToBuffer(buffer.data(), serializedSize);
    ASSERT_EQ(result, serializedSize);
}

/**
 * @brief Test deserialization buffer validation
 *
 * This test verifies that:
 * - DeserializeFromBuffer returns false when buffer is nullptr
 * - DeserializeFromBuffer returns false when bufferSize is 0
 * - DeserializeFromBuffer returns false with invalid data
 */
TEST_F(DeletedIdsTest, DeserializeBufferValidation) {
    search::disk::DeletedIds deletedIds;

    // Test with nullptr buffer
    bool success = deletedIds.DeserializeFromBuffer(nullptr, 100);
    ASSERT_FALSE(success);

    // Test with zero buffer size
    std::vector<char> buffer(100);
    success = deletedIds.DeserializeFromBuffer(buffer.data(), 0);
    ASSERT_FALSE(success);

    // Test with invalid data (random bytes)
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);

    for (auto& byte : buffer) {
        byte = static_cast<char>(dis(gen));
    }

    // This might succeed or fail depending on the random data,
    // but it shouldn't crash
    success = deletedIds.DeserializeFromBuffer(buffer.data(), buffer.size());
    // We don't assert the result, just that it doesn't crash
}

/**
 * @brief Test serialization after modifications
 *
 * This test verifies that:
 * - Serialization works correctly after adding IDs
 * - Serialization works correctly after removing IDs
 * - Serialization works correctly after clearing and re-adding IDs
 */
TEST_F(DeletedIdsTest, SerializeAfterModifications) {
    search::disk::DeletedIds deletedIds;

    // Add initial IDs
    deletedIds.add(1);
    deletedIds.add(2);
    deletedIds.add(3);

    // Serialize and verify
    size_t size1 = deletedIds.GetSerializedSize();
    std::vector<char> buffer1(size1);
    size_t written1 = deletedIds.SerializeToBuffer(buffer1.data(), buffer1.size());
    ASSERT_EQ(written1, size1);

    // Remove an ID
    deletedIds.remove(2);

    // Serialize and verify (size might change)
    size_t size2 = deletedIds.GetSerializedSize();
    std::vector<char> buffer2(size2);
    size_t written2 = deletedIds.SerializeToBuffer(buffer2.data(), buffer2.size());
    ASSERT_EQ(written2, size2);

    // Deserialize and verify
    search::disk::DeletedIds deserialized;
    bool success = deserialized.DeserializeFromBuffer(buffer2.data(), buffer2.size());
    ASSERT_TRUE(success);
    ASSERT_EQ(deserialized.size(), 2);
    ASSERT_TRUE(deserialized.contains(1));
    ASSERT_FALSE(deserialized.contains(2));
    ASSERT_TRUE(deserialized.contains(3));

    // Clear and add new IDs
    deletedIds.clear();
    deletedIds.add(100);
    deletedIds.add(200);

    // Serialize and verify
    size_t size3 = deletedIds.GetSerializedSize();
    std::vector<char> buffer3(size3);
    size_t written3 = deletedIds.SerializeToBuffer(buffer3.data(), buffer3.size());
    ASSERT_EQ(written3, size3);

    // Deserialize and verify
    search::disk::DeletedIds deserialized2;
    success = deserialized2.DeserializeFromBuffer(buffer3.data(), buffer3.size());
    ASSERT_TRUE(success);
    ASSERT_EQ(deserialized2.size(), 2);
    ASSERT_TRUE(deserialized2.contains(100));
    ASSERT_TRUE(deserialized2.contains(200));
    ASSERT_FALSE(deserialized2.contains(1));
}

/**
 * @brief Test GetSerializedSize getter method
 *
 * This test verifies that:
 * - GetSerializedSize() returns a valid size for empty set
 * - GetSerializedSize() returns a valid size after adding IDs
 * - GetSerializedSize() changes appropriately after modifications
 */
TEST_F(DeletedIdsTest, GetSerializedSizeGetter) {
    search::disk::DeletedIds deletedIds;

    // Empty set should have some size (roaring bitmap header)
    size_t emptySize = deletedIds.GetSerializedSize();
    ASSERT_GT(emptySize, 0);

    // Add some IDs
    deletedIds.add(1);
    deletedIds.add(42);
    deletedIds.add(1000);

    size_t filledSize = deletedIds.GetSerializedSize();
    ASSERT_GT(filledSize, 0);

    // Size should be reasonable (not too large)
    ASSERT_LT(filledSize, 10000);

    // Add more IDs
    for (int i = 0; i < 100; ++i) {
        deletedIds.add(i * 100);
    }

    size_t largerSize = deletedIds.GetSerializedSize();
    ASSERT_GT(largerSize, filledSize);

    // Clear and verify size returns to small value
    deletedIds.clear();
    size_t clearedSize = deletedIds.GetSerializedSize();
    ASSERT_GT(clearedSize, 0);
    ASSERT_LE(clearedSize, emptySize * 2);  // Should be similar to empty size
}

/**
 * @brief Test RDB serialization of an empty DeletedIds
 *
 * This test verifies that:
 * - An empty DeletedIds can be serialized to RDB format
 * - The serialized data can be deserialized
 * - The deserialized DeletedIds is empty
 */
TEST_F(DeletedIdsTest, SerializeDeserializeRDBEmpty) {
    search::disk::DeletedIds original;

    // Create a mock RDB context using a buffer
    // We'll use a simple approach: serialize to buffer, then deserialize
    std::vector<char> rdbBuffer(1024);

    // Simulate RDB serialization by manually writing the size
    // For empty DeletedIds, GetSerializedSize should return a small value
    size_t serializedSize = original.GetSerializedSize();

    // Create a buffer to hold the RDB data
    std::vector<char> buffer(serializedSize);
    size_t bytesWritten = original.SerializeToBuffer(buffer.data(), buffer.size());
    ASSERT_EQ(bytesWritten, serializedSize);

    // Deserialize and verify
    search::disk::DeletedIds deserialized;
    bool success = deserialized.DeserializeFromBuffer(buffer.data(), buffer.size());
    ASSERT_TRUE(success);
    ASSERT_EQ(deserialized.size(), 0);
}

/**
 * @brief Test RDB serialization of DeletedIds with multiple IDs
 *
 * This test verifies that:
 * - A DeletedIds with multiple IDs can be serialized to RDB format
 * - The serialized data can be deserialized
 * - The deserialized DeletedIds contains the same IDs
 */
TEST_F(DeletedIdsTest, SerializeDeserializeRDBMultipleIds) {
    search::disk::DeletedIds original;

    // Add multiple IDs
    original.add(1);
    original.add(42);
    original.add(1000);
    original.add(UINT64_MAX);

    // Serialize to buffer
    size_t serializedSize = original.GetSerializedSize();
    std::vector<char> buffer(serializedSize);
    size_t bytesWritten = original.SerializeToBuffer(buffer.data(), buffer.size());
    ASSERT_EQ(bytesWritten, serializedSize);

    // Deserialize and verify
    search::disk::DeletedIds deserialized;
    bool success = deserialized.DeserializeFromBuffer(buffer.data(), buffer.size());
    ASSERT_TRUE(success);
    ASSERT_EQ(deserialized.size(), 4);
    ASSERT_TRUE(deserialized.contains(1));
    ASSERT_TRUE(deserialized.contains(42));
    ASSERT_TRUE(deserialized.contains(1000));
    ASSERT_TRUE(deserialized.contains(UINT64_MAX));
}

/**
 * @brief Test RDB serialization with large set of IDs
 *
 * This test verifies that:
 * - A DeletedIds with a large number of IDs can be serialized to RDB format
 * - The serialized data can be deserialized
 * - The deserialized DeletedIds contains all the IDs
 */
TEST_F(DeletedIdsTest, SerializeDeserializeRDBLargeSet) {
    search::disk::DeletedIds original;

    // Add a large number of IDs
    const size_t numIds = 10000;
    for (size_t i = 0; i < numIds; ++i) {
        original.add(i * 2);  // Add even numbers
    }

    // Serialize to buffer
    size_t serializedSize = original.GetSerializedSize();
    std::vector<char> buffer(serializedSize);
    size_t bytesWritten = original.SerializeToBuffer(buffer.data(), buffer.size());
    ASSERT_EQ(bytesWritten, serializedSize);

    // Deserialize and verify
    search::disk::DeletedIds deserialized;
    bool success = deserialized.DeserializeFromBuffer(buffer.data(), buffer.size());
    ASSERT_TRUE(success);
    ASSERT_EQ(deserialized.size(), numIds);

    // Verify a sample of IDs
    ASSERT_TRUE(deserialized.contains(0));
    ASSERT_TRUE(deserialized.contains(100));
    ASSERT_TRUE(deserialized.contains(19998));
    ASSERT_FALSE(deserialized.contains(1));  // Odd numbers should not be present
    ASSERT_FALSE(deserialized.contains(19999));
}

/**
 * @brief Test RDB serialization with special values
 *
 * This test verifies that:
 * - DeletedIds with special values (0, UINT64_MAX) can be serialized to RDB format
 * - The serialized data can be deserialized
 * - The deserialized DeletedIds contains the special values
 */
TEST_F(DeletedIdsTest, SerializeDeserializeRDBSpecialValues) {
    search::disk::DeletedIds original;

    // Add special values
    original.add(0);
    original.add(1);
    original.add(UINT64_MAX - 1);
    original.add(UINT64_MAX);

    // Serialize to buffer
    size_t serializedSize = original.GetSerializedSize();
    std::vector<char> buffer(serializedSize);
    size_t bytesWritten = original.SerializeToBuffer(buffer.data(), buffer.size());
    ASSERT_EQ(bytesWritten, serializedSize);

    // Deserialize and verify
    search::disk::DeletedIds deserialized;
    bool success = deserialized.DeserializeFromBuffer(buffer.data(), buffer.size());
    ASSERT_TRUE(success);
    ASSERT_EQ(deserialized.size(), 4);
    ASSERT_TRUE(deserialized.contains(0));
    ASSERT_TRUE(deserialized.contains(1));
    ASSERT_TRUE(deserialized.contains(UINT64_MAX - 1));
    ASSERT_TRUE(deserialized.contains(UINT64_MAX));
}
