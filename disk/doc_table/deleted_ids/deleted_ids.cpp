/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

/**
 * @file deleted_ids.cpp
 * @brief Implementation of the DeletedIds class
 *
 * This file contains the implementation of the DeletedIds class, which provides
 * an efficient, thread-safe container for tracking deleted document IDs using
 * roaring bitmaps from the CRoaring library.
 *
 * The implementation uses a read-write lock mechanism to ensure thread safety,
 * allowing concurrent reads but exclusive writes.
 */

#include "disk/doc_table/deleted_ids/deleted_ids.hpp"
#include "redisearch.h"
#include "rmalloc.h"
#include "disk/doc_table/doc_table_disk_c.h"
#include <mutex>

/**
 * @brief Initialize the CRoaring library to use Redis allocators
 *
 * This function sets up the CRoaring library to use Redis allocators
 * for all memory operations. It is called once during module initialization
 * in the OnLoad function.
 */
void initRoaringWithRedisAllocators() {
    // Set up the memory hooks for CRoaring
    roaring_memory_t memory_hook = {
        .malloc = rm_malloc,
        .realloc = rm_realloc,
        .calloc = rm_calloc,
        .free = rm_free,
        .aligned_malloc = nullptr,  // Use default implementation
        .aligned_free = nullptr     // Use default implementation
    };

    // Initialize CRoaring with Redis allocators
    roaring_init_memory_hook(memory_hook);
}

namespace search::disk {

/**
 * @brief Constructs an empty DeletedIds container
 *
 * Initializes the bitmap_ member with a new Roaring64Map instance.
 * The rwlock_ member is automatically initialized by its default constructor.
 *
 * Note: The CRoaring library should be initialized with Redis allocators
 * before any DeletedIds instances are created, which is done in the module's
 * OnLoad function.
 */
DeletedIds::DeletedIds() : bitmap_(roaring::Roaring64Map()) {}

/**
 * @brief Destructor
 *
 * The default destructor is sufficient because:
 * - roaring::Roaring64Map automatically cleans up the bitmap
 * - std::shared_mutex is automatically destroyed
 */
DeletedIds::~DeletedIds() = default;

/**
 * @brief Adds a document ID to the deleted set
 *
 * This method marks a document as deleted by adding its ID to the bitmap.
 * It acquires a write lock to ensure thread safety during the modification.
 *
 * @param id The document ID to mark as deleted
 * @return true if the ID was newly added, false if it was already in the set
 */
bool DeletedIds::add(t_docId id) {
    // Acquire write lock before modifying the bitmap
    std::unique_lock<std::shared_mutex> lock(rwlock_);
    return bitmap_.addChecked(id);
}

/**
 * @brief Removes a document ID from the deleted set
 *
 * This method unmarks a document as deleted by removing its ID from the bitmap.
 * It acquires a write lock to ensure thread safety during the modification.
 *
 * @param id The document ID to unmark as deleted
 * @return true if the ID was removed, false if it wasn't in the set
 */
bool DeletedIds::remove(t_docId id) {
    // Acquire write lock before modifying the bitmap
    std::unique_lock<std::shared_mutex> lock(rwlock_);
    return bitmap_.removeChecked(id);
}

/**
 * @brief Checks if a document ID is in the deleted set
 *
 * This method checks whether a document is marked as deleted by looking up its ID
 * in the bitmap. It acquires a read lock, which allows multiple threads to
 * perform this check concurrently.
 *
 * @param id The document ID to check
 * @return true if the document ID is marked as deleted, false otherwise
 */
bool DeletedIds::contains(t_docId id) const {
    // Acquire read lock for reading from the bitmap
    std::shared_lock<std::shared_mutex> lock(rwlock_);
    return bitmap_.contains(id);
}

/**
 * @brief Returns the number of document IDs in the deleted set
 *
 * This method returns the total count of document IDs that are currently marked
 * as deleted. It acquires a read lock, which allows multiple threads to
 * perform this operation concurrently.
 *
 * @return The count of deleted document IDs
 */
uint64_t DeletedIds::size() const {
    // Acquire read lock for reading from the bitmap
    std::shared_lock<std::shared_mutex> lock(rwlock_);
    return bitmap_.cardinality();
}

/**
 * @brief Clears all deleted document IDs
 *
 * This method removes all document IDs from the deleted set, effectively
 * marking all documents as not deleted. It acquires a write lock to ensure
 * thread safety during the modification.
 */
void DeletedIds::clear() {
    // Acquire write lock before modifying the bitmap
    std::unique_lock<std::shared_mutex> lock(rwlock_);
    bitmap_.clear();
}

/**
 * @brief Gets the size needed for serialization
 *
 * Returns the number of bytes needed to serialize this DeletedIds container
 * in a portable format using the roaring bitmap's getSizeInBytes method.
 *
 * @return Size in bytes needed for serialization
 */
size_t DeletedIds::GetSerializedSize() const {
    // Acquire read lock for reading from the bitmap
    std::shared_lock<std::shared_mutex> lock(rwlock_);
    return bitmap_.getSizeInBytes();
}

/**
 * @brief Serializes the deleted IDs to a buffer
 *
 * Serializes the roaring bitmap to a portable format that can be stored
 * in RDB or transmitted over the network.
 *
 * @param buffer Buffer to write the serialized data to
 * @param bufferSize Size of the buffer
 * @return Number of bytes written, or 0 on error
 */
size_t DeletedIds::SerializeToBuffer(char* buffer, size_t bufferSize) const {
    if (!buffer || bufferSize == 0) {
        return 0;
    }

    // Acquire read lock for reading from the bitmap
    std::shared_lock<std::shared_mutex> lock(rwlock_);

    size_t requiredSize = bitmap_.getSizeInBytes();
    if (bufferSize < requiredSize) {
        return 0; // Buffer too small
    }

    // Write the bitmap to the buffer
    bitmap_.write(buffer);
    return requiredSize;
}

/**
 * @brief Deserializes deleted IDs from a buffer
 *
 * Loads the roaring bitmap from a portable format that was previously
 * serialized using SerializeToBuffer.
 *
 * @param buffer Buffer containing the serialized data
 * @param bufferSize Size of the buffer
 * @return true on success, false on error
 */
bool DeletedIds::DeserializeFromBuffer(const char* buffer, size_t bufferSize) {
    if (!buffer || bufferSize == 0) {
        return false;
    }
    std::unique_lock<std::shared_mutex> lock(rwlock_);
    bitmap_.clear();

    try {
        // Clear existing data
        // Read from the buffer
        bitmap_ = roaring::Roaring64Map::read(buffer);
        return true;
    } catch (...) {
        // If deserialization fails, ensure bitmap is in a clean state
        // TODO: Handle some error logging
        return false;
    }
}

/**
 * @brief Serializes the deleted IDs to Redis RDB format
 *
 * Serializes the roaring bitmap to Redis RDB format for persistence.
 * This method handles the complete serialization including size information.
 *
 * @param rdb Redis module IO handle for writing
 */
void DeletedIds::SerializeToRDB(RedisModuleIO* rdb) const {
    RS_ASSERT(rdb != nullptr);

    // Get the roaring bitmap data
    // We need to serialize the roaring bitmap to a portable format

    // First, get the size needed for serialization
    size_t serializedSize = GetSerializedSize();

    // Save the size
    RedisModule_SaveUnsigned(rdb, serializedSize);

    if (serializedSize > 0) {
        // Allocate buffer and serialize
        std::vector<char> buffer(serializedSize);
        size_t actualSize = SerializeToBuffer(buffer.data(), buffer.size());

        RS_ASSERT(actualSize == serializedSize);

        // Save the serialized data
        RedisModule_SaveStringBuffer(rdb, buffer.data(), actualSize);
    }
}

/**
 * @brief Deserializes deleted IDs from Redis RDB format
 *
 * Loads the roaring bitmap from Redis RDB format that was previously
 * serialized using SerializeToRDB.
 *
 * @param rdb Redis module IO handle for reading
 * @return true on success, false on error
 */
bool DeletedIds::DeserializeFromRDB(RedisModuleIO* rdb) {
    RS_ASSERT(rdb != nullptr);

    // Load the size
    uint64_t serializedSize = RedisModule_LoadUnsigned(rdb);

    if (serializedSize > 0) {
        // Load the serialized data
        size_t loadedSize;
        char* buffer = RedisModule_LoadStringBuffer(rdb, &loadedSize);
        if (!buffer || loadedSize != serializedSize) {
            if (buffer) RedisModule_Free(buffer);
            return false;
        }

        // Deserialize the roaring bitmap
        if (!DeserializeFromBuffer(buffer, loadedSize)) {
            RedisModule_Free(buffer);
            return false;
        }

        RedisModule_Free(buffer);
    }

    return true;
}

} // namespace search::disk
