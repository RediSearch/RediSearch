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

} // namespace search::disk
