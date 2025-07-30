/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

/**
 * @file deleted_ids.hpp
 * @brief Header file for the DeletedIds class
 *
 * This file defines the DeletedIds class, which provides an efficient, thread-safe
 * container for tracking deleted document IDs using roaring bitmaps.
 *
 * The implementation uses the CRoaring library's Roaring64Map for efficient storage
 * and retrieval of 64-bit document IDs, and provides thread safety through a
 * read-write lock mechanism that allows concurrent reads but exclusive writes.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <shared_mutex>  // For thread-safe read-write lock
#include "cpp/roaring/roaring64map.hh"  // From CRoaring library
#include "roaring/memory.h"     // For custom allocators
#include "rmalloc.h"            // For Redis allocators
#include "redisearch.h"

namespace search::disk {

/**
 * @brief A thread-safe container for tracking deleted document IDs using roaring bitmaps
 *
 * The DeletedIds class provides an efficient way to store and query a set of
 * deleted document IDs using the CRoaring library's Roaring64Map implementation.
 * Roaring bitmaps are a compressed bitmap format that is particularly efficient
 * for sparse sets of integers.
 *
 * This class is thread-safe, using a read-write lock mechanism that allows:
 * - Multiple threads to concurrently read from the container
 * - Exclusive access for threads that modify the container
 *
 * Performance characteristics:
 * - Space efficiency: Roaring bitmaps typically use less memory than standard
 *   collections for storing large sets of integers
 * - Time efficiency: Operations like add, remove, and contains are typically O(1)
 *   or O(log n) depending on the internal state of the bitmap
 *
 * Usage example:
 * @code
 * DeletedIds deletedIds;
 *
 * // Mark documents as deleted
 * deletedIds.add(42);
 * deletedIds.add(1000);
 *
 * // Check if a document is deleted
 * if (deletedIds.contains(42)) {
 *     // Document 42 is deleted
 * }
 *
 * // Get the count of deleted documents
 * uint64_t count = deletedIds.size();
 *
 * // Remove a document from the deleted set
 * deletedIds.remove(42);
 *
 * // Clear all deleted document IDs
 * deletedIds.clear();
 * @endcode
 */
class DeletedIds {
public:
    /**
     * @brief Constructs an empty DeletedIds container
     *
     * Initializes a new, empty container for tracking deleted document IDs.
     * The container starts with no deleted IDs and is ready to use immediately.
     *
     * Note: The CRoaring library should be initialized with Redis allocators
     * before any DeletedIds instances are created, which is done in the module's
     * OnLoad function.
     */
    DeletedIds();

    /**
     * @brief Destructor
     *
     * Cleans up resources used by the DeletedIds container.
     */
    ~DeletedIds();

    /**
     * @brief Adds a document ID to the deleted set
     *
     * Marks the specified document ID as deleted by adding it to the set.
     * If the ID is already in the set, this operation has no effect.
     *
     * @param id The document ID to mark as deleted
     * @return true if the ID was newly added, false if it was already in the set
     *
     * Time complexity: O(1) amortized
     * Thread safety: Thread-safe, acquires a write lock
     */
    bool add(t_docId id);

    /**
     * @brief Removes a document ID from the deleted set
     *
     * Unmarks the specified document ID as deleted by removing it from the set.
     * If the ID is not in the set, this operation has no effect.
     *
     * @param id The document ID to unmark as deleted
     * @return true if the ID was removed, false if it wasn't in the set
     *
     * Time complexity: O(1) amortized
     * Thread safety: Thread-safe, acquires a write lock
     */
    bool remove(t_docId id);

    /**
     * @brief Checks if a document ID is in the deleted set
     *
     * Determines whether the specified document ID is marked as deleted.
     *
     * @param id The document ID to check
     * @return true if the document ID is marked as deleted, false otherwise
     *
     * Time complexity: O(1) amortized
     * Thread safety: Thread-safe, acquires a read lock
     */
    bool contains(t_docId id) const;

    /**
     * @brief Returns the number of document IDs in the deleted set
     *
     * Counts the total number of document IDs that are currently marked as deleted.
     *
     * @return The count of deleted document IDs
     *
     * Time complexity: O(1)
     * Thread safety: Thread-safe, acquires a read lock
     */
    uint64_t size() const;

    /**
     * @brief Clears all deleted document IDs
     *
     * Removes all document IDs from the deleted set, effectively marking
     * all documents as not deleted.
     *
     * Time complexity: O(1)
     * Thread safety: Thread-safe, acquires a write lock
     */
    void clear();

private:
    /**
     * @brief The underlying roaring bitmap storing the deleted IDs
     *
     * This is the core data structure that efficiently stores the set of deleted
     * document IDs. We use a unique_ptr to manage the memory and lifetime of the
     * Roaring64Map object.
     *
     * The Roaring64Map is a specialized data structure from the CRoaring library
     * that efficiently stores and queries large sets of 64-bit integers using
     * compressed bitmap techniques.
     */
    roaring::Roaring64Map bitmap_;

    /**
     * @brief Read-write lock for thread safety
     *
     * This lock provides thread safety for the DeletedIds class by:
     * - Allowing multiple threads to concurrently read from the bitmap (shared lock)
     * - Ensuring exclusive access when a thread modifies the bitmap (exclusive lock)
     *
     * The lock is marked as mutable to allow const methods like contains() and size()
     * to acquire the lock.
     */
    mutable std::shared_mutex rwlock_;
};

} // namespace search::disk
