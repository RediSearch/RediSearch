/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <mutex>
#include <shared_mutex>

/**
 * @brief Global consistency lock for coordinating async operations with fork/snapshot.
 *
 * This lock ensures that disk and in-memory data structures are in a consistent state
 * when the main thread needs to fork.
 *
 * Usage pattern:
 * - Async jobs (background threads): Acquire SHARED lock while applying changes to
 *   disk and in-memory structures, release after both are consistent.
 * - Main thread (before fork): Acquire EXCLUSIVE lock to block all async modifications,
 *   release after fork completes.
 *
 * The lock is global (not per-index) because:
 * 1. Fork affects all indexes simultaneously
 * 2. Main thread needs a single synchronization point
 * 3. Async jobs are I/O-bound, so cache-line contention from shared lock
 *    acquisition is negligible compared to disk latency
 */

namespace vecsim_disk {

/**
 * @brief Get the global consistency mutex.
 *
 * The mutex has static storage duration - it's created on first access
 * and persists for the program's lifetime. Safe to call even if no
 * indexes exist.
 *
 * @return Reference to the global shared mutex.
 */
inline std::shared_mutex& getConsistencyMutex() {
    static std::shared_mutex mutex;
    return mutex;
}

/**
 * @brief RAII guard for shared (reader) access to the consistency lock.
 *
 * Use this in async jobs when modifying in-memory structures.
 */
class ConsistencySharedGuard {
public:
    ConsistencySharedGuard() : lock_(getConsistencyMutex()) {}
    ~ConsistencySharedGuard() = default;

    // Non-copyable, non-movable
    ConsistencySharedGuard(const ConsistencySharedGuard&) = delete;
    ConsistencySharedGuard& operator=(const ConsistencySharedGuard&) = delete;
    ConsistencySharedGuard(ConsistencySharedGuard&&) = delete;
    ConsistencySharedGuard& operator=(ConsistencySharedGuard&&) = delete;

private:
    std::shared_lock<std::shared_mutex> lock_;
};

/**
 * @brief RAII guard for exclusive (writer) access to the consistency lock.
 *
 * Use this in the main thread before fork operations.
 */
class ConsistencyExclusiveGuard {
public:
    ConsistencyExclusiveGuard() : lock_(getConsistencyMutex()) {}
    ~ConsistencyExclusiveGuard() = default;

    // Non-copyable, non-movable
    ConsistencyExclusiveGuard(const ConsistencyExclusiveGuard&) = delete;
    ConsistencyExclusiveGuard& operator=(const ConsistencyExclusiveGuard&) = delete;
    ConsistencyExclusiveGuard(ConsistencyExclusiveGuard&&) = delete;
    ConsistencyExclusiveGuard& operator=(ConsistencyExclusiveGuard&&) = delete;

private:
    std::unique_lock<std::shared_mutex> lock_;
};

} // namespace vecsim_disk
