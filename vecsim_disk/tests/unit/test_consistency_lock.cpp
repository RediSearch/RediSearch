/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "utils/consistency_lock.h"
#include "vecsim_disk_api.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace vecsim_disk;
using namespace std::chrono_literals;

class ConsistencyLockTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Ensure lock is released before each test
        // (in case a previous test failed while holding the lock)
    }
};

// =============================================================================
// Test: ConsistencySharedGuard blocks exclusive lock
// =============================================================================
// When a shared guard is held, VecSimDisk_AcquireConsistencyLock() should block
// until the shared guard is released.

TEST_F(ConsistencyLockTest, SharedGuardBlocksExclusiveLock) {
    std::atomic<bool> shared_acquired{false};
    std::atomic<bool> exclusive_started{false};
    std::atomic<bool> exclusive_acquired{false};
    std::atomic<bool> shared_released{false};

    // Thread 1: Hold shared lock for a while
    std::thread shared_thread([&]() {
        ConsistencySharedGuard guard;
        shared_acquired.store(true);

        // Wait until exclusive thread has started trying to acquire
        while (!exclusive_started.load()) {
            std::this_thread::sleep_for(1ms);
        }

        // Hold the lock for a bit to ensure exclusive is blocked
        std::this_thread::sleep_for(50ms);

        // Verify exclusive hasn't acquired yet (it should be blocked)
        EXPECT_FALSE(exclusive_acquired.load());

        shared_released.store(true);
        // Guard released here
    });

    // Wait for shared lock to be acquired
    while (!shared_acquired.load()) {
        std::this_thread::sleep_for(1ms);
    }

    // Thread 2: Try to acquire exclusive lock (should block)
    std::thread exclusive_thread([&]() {
        exclusive_started.store(true);

        VecSimDisk_AcquireConsistencyLock();
        exclusive_acquired.store(true);

        // Should only reach here after shared was released
        EXPECT_TRUE(shared_released.load());

        VecSimDisk_ReleaseConsistencyLock();
    });

    shared_thread.join();
    exclusive_thread.join();

    EXPECT_TRUE(exclusive_acquired.load());
}

// =============================================================================
// Test: Multiple shared locks don't block each other
// =============================================================================
// Multiple threads should be able to hold ConsistencySharedGuard simultaneously.

TEST_F(ConsistencyLockTest, MultipleSharedLocksDontBlock) {
    constexpr int NUM_THREADS = 4;
    std::atomic<int> guards_acquired{0};
    std::atomic<bool> all_acquired{false};
    std::atomic<int> guards_verified{0};

    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);

    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([&]() {
            ConsistencySharedGuard guard;
            guards_acquired.fetch_add(1);

            // Wait until all threads have acquired the lock
            while (!all_acquired.load()) {
                if (guards_acquired.load() == NUM_THREADS) {
                    all_acquired.store(true);
                }
                std::this_thread::sleep_for(1ms);
            }

            // All threads should be holding the lock simultaneously
            EXPECT_EQ(guards_acquired.load(), NUM_THREADS);
            guards_verified.fetch_add(1);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(guards_verified.load(), NUM_THREADS);
}

// =============================================================================
// Test: Exclusive lock blocks shared locks
// =============================================================================
// When exclusive lock is held, new shared guards should block.

TEST_F(ConsistencyLockTest, ExclusiveLockBlocksSharedLocks) {
    std::atomic<bool> exclusive_acquired{false};
    std::atomic<bool> shared_started{false};
    std::atomic<bool> shared_acquired{false};
    std::atomic<bool> exclusive_released{false};

    // Thread 1: Hold exclusive lock
    std::thread exclusive_thread([&]() {
        VecSimDisk_AcquireConsistencyLock();
        exclusive_acquired.store(true);

        // Wait until shared thread has started trying to acquire
        while (!shared_started.load()) {
            std::this_thread::sleep_for(1ms);
        }

        // Hold the lock for a bit
        std::this_thread::sleep_for(50ms);

        // Verify shared hasn't acquired yet
        EXPECT_FALSE(shared_acquired.load());

        exclusive_released.store(true);
        VecSimDisk_ReleaseConsistencyLock();
    });

    // Wait for exclusive lock to be acquired
    while (!exclusive_acquired.load()) {
        std::this_thread::sleep_for(1ms);
    }

    // Thread 2: Try to acquire shared lock (should block)
    std::thread shared_thread([&]() {
        shared_started.store(true);

        ConsistencySharedGuard guard;
        shared_acquired.store(true);

        // Should only reach here after exclusive was released
        EXPECT_TRUE(exclusive_released.load());
    });

    exclusive_thread.join();
    shared_thread.join();

    EXPECT_TRUE(shared_acquired.load());
}

// =============================================================================
// Test: C API works when no indexes exist
// =============================================================================
// The lock should be safe to acquire/release even if no indexes exist.

TEST_F(ConsistencyLockTest, CAcquireWithNoIndexes) {
    // Should not block or crash
    VecSimDisk_AcquireConsistencyLock();
    VecSimDisk_ReleaseConsistencyLock();
}

// =============================================================================
// Test: RAII guards properly release on scope exit
// =============================================================================

TEST_F(ConsistencyLockTest, RAIIGuardReleasesOnScopeExit) {
    {
        ConsistencySharedGuard guard;
        // Lock is held
    }
    // Lock should be released

    // Should be able to acquire exclusive immediately
    std::atomic<bool> acquired{false};
    std::thread t([&]() {
        VecSimDisk_AcquireConsistencyLock();
        acquired.store(true);
        VecSimDisk_ReleaseConsistencyLock();
    });

    t.join();
    EXPECT_TRUE(acquired.load());
}

// =============================================================================
// Test: ConsistencyExclusiveGuard RAII works correctly
// =============================================================================

TEST_F(ConsistencyLockTest, ExclusiveGuardRAII) {
    std::atomic<bool> exclusive_held{false};
    std::atomic<bool> shared_attempted{false};
    std::atomic<bool> shared_acquired_while_exclusive_held{false};

    std::thread shared_thread([&]() {
        // Wait for exclusive lock to be held
        while (!exclusive_held.load()) {
            std::this_thread::yield();
        }

        shared_attempted.store(true);
        ConsistencySharedGuard guard;
        // If we get here while exclusive is still held, that's a bug
        shared_acquired_while_exclusive_held.store(exclusive_held.load());
    });

    {
        ConsistencyExclusiveGuard guard;
        exclusive_held.store(true);

        // Wait for shared thread to attempt acquisition
        while (!shared_attempted.load()) {
            std::this_thread::yield();
        }

        // Give shared thread time to block on the lock
        std::this_thread::sleep_for(10ms);

        exclusive_held.store(false);
    }
    // Exclusive guard released - shared thread should now acquire

    shared_thread.join();

    // Shared lock should NOT have been acquired while exclusive was held
    EXPECT_FALSE(shared_acquired_while_exclusive_held.load());
}
