/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "util/shared_exclusive_lock.h"
#include "redismock/redismock.h"
#include "redismock/internal.h"
#include <pthread.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <mutex>
#include <set>
#include <unordered_set>

// Test-specific state for controlling GIL behavior
static std::atomic<bool> test_gil_owned{false};

class SharedExclusiveLockTest : public ::testing::Test {
protected:
    void SetUp() override {
        SharedExclusiveLock_Init();
        test_gil_owned.store(false);
        ctx = new RedisModuleCtx();
    }

    void TearDown() override {
        SharedExclusiveLock_Destroy();
        delete ctx;
    }

    RedisModuleCtx *ctx;
};

// Structure to pass data to worker threads
struct WorkerThreadData {
    RedisModuleCtx *ctx;
    int *counter;
    std::atomic<int> *threads_ready;
    std::atomic<bool> *start_flag;
    std::unordered_set<int> *thread_ids_set;
    int thread_id;
    int work_iterations;
};

// Worker thread function that tries to acquire the shared exclusive lock
void* worker_thread_func(void* arg) {
    WorkerThreadData* data = static_cast<WorkerThreadData*>(arg);

    data->threads_ready->fetch_add(1);
    // Wait for start signal
    while (!data->start_flag->load()) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    // Try to acquire the lock and do some work
    SharedExclusiveLockType lock_type = SharedExclusiveLock_Acquire(data->ctx);
    data->thread_ids_set->insert(data->thread_id);

    // Simulate some work
    for (int i = 0; i < data->work_iterations; ++i) {
        *(data->counter) += 1;
    }
    // Release the lock
    SharedExclusiveLock_Release(data->ctx, lock_type);
    return nullptr;
}

TEST_F(SharedExclusiveLockTest, test_concurrency) {
    const int num_threads = 1000;
    const int work_iterations = 500;
    const int num_threads_to_remove = 500;
    int counter = 0;
    std::atomic<bool> start_flag{false};
    std::atomic<int> threads_ready{0};
    std::unordered_set<int> thread_ids_set;
    std::vector<pthread_t> threads(num_threads);
    std::vector<WorkerThreadData> thread_data(num_threads);

    // Create worker threads
    for (int i = 0; i < num_threads; ++i) {
        thread_data[i] = {
            ctx,
            &counter,
            &threads_ready,
            &start_flag,
            &thread_ids_set,
            i,
            work_iterations,
        };
        int rc = pthread_create(&threads[i], nullptr, worker_thread_func, &thread_data[i]);
        ASSERT_EQ(rc, 0) << "Failed to create thread " << i;
    }

    // Wait for all threads to be ready
    while (threads_ready.load() < num_threads) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    RedisModule_ThreadSafeContextLock(ctx);
    SharedExclusiveLock_SetOwned();

    // Start all threads simultaneously
    start_flag.store(true);

    // Wait for all threads to finish
    for (int i = 0; i < num_threads_to_remove; ++i) {
        int rc = pthread_join(threads[i], nullptr);
        ASSERT_EQ(rc, 0) << "Failed to join thread " << i;
    }
    SharedExclusiveLock_UnsetOwned();
    RedisModule_ThreadSafeContextUnlock(ctx);
    // Verify the total work done
    for (int i = num_threads_to_remove; i < num_threads; ++i) {
        int rc = pthread_join(threads[i], nullptr);
        ASSERT_EQ(rc, 0) << "Failed to join thread " << i;
    }
    ASSERT_EQ(counter, num_threads * work_iterations);

    // Verify that all threads were properly recorded in the set
    ASSERT_EQ(thread_ids_set.size(), num_threads) << "Not all thread IDs were recorded in the set";

    // Verify that each created thread ID is in the set
    for (int i = 0; i < num_threads; ++i) {
        ASSERT_TRUE(thread_ids_set.find(i) != thread_ids_set.end())
            << "Thread " << i << " was not found in the thread IDs set";
    }
}
