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
    std::atomic<int> *counter;
    std::atomic<bool> *start_flag;
    std::atomic<int> *threads_ready;
    std::atomic<int> *threads_finished;
    int thread_id;
    int work_iterations;
    std::vector<int> *execution_order;
    std::mutex *order_mutex;
};

// Worker thread function that tries to acquire the shared exclusive lock
void* worker_thread_func(void* arg) {
    WorkerThreadData* data = static_cast<WorkerThreadData*>(arg);

    // Signal that this thread is ready
    data->threads_ready->fetch_add(1);

    // Wait for start signal
    while (!data->start_flag->load()) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    // Try to acquire the lock and do some work
    SharedExclusiveLockType lock_type = SharedExclusiveLock_Acquire(data->ctx);
    data->execution_order->push_back(data->thread_id);

    // Simulate some work
    for (int i = 0; i < data->work_iterations; ++i) {
        data->counter->fetch_add(1);
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    // Signal that this thread is finished
    data->threads_finished->fetch_add(1);
    // Release the lock
    SharedExclusiveLock_Release(data->ctx, lock_type);
    return nullptr;
}

TEST_F(SharedExclusiveLockTest, MultipleThreadsWithGILAvailable) {
    const int num_threads = 500;
    const int work_iterations = 10;

    std::atomic<int> counter{0};
    std::atomic<bool> start_flag{false};
    std::atomic<int> threads_ready{0};
    std::atomic<int> threads_finished{0};
    std::vector<int> execution_order;
    std::mutex order_mutex;

    std::vector<pthread_t> threads(num_threads);
    std::vector<WorkerThreadData> thread_data(num_threads);

    // Create worker threads
    for (int i = 0; i < num_threads; ++i) {
        thread_data[i] = {
            ctx,
            &counter,
            &start_flag,
            &threads_ready,
            &threads_finished,
            i,
            work_iterations,
            &execution_order,
            &order_mutex
        };

        int rc = pthread_create(&threads[i], nullptr, worker_thread_func, &thread_data[i]);
        ASSERT_EQ(rc, 0) << "Failed to create thread " << i;
    }

    // Wait for all threads to be ready
    while (threads_ready.load() < num_threads) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    // Start all threads simultaneously
    start_flag.store(true);

    // Wait for all threads to finish
    for (int i = 0; i < num_threads; ++i) {
        int rc = pthread_join(threads[i], nullptr);
        ASSERT_EQ(rc, 0) << "Failed to join thread " << i;
    }

    // Verify all threads finished
    ASSERT_EQ(threads_finished.load(), num_threads);

    // Verify the total work done
    ASSERT_EQ(counter.load(), num_threads * work_iterations);

    // Verify execution order was recorded
    ASSERT_EQ(execution_order.size(), num_threads);
}

TEST_F(SharedExclusiveLockTest, MainThreadHoldsGILWorkerThreadsUseInternalLock) {
    const int num_threads = 3;
    const int work_iterations = 5;

    // Main thread claims the GIL by setting it as owned
    SharedExclusiveLock_SetOwned();

    std::atomic<int> counter{0};
    std::atomic<bool> start_flag{false};
    std::atomic<int> threads_ready{0};
    std::atomic<int> threads_finished{0};
    std::vector<int> execution_order;
    std::mutex order_mutex;

    std::vector<pthread_t> threads(num_threads);
    std::vector<WorkerThreadData> thread_data(num_threads);

    // Create worker threads
    for (int i = 0; i < num_threads; ++i) {
        thread_data[i] = {
            ctx,
            &counter,
            &start_flag,
            &threads_ready,
            &threads_finished,
            i,
            work_iterations,
            &execution_order,
            &order_mutex
        };

        int rc = pthread_create(&threads[i], nullptr, worker_thread_func, &thread_data[i]);
        ASSERT_EQ(rc, 0) << "Failed to create thread " << i;
    }

    // Wait for all threads to be ready
    while (threads_ready.load() < num_threads) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    // Start all threads simultaneously
    start_flag.store(true);

    // Main thread does some work while holding the GIL
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Release the GIL
    SharedExclusiveLock_UnsetOwned();

    // Wait for all threads to finish
    for (int i = 0; i < num_threads; ++i) {
        int rc = pthread_join(threads[i], nullptr);
        ASSERT_EQ(rc, 0) << "Failed to join thread " << i;
    }

    // Verify all threads finished
    ASSERT_EQ(threads_finished.load(), num_threads);

    // Verify the total work done
    ASSERT_EQ(counter.load(), num_threads * work_iterations);

    // Verify execution order was recorded
    ASSERT_EQ(execution_order.size(), num_threads);
}

// Test that simulates the basic functionality with serialized access
TEST_F(SharedExclusiveLockTest, SerializedAccess) {
    const int num_threads = 4;

    std::atomic<int> counter{0};
    std::atomic<bool> start_flag{false};
    std::atomic<int> threads_ready{0};
    std::atomic<int> threads_finished{0};
    std::vector<int> execution_order;
    std::mutex order_mutex;

    std::vector<pthread_t> threads(num_threads);
    std::vector<WorkerThreadData> thread_data(num_threads);

    // Create worker threads
    for (int i = 0; i < num_threads; ++i) {
        thread_data[i] = {
            ctx,
            &counter,
            &start_flag,
            &threads_ready,
            &threads_finished,
            i,
            1, // minimal work
            &execution_order,
            &order_mutex
        };

        int rc = pthread_create(&threads[i], nullptr, worker_thread_func, &thread_data[i]);
        ASSERT_EQ(rc, 0) << "Failed to create thread " << i;
    }

    // Wait for all threads to be ready
    while (threads_ready.load() < num_threads) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    // Start all threads simultaneously
    start_flag.store(true);

    // Wait for all threads to finish
    for (int i = 0; i < num_threads; ++i) {
        int rc = pthread_join(threads[i], nullptr);
        ASSERT_EQ(rc, 0) << "Failed to join thread " << i;
    }

    // Verify all threads finished
    ASSERT_EQ(threads_finished.load(), num_threads);

    // Verify some work was done
    ASSERT_GT(counter.load(), 0);

    // Verify execution order was recorded
    ASSERT_EQ(execution_order.size(), num_threads);
}
