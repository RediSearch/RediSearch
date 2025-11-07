/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "benchmark/benchmark.h"
#include "util/shared_exclusive_lock.h"
#include "redismock/redismock.h"
#include "redismock/internal.h"
#include "redismock/util.h"
#include <pthread.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <mutex>

// Structure for SharedExclusiveLock benchmark data
struct SharedExclusiveLockBenchmarkData {
    RedisModuleCtx *ctx;
    std::atomic<int> *threads_ready;
    std::atomic<bool> *start_flag;
    int sleep_microseconds;
};

// Structure for regular mutex benchmark data
struct MutexBenchmarkData {
    std::mutex *mtx;
    std::atomic<int> *threads_ready;
    std::atomic<bool> *start_flag;
    int sleep_microseconds;
};

// Worker thread function using SharedExclusiveLock
void* shared_exclusive_lock_worker(void* arg) {
    SharedExclusiveLockBenchmarkData* data = static_cast<SharedExclusiveLockBenchmarkData*>(arg);

    data->threads_ready->fetch_add(1);
    // Wait for start signal
    while (!data->start_flag->load()) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    // Try to acquire the SharedExclusiveLock and do work
    SharedExclusiveLockType lock_type = SharedExclusiveLock_Acquire(data->ctx);

    // Optional sleep to simulate work duration
    if (data->sleep_microseconds > 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(data->sleep_microseconds));
    }

    // Release the lock
    SharedExclusiveLock_Release(data->ctx, lock_type);
    return nullptr;
}

// Worker thread function using regular mutex
void* mutex_worker(void* arg) {
    MutexBenchmarkData* data = static_cast<MutexBenchmarkData*>(arg);

    data->threads_ready->fetch_add(1);
    // Wait for start signal
    while (!data->start_flag->load()) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    // Acquire mutex and do work
    std::lock_guard<std::mutex> lock(*data->mtx);

    // Optional sleep to simulate work duration
    if (data->sleep_microseconds > 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(data->sleep_microseconds));
    }

    return nullptr;
}

class BM_SharedExclusiveLockVsMutex : public benchmark::Fixture {
public:
    RedisModuleCtx *ctx;
    static bool initialized;

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            SharedExclusiveLock_Init();
            ctx = new RedisModuleCtx();
            initialized = true;
        }
    }

    void TearDown(::benchmark::State &state) {
        // Note: We don't destroy here as it might be used by other benchmark instances
    }

    static void GlobalTearDown() {
        if (initialized) {
            SharedExclusiveLock_Destroy();
            initialized = false;
        }
    }
};

bool BM_SharedExclusiveLockVsMutex::initialized = false;

// Benchmark using SharedExclusiveLock coordination
BENCHMARK_DEFINE_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLock)(benchmark::State& state) {
    const int num_threads = state.range(0);
    const int sleep_microseconds = state.range(1);

    for (auto _ : state) {
        std::atomic<int> threads_ready{0};
        std::atomic<bool> start_flag{false};

        std::vector<pthread_t> threads(num_threads);
        std::vector<SharedExclusiveLockBenchmarkData> thread_data(num_threads);

        // Create worker threads
        for (int i = 0; i < num_threads; ++i) {
            thread_data[i] = {
                ctx,
                &threads_ready,
                &start_flag,
                sleep_microseconds
            };
            int rc = pthread_create(&threads[i], nullptr, shared_exclusive_lock_worker, &thread_data[i]);
            if (rc != 0) {
                state.SkipWithError("Failed to create thread");
                return;
            }
        }

        // Wait for all threads to be ready
        while (threads_ready.load() < num_threads) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }

        // Signal threads to start
        start_flag.store(true);

        // Wait for all threads to complete
        for (int i = 0; i < num_threads; ++i) {
            pthread_join(threads[i], nullptr);
        }
    }
}

// Benchmark using regular mutex coordination
BENCHMARK_DEFINE_F(BM_SharedExclusiveLockVsMutex, RegularMutex)(benchmark::State& state) {
    const int num_threads = state.range(0);
    const int sleep_microseconds = state.range(1);

    for (auto _ : state) {
        std::atomic<int> threads_ready{0};
        std::atomic<bool> start_flag{false};
        std::mutex mtx;

        std::vector<pthread_t> threads(num_threads);
        std::vector<MutexBenchmarkData> thread_data(num_threads);

        // Create worker threads
        for (int i = 0; i < num_threads; ++i) {
            thread_data[i] = {
                &mtx,
                &threads_ready,
                &start_flag,
                sleep_microseconds
            };
            int rc = pthread_create(&threads[i], nullptr, mutex_worker, &thread_data[i]);
            if (rc != 0) {
                state.SkipWithError("Failed to create thread");
                return;
            }
        }

        // Wait for all threads to be ready
        while (threads_ready.load() < num_threads) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }

        // Signal threads to start
        start_flag.store(true);

        // Wait for all threads to complete
        for (int i = 0; i < num_threads; ++i) {
            pthread_join(threads[i], nullptr);
        }
    }
}

// Benchmark using SharedExclusiveLock coordination with main thread holding lock initially
BENCHMARK_DEFINE_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLockWhileOwned)(benchmark::State& state) {
    const int num_threads = state.range(0);
    const int sleep_microseconds = state.range(1);

    for (auto _ : state) {
        std::atomic<int> threads_ready{0};
        std::atomic<bool> start_flag{false};

        std::vector<pthread_t> threads(num_threads);
        std::vector<SharedExclusiveLockBenchmarkData> thread_data(num_threads);

        // Create worker threads
        for (int i = 0; i < num_threads; ++i) {
            thread_data[i] = {
                ctx,
                &threads_ready,
                &start_flag,
                sleep_microseconds
            };
            int rc = pthread_create(&threads[i], nullptr, shared_exclusive_lock_worker, &thread_data[i]);
            if (rc != 0) {
                state.SkipWithError("Failed to create thread");
                return;
            }
        }

        // Wait for all threads to be ready
        while (threads_ready.load() < num_threads) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }

        // Set the lock to owned state before starting threads
        SharedExclusiveLock_SetOwned();

        // Signal threads to start
        start_flag.store(true);

        // Wait for all threads to complete
        for (int i = 0; i < num_threads; ++i) {
            pthread_join(threads[i], nullptr);
        }

        SharedExclusiveLock_UnsetOwned();
    }
}

// Register benchmarks with different configurations
// Arguments: (num_threads, sleep_microseconds)

// Light workload - few threads, minimal work, no sleep
BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLock)
    ->Args({4, 0})
    ->Args({8, 0})
    ->Args({16, 0})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, RegularMutex)
    ->Args({4, 0})
    ->Args({8, 0})
    ->Args({16, 0})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLockWhileOwned)
    ->Args({4, 0})
    ->Args({8, 0})
    ->Args({16, 0})
    ->Unit(benchmark::kMillisecond);

// Light workload with sleep - few threads, no sleep
BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLock)
    ->Args({4, 100})
    ->Args({8, 100})
    ->Args({16, 100})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, RegularMutex)
    ->Args({4, 100})
    ->Args({8, 100})
    ->Args({16, 100})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLockWhileOwned)
  ->Args({4, 100})
  ->Args({8, 100})
  ->Args({16, 100})
  ->Unit(benchmark::kMillisecond);

// Medium workload - more threads, small sleep
BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLock)
    ->Args({16, 100})
    ->Args({32, 100})
    ->Args({64, 100})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, RegularMutex)
    ->Args({16, 100})
    ->Args({32, 100})
    ->Args({64, 100})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLockWhileOwned)
    ->Args({16, 100})
    ->Args({32, 100})
    ->Args({64, 100})
    ->Unit(benchmark::kMillisecond);

// Heavy workload - many threads, longer sleep
BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLock)
    ->Args({64, 1000})
    ->Args({128, 1000})
    ->Args({256, 1000})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, RegularMutex)
    ->Args({64, 1000})
    ->Args({128, 1000})
    ->Args({256, 1000})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(BM_SharedExclusiveLockVsMutex, SharedExclusiveLockWhileOwned)
    ->Args({64, 1000})
    ->Args({128, 1000})
    ->Args({256, 1000})
    ->Unit(benchmark::kMillisecond);

// Cleanup function to be called at program exit
static void cleanup() {
    BM_SharedExclusiveLockVsMutex::GlobalTearDown();
}

BENCHMARK_MAIN();
