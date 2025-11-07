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

class SharedExclusiveLockTest : public ::testing::Test {
protected:
    void SetUp() override {
        SharedExclusiveLock_Init();
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
    std::atomic<int> *threads_finished;
    std::atomic<bool> *start_flag;
    std::unordered_set<int> *thread_ids_set;
    int thread_id;
    int work_iterations;
    int sleep_microseconds;
    int **shared_ptr;  // Shared pointer that each thread will allocate/free
};

// Worker thread function that tries to acquire the shared exclusive lock
void* worker_thread_func(void* arg) {
    WorkerThreadData* data = static_cast<WorkerThreadData*>(arg);

    data->threads_ready->fetch_add(1);
    // Wait for start signal
    while (!data->start_flag->load()) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    std::this_thread::sleep_for(std::chrono::microseconds(10 * data->sleep_microseconds)); // 0.1s
    // Try to acquire the lock and do some work
    SharedExclusiveLockType lock_type = SharedExclusiveLock_Acquire(data->ctx);
    data->thread_ids_set->insert(data->thread_id);

    // Allocate and free shared pointer - this will crash if there are race conditions
    *(data->shared_ptr) = (int*)malloc(sizeof(int) * 10);
    // Write to the allocated memory
    for (int i = 0; i < 10; ++i) {
        (*(data->shared_ptr))[i] = data->thread_id;
    }
    // Simulate some work
    for (int i = 0; i < data->work_iterations; ++i) {
        *(data->counter) += 1;
    }
    // Free the memory
    free(*(data->shared_ptr));
    *(data->shared_ptr) = nullptr;
    std::this_thread::sleep_for(std::chrono::microseconds(data->sleep_microseconds));

    // Release the lock
    SharedExclusiveLock_Release(data->ctx, lock_type);
    data->threads_finished->fetch_add(1);
    return nullptr;
}

// Parametrized test class for concurrency testing with different timing values
class SharedExclusiveLockParametrizedTest : public SharedExclusiveLockTest,
                                           public ::testing::WithParamInterface<size_t> {
};

TEST_P(SharedExclusiveLockParametrizedTest, test_concurrency) {
    const size_t param_value = GetParam();
    const int num_threads = 260;
    const int work_iterations = 50;
    const int num_threads_to_remove = 50;
    int counter = 0;
    int* shared_ptr = nullptr;  // Shared pointer for race condition detection
    std::atomic<bool> start_flag{false};
    std::atomic<int> threads_ready{0};
    std::unordered_set<int> thread_ids_set;
    std::atomic<int> threads_finished{0};
    std::vector<pthread_t> threads(num_threads);
    std::vector<WorkerThreadData> thread_data(num_threads);

    // Create worker threads
    for (int i = 0; i < num_threads / 2; ++i) {
        thread_data[i].ctx = ctx;
        thread_data[i].counter = &counter;
        thread_data[i].threads_ready = &threads_ready;
        thread_data[i].threads_finished = &threads_finished;
        thread_data[i].start_flag = &start_flag;
        thread_data[i].thread_ids_set = &thread_ids_set;
        thread_data[i].thread_id = i;
        thread_data[i].work_iterations = work_iterations;
        thread_data[i].sleep_microseconds = 10000; // 0.01s
        thread_data[i].shared_ptr = &shared_ptr;  // Point to shared counter

        int rc = pthread_create(&threads[i], nullptr, worker_thread_func, &thread_data[i]);
        ASSERT_EQ(rc, 0) << "Failed to create thread " << i;
    }

    // Wait for all threads to be ready
    while (threads_ready.load() < num_threads / 2) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    RedisModule_ThreadSafeContextLock(ctx);

    // Start all threads simultaneously
    start_flag.store(true);
    usleep(1000000); // 1s
    ASSERT_EQ(thread_ids_set.size(), 0) << "No thread could have acquired the lock, since the GIL is owned by the main thread.";
    SharedExclusiveLock_SetOwned();

    while (threads_finished.load() < num_threads_to_remove) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    ASSERT_GE(thread_ids_set.size(), num_threads_to_remove) << "At least num_threads_to_remove should have finished";
    for (int i = num_threads / 2; i < num_threads; ++i) {
        thread_data[i].ctx = ctx;
        thread_data[i].counter = &counter;
        thread_data[i].threads_ready = &threads_ready;
        thread_data[i].threads_finished = &threads_finished;
        thread_data[i].start_flag = &start_flag;
        thread_data[i].thread_ids_set = &thread_ids_set;
        thread_data[i].thread_id = i;
        thread_data[i].work_iterations = work_iterations;
        thread_data[i].sleep_microseconds = 10000; // 0.01s
        thread_data[i].shared_ptr = &shared_ptr;  // Point to shared counter

        int rc = pthread_create(&threads[i], nullptr, worker_thread_func, &thread_data[i]);
        ASSERT_EQ(rc, 0) << "Failed to create thread " << i;
    }
    // Wait for all threads to be ready
    while (threads_ready.load() < num_threads) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    SharedExclusiveLock_UnsetOwned();
    size_t thread_ids_set_size = thread_ids_set.size();
    ASSERT_LT(thread_ids_set_size, num_threads) << "Not all threads were able to acquire the lock";

    usleep(param_value);
    ASSERT_EQ(thread_ids_set_size, thread_ids_set.size()) << "Thread did not finish after UnsetOwned, while the GIL is not unlocked";
    RedisModule_ThreadSafeContextUnlock(ctx);
    // Verify the total work done
    while (threads_finished.load() < num_threads) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    ASSERT_EQ(counter, num_threads * work_iterations);

    // Verify that all threads were properly recorded in the set
    ASSERT_EQ(thread_ids_set.size(), num_threads) << "Not all thread IDs were recorded in the set";

    // Verify that each created thread ID is in the set
    for (int i = 0; i < num_threads; ++i) {
        ASSERT_TRUE(thread_ids_set.find(i) != thread_ids_set.end())
            << "Thread " << i << " was not found in the thread IDs set";
    }

    for (int i = 0; i < num_threads; ++i) {
        int rc = pthread_join(threads[i], nullptr);
        ASSERT_EQ(rc, 0) << "Failed to join thread " << i;
    }
}


// Structure to pass data to worker threads
struct JobsThreadData {
  RedisModuleCtx *ctx;
  int num_jobs;
  int *job_counter;
  std::atomic<int> *threads_ready;
  std::atomic<int> *jobs_finished;
  std::atomic<bool> *start_flag;
  int sleep_microseconds;
  int **shared_ptr;  // Shared pointer that each thread will allocate/free
};

// Worker thread function that tries to acquire the shared exclusive lock
void* worker_thread_jobs(void* arg) {
  JobsThreadData* data = static_cast<JobsThreadData*>(arg);

  data->threads_ready->fetch_add(1);
  // Wait for start signal
  while (!data->start_flag->load()) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
  }

  for (int i = 0; i < data->num_jobs; ++i) {
    std::this_thread::sleep_for(std::chrono::microseconds(10 * data->sleep_microseconds));
    // Try to acquire the lock and do some work
    SharedExclusiveLockType lock_type = SharedExclusiveLock_Acquire(data->ctx);
    // Allocate and free shared pointer - this will crash if there are race conditions
    *(data->shared_ptr) = (int*)malloc(sizeof(int) * 10);
    *(data->job_counter) += 1;
    // Write to the allocated memory
    for (int i = 0; i < 10; ++i) {
      (*(data->shared_ptr))[i] = i;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(data->sleep_microseconds));

    // Release the lock
    SharedExclusiveLock_Release(data->ctx, lock_type);
    data->jobs_finished->fetch_add(1);
  }
  return nullptr;
}

// This test is more representative of the real use case.
TEST_P(SharedExclusiveLockParametrizedTest, test_jobs) {
  const size_t param_value = GetParam();
  const int num_threads = 16;
  const int num_jobs_per_thread = 200;
  const int num_jobs_to_wait = 100;
  int job_counter = 0;
  int* shared_ptr = nullptr;  // Shared pointer for race condition detection
  std::atomic<bool> start_flag{false};
  std::atomic<int> threads_ready{0};
  std::unordered_set<int> thread_ids_set;
  std::atomic<int> jobs_finished{0};
  std::vector<pthread_t> threads(num_threads);
  std::vector<JobsThreadData> thread_data(num_threads);

  // Create worker threads
  for (int i = 0; i < num_threads; ++i) {
    thread_data[i] = {
        ctx,
        num_jobs_per_thread,
        &job_counter,
        &threads_ready,
        &jobs_finished,
        &start_flag,
        1000, // 0.001s
        &shared_ptr,
    };
    int rc = pthread_create(&threads[i], nullptr, worker_thread_jobs, &thread_data[i]);
    ASSERT_EQ(rc, 0) << "Failed to create thread " << i;
  }
  // Wait for all threads to be ready
  while (threads_ready.load() < num_threads) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
  }

  RedisModule_ThreadSafeContextLock(ctx);

  // Start all threads simultaneously
  start_flag.store(true);
  usleep(1000000);
  ASSERT_EQ(job_counter, 0) << "No job could have acquired the lock, since the GIL is owned by the main thread.";
  SharedExclusiveLock_SetOwned();

  while (jobs_finished.load() < num_jobs_to_wait) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
  }
  ASSERT_GE(job_counter, num_jobs_to_wait) << "At least num_jobs_to_wait should have finished";

  SharedExclusiveLock_UnsetOwned();
  int num_jobs_finished = jobs_finished.load();
  int num_jobs_executed = job_counter;

  usleep(param_value);

  ASSERT_EQ(num_jobs_executed, job_counter) << "No more jobs should have run after UnsetOwned before releasing the GIL";
  RedisModule_ThreadSafeContextUnlock(ctx);

  for (int i = 0; i < num_threads; ++i) {
      int rc = pthread_join(threads[i], nullptr);
      ASSERT_EQ(rc, 0) << "Failed to join thread " << i;
  }
  ASSERT_EQ(num_jobs_per_thread * num_threads, jobs_finished.load()) << "No more jobs should have finished after UnsetOwned before releasing the GIL";
  ASSERT_EQ(num_jobs_per_thread * num_threads, job_counter) << "No more jobs should have run after UnsetOwned before releasing the GIL";
}

// Instantiate the parametrized tests with different timing values (in microseconds)
INSTANTIATE_TEST_SUITE_P(
    TimingVariations,
    SharedExclusiveLockParametrizedTest,
    ::testing::Values(
        0,        // No delay
        1000000   // 1s
    )
);
