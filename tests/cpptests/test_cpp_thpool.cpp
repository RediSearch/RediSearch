/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "thpool/thpool.h"
#include <chrono>
#include <thread>
#include <stdarg.h>
#include <string>
#include <pthread.h>
#include <atomic>

/* ========================== UTILS ========================== */

#ifdef VERBOSE_UTESTS
static void LogCallback(const char *level, const char *fmt, ...) {
    std::string msg(1024, '\0');

    va_list ap;
    va_start(ap, fmt);
    std::vsnprintf(&msg[0], msg.size(), fmt, ap);
    std::cout << msg << std::endl;
    va_end(ap);
}
#else
static void LogCallback(const char *level, const char *fmt, ...) {/*do nothing*/}
#endif

void jobs_pull_wait(redisearch_thpool_t *thpool_p, size_t num_jobs) {
    while (redisearch_thpool_num_jobs_in_progress(thpool_p) < num_jobs) {
        usleep(1);
    }
}

/* ========================== TEST CLASS ========================== */

typedef struct {
    size_t num_threads;
    size_t num_high_priority_bias;
} ThpoolParams;

class PriorityThpoolTestBase : public testing::TestWithParam<ThpoolParams> {
public:
    redisearch_thpool_t *pool;
        virtual void SetUp() {
            ThpoolParams params = GetParam();
            // Thread pool with a single thread which is also high-priority bias that
            // runs high priority tasks before low priority tasks.
            this->pool = redisearch_thpool_create(params.num_threads, params.num_high_priority_bias, LogCallback, "test");
        }

        virtual void TearDown() {
            redisearch_thpool_destroy(this->pool);
        }
};
#define THPOOL_TEST_SUITE(CLASS_NAME, NUM_THREADS, NUM_HIGH_PRIORITY_BIAS) \
    class CLASS_NAME : public PriorityThpoolTestBase {}; \
    INSTANTIATE_TEST_SUITE_P(CLASS_NAME, \
                            CLASS_NAME, \
                            testing::Values(ThpoolParams{NUM_THREADS, NUM_HIGH_PRIORITY_BIAS}));

/* ========================== Callbacks ========================== */

// This job will run until we insert the terminate when empty job
void waitForAdminJobFunc(void *p) {
    redisearch_thpool_t *thpool_p = (redisearch_thpool_t *)p;

    // wait for the admin jobs to be pushed
    while (!redisearch_thpool_get_stats(thpool_p).admin_priority_pending_jobs) {
        usleep(1);
    }
};

void sleep_job_ms(void *p) {
    size_t *time_ms = (size_t *)p;
    std::this_thread::sleep_for(std::chrono::milliseconds(*time_ms));
}

void sleep_job_us(void *p) {
    size_t *time_us = (size_t *)p;
    std::this_thread::sleep_for(std::chrono::microseconds(*time_us));
};

struct test_struct {
    std::chrono::time_point<std::chrono::high_resolution_clock> *arr; // Pointer to the array of timestamps
    int index;                                                        // Index of the timestamp in the array
};

/* The purpose of the function is to sleep for 100ms and then set the timestamp
 * in the test_struct.
*/
void sleep_and_set(test_struct *ts) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ts->arr[ts->index] = std::chrono::high_resolution_clock::now();
}

/* ========================== NUM_THREADS = 1, NUM_HIGH_PRIORITY_BIAS = 1 ========================== */
THPOOL_TEST_SUITE(PriorityThpoolTestBasic, 1, 1)

/* The purpose of the test is to check that tasks with the same priority are handled
 * in FIFO manner. The test adds 10 tasks with low priority and checks that the
 * tasks are handled in the order they were added.
 */
TEST_P(PriorityThpoolTestBasic, AllLowPriority) {
    int array_len = 10;
    std::chrono::time_point<std::chrono::high_resolution_clock> arr[array_len];
    test_struct ts[array_len];
    for (int i = 0; i < array_len; i++) {
        ts[i].arr = arr;
        ts[i].index = i;
        redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[i], THPOOL_PRIORITY_LOW);
    }
    redisearch_thpool_wait(this->pool);
    for (int i = 0; i < array_len-1; i++) {
        ASSERT_LT(arr[i],  arr[i+1]);
    }
}

/* The purpose of the test is to check that tasks with the same priority are handled
 * in FIFO manner. The test adds 10 tasks with HIGH priority and checks that the
 * tasks are handled in the order they were added.
 */
TEST_P(PriorityThpoolTestBasic, AllHighPriority) {
    int array_len = 10;
    std::chrono::time_point<std::chrono::high_resolution_clock> arr[array_len];
    test_struct ts[array_len];
    for (int i = 0; i < array_len; i++) {
        ts[i].arr = arr;
        ts[i].index = i;
        redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[i], THPOOL_PRIORITY_HIGH);
    }
    redisearch_thpool_wait(this->pool);
    for (int i = 0; i < array_len-1; i++) {
        ASSERT_LT(arr[i],  arr[i+1]);
    }
}

/* The purpose of the test is to check that tasks with different priorities are handled
 * in FIFO manner. The test adds 2 tasks with high priority and 1 task with low priority between them
 * and checks that the high priority tasks are handled before the low priority task, since the
 * single thread in the pool is high priority bias and will prefer to take high priority tasks.
 */
TEST_P(PriorityThpoolTestBasic, HighLowHighTest) {
    int high_priority_tasks = 2;
    std::chrono::time_point<std::chrono::high_resolution_clock> arr[high_priority_tasks];
    std::chrono::time_point<std::chrono::high_resolution_clock> low_priority_timestamp;
    // Initialize the test_struct array
    test_struct ts[high_priority_tasks + 1];
    for (int i = 0; i < high_priority_tasks; i++) {
        ts[i].arr = arr;
        ts[i].index = i;
    }
    ts[high_priority_tasks] = {&low_priority_timestamp, 0};
    // The low priority task is added in the middle, but it should run after the high priority tasks
    ts[high_priority_tasks].arr = &low_priority_timestamp;
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[0], THPOOL_PRIORITY_HIGH);
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[2], THPOOL_PRIORITY_LOW);
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[1], THPOOL_PRIORITY_HIGH);

    redisearch_thpool_wait(this->pool);
    for (int i = 0; i < high_priority_tasks; i++) {
        ASSERT_LT(arr[i],  low_priority_timestamp);
    }
}

/* ========================== NUM_THREADS = 1, NUM_HIGH_PRIORITY_BIAS = 0 ========================== */
THPOOL_TEST_SUITE(PriorityThpoolTestWithoutBiasThreads, 1, 0)

TEST_P(PriorityThpoolTestWithoutBiasThreads, CombinationTest) {
    int total_tasks = 5;
    std::chrono::time_point<std::chrono::high_resolution_clock> arr[total_tasks];

    // Initialize the test_struct array
    test_struct ts[total_tasks];
    for (int i = 0; i < total_tasks; i++) {
        ts[i].arr = arr;
        ts[i].index = i;
    }

    // Pause the thread pool before adding tasks, to validate that jobs won't get executed before
    // all other jobs are inserted into the queue.
    redisearch_thpool_pause_threads(this->pool);

    // Fill the job queue with tasks.
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[0], THPOOL_PRIORITY_LOW);  // Prefers HIGH
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[1], THPOOL_PRIORITY_HIGH); // Prefers LOW
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[2], THPOOL_PRIORITY_HIGH); // Prefers HIGH
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[3], THPOOL_PRIORITY_HIGH); // Prefers LOW
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[4], THPOOL_PRIORITY_LOW);  // Prefers HIGH

    redisearch_thpool_resume_threads(this->pool);
    redisearch_thpool_wait(this->pool);

    // Expect alternate high-low order: 1->0->2->4->3
    ASSERT_LT(arr[1], arr[0]);
    ASSERT_LT(arr[0], arr[2]);
    ASSERT_LT(arr[2], arr[4]);
    ASSERT_LT(arr[4], arr[3]);
}

/* ========================== NUM_THREADS = 2, NUM_HIGH_PRIORITY_BIAS = 0 ========================== */
static constexpr size_t TEST_FUNCTIONALITY_N_THREADS = 2;
THPOOL_TEST_SUITE(PriorityThpoolTestFunctionality, TEST_FUNCTIONALITY_N_THREADS, 0)

/** This test scenario follows these steps:
 * 1. Push jobs to the queue.
 * 2. Change the state of running threads to TERMINATE_WHEN_EMPTY.
 * 3. Since the job queue is not empty, both threads continue to the next iteration.
 * 4. When only one job is left in the queue, one thread will pull it and the second pull attempt will result in a NULL job.
 *
 * Prior to the fix, when there was one job left in the queue, the other thread would get stuck,
 * waiting indefinitely for new jobs to arrive, and would never terminate.
*/
TEST_P(PriorityThpoolTestFunctionality, TestTerminateWhenEmpty) {
    typedef struct {
        volatile std::atomic<bool> &waitForSign;
        volatile std::atomic<size_t> &received;
    } MarkAndWait;

    // This job will keep the thread busy until we signal it to finish.
    auto MarkAndWaitForSignFunc = [](void *p) {
        MarkAndWait *mark_and_wait = (MarkAndWait *)p;
        ++mark_and_wait->received;
        while (mark_and_wait->waitForSign) {
            usleep(1);
        }
    };

    size_t time_us = 1;

    // Let the threads wait until we push change state jobs.
    for (int i = 0; i < TEST_FUNCTIONALITY_N_THREADS; i++) {
        redisearch_thpool_add_work(this->pool, waitForAdminJobFunc, this->pool, THPOOL_PRIORITY_HIGH);
    }

    // Wait for the jobs to be pulled.
    jobs_pull_wait(this->pool, TEST_FUNCTIONALITY_N_THREADS);

    // Push jobs to the queue to ensure the threads do not quit after their state is changed.
    // They will wait to make sure each thread takes one job.
    volatile std::atomic<bool> sign {true};
    volatile std::atomic<size_t> received {0};
    MarkAndWait mark_and_wait = {sign, received};
    for (int i = 0; i < TEST_FUNCTIONALITY_N_THREADS; i++) {
        redisearch_thpool_add_work(this->pool, MarkAndWaitForSignFunc, &mark_and_wait, THPOOL_PRIORITY_HIGH);
    }

    // Push another job so the queue will not be empty when the threads finish MarkAndWaitForSignFunc.
    // We will pause the job queue to ensure no thread is pulling it.
    redisearch_thpool_add_work(this->pool, sleep_job_us, &time_us, THPOOL_PRIORITY_HIGH);

    // Change the threads state.
    redisearch_thpool_terminate_when_empty(this->pool);
    ASSERT_FALSE(redisearch_thpool_is_initialized(this->pool));
    // Threads should still be alive.
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).num_threads_alive, TEST_FUNCTIONALITY_N_THREADS);

    // Wait for MarkAndWaitForSignFunc jobs to be pulled.
    while (mark_and_wait.received < TEST_FUNCTIONALITY_N_THREADS) {
        usleep(1);
    }

    ASSERT_EQ(redisearch_thpool_num_jobs_in_progress(this->pool), TEST_FUNCTIONALITY_N_THREADS);

    // Pause thpool. This has no effect when the thread is in terminate when empty state.
    // BAD SCENARIO: we need to pause the job queue to ensure both threads try to pull the last job.
    // Note that to reproduce the bad behaviour we need to introduce a wait function that doesn't wait
    // for 0 num_jobs_in_progress as the threads are waiting on the sign in MarkAndWaitForSignFunc.
    redisearch_thpool_pause_threads_no_wait(this->pool);

    // Let the threads finish MarkAndWaitForSignFunc job.
    sign = false;
    // Wait for them to finish the job.
    while (redisearch_thpool_num_jobs_in_progress(this->pool)) {
        usleep(1);
    }

    // Resume the thpool (should not have any effect since the pull function for the threads was changed)
    // BAD SCENARIO: one thread pulls the job and the other is stuck on job queue pull since it's empty.
    redisearch_thpool_resume_threads(this->pool);
    redisearch_thpool_wait(this->pool);

    // BAD SCENARIO: We are stuck since no all threads have terminated.
    // GOOD SCENARIO: Both threads finish the job and terminate.
    while (redisearch_thpool_get_stats(this->pool).num_threads_alive) {
        usleep(1);
    }
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).num_threads_alive, 0);

    // Recreate threads.
    redisearch_thpool_add_work(this->pool, sleep_job_us, &time_us, THPOOL_PRIORITY_HIGH);
    ASSERT_TRUE(redisearch_thpool_is_initialized(this->pool));

    while (redisearch_thpool_get_stats(this->pool).num_threads_alive < TEST_FUNCTIONALITY_N_THREADS) {
        usleep(1);
    }
}

TEST_P(PriorityThpoolTestFunctionality, TestPauseResume) {
    // Add job long enough so they won't finish until we call pause.
    size_t time_ms = 1000;
    redisearch_thpool_add_work(this->pool, sleep_job_ms, &time_ms, THPOOL_PRIORITY_HIGH);

    // Wait for the job to be pulled.
    jobs_pull_wait(this->pool, 1);

    // Pause threads.
    redisearch_thpool_pause_threads(this->pool);

    // The job should have finished.
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).total_jobs_done, 1);

    // There should not be any jobs in progress
    ASSERT_EQ(redisearch_thpool_num_jobs_in_progress(this->pool), 0);

}

/* ========================== NUM_THREADS = 5, NUM_HIGH_PRIORITY_BIAS = 0 ========================== */
static constexpr size_t RUNTIME_CONFIG_N_THREADS = 5;
THPOOL_TEST_SUITE(PriorityThpoolTestRuntimeConfig, RUNTIME_CONFIG_N_THREADS, 0)

TEST_P(PriorityThpoolTestRuntimeConfig, TestRemoveThreads) {
    // Add a job to trigger thpool initialization
    size_t total_jobs_pushed = 0;
    size_t time_ms = 1;
    redisearch_thpool_add_work(this->pool, sleep_job_ms, &time_ms, THPOOL_PRIORITY_HIGH);
    total_jobs_pushed++;

    // Expect num_threads_alive is 5
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).num_threads_alive, RUNTIME_CONFIG_N_THREADS);

    // Ensure that the first job has done and `num_jobs_in_progress` has already been updated
    redisearch_thpool_wait(this->pool);

    // remove 3 threads
    size_t n_threads_to_remove = 3;
    size_t n_threads = RUNTIME_CONFIG_N_THREADS - n_threads_to_remove;
    ASSERT_EQ(redisearch_thpool_remove_threads(this->pool, n_threads_to_remove), n_threads);
    // assert num threads alive is 2
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).num_threads_alive, n_threads);
    ASSERT_TRUE(redisearch_thpool_is_initialized(this->pool));

    // Let the thread wait until an admin job is pushed to the queue.
    for (int i = 0; i < n_threads; i++) {
        redisearch_thpool_add_work(this->pool, waitForAdminJobFunc, this->pool, THPOOL_PRIORITY_HIGH);
        total_jobs_pushed++;
    }

    // Wait for the jobs to be pulled.
    jobs_pull_wait(this->pool, n_threads);

    // Remove rest of the threads while the threads are waiting for the admin jobs to be pushed to the queue
    ASSERT_EQ(redisearch_thpool_remove_threads(this->pool, n_threads), 0);
    thpool_stats stats = redisearch_thpool_get_stats(this->pool);
    ASSERT_EQ(stats.num_threads_alive, 0);
    ASSERT_EQ(stats.total_jobs_done, total_jobs_pushed);
    ASSERT_EQ(stats.total_pending_jobs , 0);
}

TEST_P(PriorityThpoolTestRuntimeConfig, TestAddThreads) {
    // Add a job to trigger thpool initialization
    size_t total_jobs_pushed = 0;
    size_t time_ms = 1;
    redisearch_thpool_add_work(this->pool, sleep_job_ms, &time_ms, THPOOL_PRIORITY_HIGH);
    total_jobs_pushed++;

    // Expect num_threads_alive is 5.
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).num_threads_alive, RUNTIME_CONFIG_N_THREADS);

    // Add 3 threads.
    size_t n_threads_to_add = 3;
    size_t n_threads = RUNTIME_CONFIG_N_THREADS + n_threads_to_add;
    ASSERT_EQ(redisearch_thpool_add_threads(this->pool, n_threads_to_add), n_threads);
    // Expect num threads alive is n_threads.
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).num_threads_alive, n_threads);
    ASSERT_TRUE(redisearch_thpool_is_initialized(this->pool));

    // Ensure that the first job has done and `num_jobs_in_progress` has already been updated
    redisearch_thpool_wait(this->pool);
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).total_jobs_done, total_jobs_pushed);

}

static void ReinitializeThreadsWhileTerminateWhenEmpty(redisearch_thpool_t *thpool_p,
                                                       size_t n_threads_to_keep_alive,
                                                       size_t final_n_threads) {
    size_t total_jobs_pushed = 0;

    auto setThpoolThreadsNumber = [thpool_p, &final_n_threads]() -> size_t {
        if (final_n_threads < RUNTIME_CONFIG_N_THREADS) {
            return redisearch_thpool_remove_threads(thpool_p, RUNTIME_CONFIG_N_THREADS - final_n_threads);
        } else {
            return redisearch_thpool_add_threads(thpool_p, final_n_threads - RUNTIME_CONFIG_N_THREADS);
        }
    };

    /** The test goes as follows:
     * 1. Add n_threads jobs to keep the threads busy until we call terminate when empty,
     * to allow us push more jobs to the queue without the threads executing them.
     * (we are not pausing, pushing, continue because the threads might execute the jobs before we change their state)
     * 2. While the threads are waiting, add another n_threads_to_keep_alive jobs to ensure the jobq is not empty
     * after they change their state to TERMINATE_WHEN_EMPTY.
     * 3. call redisearch_thpool_terminate_when_empty().
     * 4. expect n_threads_to_keep_alive threads alive.
     * 5. Set the thpool->n_threads. This will only change thpool->n_threads, but won't affect the current running threads.
     * 6. Add jobs to the queue to trigger verify init.
     * 7. Expect final_n_threads threads in the pool */


    // Keep all the threads busy until while we call terminate when empty
    for (int i = 0; i < RUNTIME_CONFIG_N_THREADS; i++) {
        redisearch_thpool_add_work(thpool_p, waitForAdminJobFunc, thpool_p, THPOOL_PRIORITY_HIGH);
        ++total_jobs_pushed;
    }

    // Wait for the jobs to be pulled.
    while (redisearch_thpool_num_jobs_in_progress(thpool_p) < RUNTIME_CONFIG_N_THREADS) {
        usleep(1);
    }

    // Keep `n_threads_to_keep_alive` of the threads working and alive during remove threads + verify init
    for (int i = 0; i < n_threads_to_keep_alive; i++) {
        redisearch_thpool_add_work(thpool_p, waitForAdminJobFunc, thpool_p, THPOOL_PRIORITY_HIGH);
        ++total_jobs_pushed;
    }

    // The threads are still waiting in the first job.
    ASSERT_EQ(redisearch_thpool_num_jobs_in_progress(thpool_p), RUNTIME_CONFIG_N_THREADS);
    ASSERT_EQ(redisearch_thpool_get_stats(thpool_p).total_pending_jobs, n_threads_to_keep_alive);
    // The threads will wake up and pull the change state job (terminate when empty).
    // `n_threads_to_keep_alive` of them will pull another job and wait. The others will see an empty queue and exit.
    redisearch_thpool_terminate_when_empty(thpool_p);
    // Jobs done should be RUNTIME_CONFIG_N_THREADS from the first waitForAdminJobFunc batch.
    // `n_threads_to_keep_alive` are still waiting in the second batch of `waitForAdminJobFunc`.
    ASSERT_EQ(redisearch_thpool_get_stats(thpool_p).total_jobs_done, RUNTIME_CONFIG_N_THREADS);

    while (!((redisearch_thpool_get_stats(thpool_p).num_threads_alive == n_threads_to_keep_alive) &&
             redisearch_thpool_num_jobs_in_progress(thpool_p) == n_threads_to_keep_alive)) {
        usleep(1);
    }

    // Now change number of threads. This will only change thpool->n_threads, but won't affect the current running threads.
    ASSERT_EQ(setThpoolThreadsNumber(), final_n_threads);

    // Assert n_threads is as expected
    ASSERT_EQ(redisearch_thpool_get_num_threads(thpool_p), final_n_threads);

    // Assert nothing has changed
    ASSERT_EQ(redisearch_thpool_get_stats(thpool_p).num_threads_alive, n_threads_to_keep_alive);
    ASSERT_EQ(redisearch_thpool_num_jobs_in_progress(thpool_p), n_threads_to_keep_alive);

    // Now we add another job to trigger verify init.
    size_t time_us = 1;
    redisearch_thpool_add_work(thpool_p, sleep_job_us, &time_us, THPOOL_PRIORITY_HIGH);
    ++total_jobs_pushed;

    redisearch_thpool_wait(thpool_p);

    ASSERT_EQ(redisearch_thpool_get_stats(thpool_p).num_threads_alive, final_n_threads);
    ASSERT_EQ(redisearch_thpool_get_stats(thpool_p).total_jobs_done, total_jobs_pushed);
}

/** case 1.a curr_num_threads_alive >= n_threads
    original number = 5
    required n_threads = 2
    threads alive = 3 (more than we need). */
TEST_P(PriorityThpoolTestRuntimeConfig, TestReinitializeThreadsWhileTerminateWhenEmptyCase1a) {
    size_t constexpr n_threads_to_keep_alive = 3;
    size_t constexpr final_n_threads = 2;
    ReinitializeThreadsWhileTerminateWhenEmpty(this->pool, n_threads_to_keep_alive, final_n_threads);
}

/** case 1.b curr_num_threads_alive < n_threads
    original number = 5
    required n_threads = 3
    threads alive = 2 (less than we need). */
TEST_P(PriorityThpoolTestRuntimeConfig, TestReinitializeThreadsWhileTerminateWhenEmptyCase1b) {
    size_t constexpr n_threads_to_keep_alive = 2;
    size_t constexpr final_n_threads = 3;
    ReinitializeThreadsWhileTerminateWhenEmpty(this->pool, n_threads_to_keep_alive, final_n_threads);
}

/** case 1.b(2) curr_num_threads_alive < n_threads
    original number = 5
    required n_threads = 7
    threads alive = 3 (less than we need). */
TEST_P(PriorityThpoolTestRuntimeConfig, TestReinitializeThreadsWhileTerminateWhenEmptyCase1b2) {
    size_t constexpr n_threads_to_keep_alive = 3;
    size_t constexpr final_n_threads = RUNTIME_CONFIG_N_THREADS + 2;
    ReinitializeThreadsWhileTerminateWhenEmpty(this->pool, n_threads_to_keep_alive, final_n_threads);
}

/** This test show cases the scenario where we would like to set the number of the threads to 0,
 * without leaving unprocessed jobs in the queue.
 * To do that we retain the threadpool with minimal resources (1 thread) that will process the remaining jobs.
 * terminate_when_empty() sets the thpool state to UNINITIALIZED, so after calling it we can
 * decrease the value of n_threads without affecting the current running thread. */
TEST_P(PriorityThpoolTestRuntimeConfig, TestSetToZeroWhileTerminateWhenEmpty) {
    size_t total_jobs_pushed = 0;
    size_t time_us = 1;

    auto dummyJob = [](void *p) {};
    // Decrease number of threads to 1 while jobs are still in the queue.
    // These jobs will run while we kill excessive threads.
    for (int i = 0; i < RUNTIME_CONFIG_N_THREADS; i++) {
        redisearch_thpool_add_work(this->pool, waitForAdminJobFunc, this->pool, THPOOL_PRIORITY_HIGH);
        ++total_jobs_pushed;
    }

    // Wait for the jobs to be pulled.
    jobs_pull_wait(this->pool, RUNTIME_CONFIG_N_THREADS);

    redisearch_thpool_remove_threads(this->pool, RUNTIME_CONFIG_N_THREADS - 1);
    // push another dummy job to ensure the last thread finishes the job
    redisearch_thpool_add_work(this->pool, dummyJob, nullptr, THPOOL_PRIORITY_ADMIN);
    redisearch_thpool_wait(this->pool);
    thpool_stats stats = redisearch_thpool_get_stats(this->pool);
    ASSERT_EQ(stats.num_threads_alive, 1);
    ASSERT_EQ(stats.total_jobs_done, total_jobs_pushed);

    // Add a job to wait for terminate when empty.
    redisearch_thpool_add_work(this->pool, waitForAdminJobFunc, this->pool, THPOOL_PRIORITY_HIGH);
    ++total_jobs_pushed;

    jobs_pull_wait(this->pool, 1);

    // Add jobs to be done in TERMINATE_WHEN_EMPTY state.
    size_t n_jobs = 10;
    size_t sleep_us = 1;
    for (int i = 0; i < n_jobs; i++) {
        redisearch_thpool_add_work(this->pool, sleep_job_us, &sleep_us, THPOOL_PRIORITY_HIGH);
        ++total_jobs_pushed;
    }
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).total_pending_jobs , n_jobs);

    // Set to terminate when empty, still jobs in the queue.
    redisearch_thpool_terminate_when_empty(this->pool);
    ASSERT_EQ(redisearch_thpool_remove_threads(this->pool, 1), 0);
    // Wait for the jobs to be done.
    redisearch_thpool_wait(this->pool);
    // Expect 0 threads alive and n_jobs done.
    stats = redisearch_thpool_get_stats(this->pool);
    while (stats.num_threads_alive) {
        usleep(1);
        stats = redisearch_thpool_get_stats(this->pool);
    }
    ASSERT_EQ(stats.total_jobs_done, total_jobs_pushed);
    ASSERT_EQ(stats.total_pending_jobs, 0);
}

/** This test purpose is to show we can add threads to a pool that was set to 0 threads. */
TEST_P(PriorityThpoolTestRuntimeConfig, TestAddThreadsToEmptyPool) {
    size_t total_jobs_pushed = 0;
    // Add a job to trigger thpool initialization
    size_t time_us = 1;
    redisearch_thpool_add_work(this->pool, sleep_job_us, &time_us, THPOOL_PRIORITY_HIGH);
    ++total_jobs_pushed;
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).num_threads_alive, RUNTIME_CONFIG_N_THREADS);
    ASSERT_TRUE(redisearch_thpool_is_initialized(this->pool));
    redisearch_thpool_wait(this->pool);

    // Remove all threads.
    ASSERT_EQ(redisearch_thpool_remove_threads(this->pool, RUNTIME_CONFIG_N_THREADS), 0);
    // Threadpool is still initialized, but no threads are alive.
    ASSERT_TRUE(redisearch_thpool_is_initialized(this->pool));
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).num_threads_alive, 0);

    // Add threads.
    ASSERT_EQ(redisearch_thpool_add_threads(this->pool, RUNTIME_CONFIG_N_THREADS), RUNTIME_CONFIG_N_THREADS);
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).num_threads_alive, RUNTIME_CONFIG_N_THREADS);
    ASSERT_TRUE(redisearch_thpool_is_initialized(this->pool));

    // Validate the thpool functionality.
    redisearch_thpool_add_work(this->pool, sleep_job_us, &time_us, THPOOL_PRIORITY_HIGH);
    ++total_jobs_pushed;
    redisearch_thpool_wait(this->pool);
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).total_jobs_done, total_jobs_pushed);
}

/* ========================== NUM_THREADS = 2, NUM_HIGH_PRIORITY_BIAS = 1 ========================== */
THPOOL_TEST_SUITE(PriorityThpoolTestBiasAndNonBias, 2, 1)

TEST_P(PriorityThpoolTestBiasAndNonBias, TestTakingTasksAsBias) {
    size_t num_jobs = 5;

    // This job will keep the thread busy until we tell it to finish.
    auto waitForSignFunc = [](void *p) {
        bool *waitForSign = (bool *)p;
        while (*waitForSign) {
            usleep(1);
        }
    };
    // This job will signal the waiting job to finish.
    auto signalFunc = [](void *p) {
        bool *waitForSign = (bool *)p;
        *waitForSign = false;
    };
    // This job will count how many times it was taken with a specific priority.
    auto countFunc = [](void *p) {
        size_t *count = (size_t *)p;
        (*count)++;
    };
    // This job will check the state of the counts.
    struct state_test {
        size_t *countHigh;
        size_t *countLow;

        size_t expectedHigh;
        size_t expectedLow;
    };
    auto stateCheck = [](void *p) {
        state_test *state = (state_test *)p;
        ASSERT_EQ(*state->countHigh, state->expectedHigh);
        ASSERT_EQ(*state->countLow, state->expectedLow);
    };

    /* Scenario of the test:
     * 1. We add 2 waiting jobs with low priority, so both threads will be unbiased, and we can control
     *    which thread will take the next job.
     * 2. We add 5 count jobs with high priority and 5 count jobs with low priority.
     * 3. We add a state check job for each priority.
     * 4. Add a final job to release the other waiting thread (low priority job)
     * 5. Release one of the threads from the waiting job to execute the rest of the jobs.
     *
     * We expect that the unblocked thread will understand that it should be high priority biased and will take the high priority
     * jobs before the low priority jobs. therefore, the high priority state check is expected to see 5 high priority jobs
     * and 0 low priority jobs, and the low priority state check is expected to see 5 high priority jobs and 5 low priority jobs.
     *
     * Before the mechanism fix, the unblocked thread would assume it is unbiased because it sees there is one running thread
     * (num jobs in progress is not smaller than the high priority bias number), and would take a high priority job and
     * a low priority job alternately, and we would get to both state checks after executing all the count jobs.
     */

    bool sign1 = true, sign2 = true;
    size_t countHigh = 0, countLow = 0;
    // Add two jobs as a low priority job so the threads taking them will be the unbiased one.
    redisearch_thpool_add_work(this->pool, waitForSignFunc, &sign1, THPOOL_PRIORITY_LOW);
    redisearch_thpool_add_work(this->pool, waitForSignFunc, &sign2, THPOOL_PRIORITY_LOW);
    // Wait for the threads to take the jobs before adding any high priority jobs.
    jobs_pull_wait(this->pool, 2);
    // Add 5 count jobs for each priority.
    for (int i = 0; i < num_jobs; i++) {
        redisearch_thpool_add_work(this->pool, countFunc, &countHigh, THPOOL_PRIORITY_HIGH);
        redisearch_thpool_add_work(this->pool, countFunc, &countLow, THPOOL_PRIORITY_LOW);
    }
    // Add the state check job.
    state_test state1 = {&countHigh, &countLow, num_jobs, 0};
    state_test state2 = {&countHigh, &countLow, num_jobs, num_jobs};
    redisearch_thpool_add_work(this->pool, stateCheck, &state1, THPOOL_PRIORITY_HIGH);
    redisearch_thpool_add_work(this->pool, stateCheck, &state2, THPOOL_PRIORITY_LOW);
    // Add the signal job for the first sign.
    redisearch_thpool_add_work(this->pool, signalFunc, &sign1, THPOOL_PRIORITY_LOW);

    // Release the second sign and wait for the jobs to finish.
    sign2 = false;
    redisearch_thpool_wait(this->pool);
}
