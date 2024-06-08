#include "gtest/gtest.h"
#include "thpool/thpool.h"
#include <chrono>
#include <thread>


typedef struct {
    size_t num_threads;
    size_t num_high_priority_bias;
} ThpoolParams;

class PriorityThpoolTestBase : public testing::TestWithParam<ThpoolParams> {
public:
    redisearch_threadpool pool;
        virtual void SetUp() {
            ThpoolParams params = GetParam();
            // Thread pool with a single thread which is also high-priority bias that
            // runs high priority tasks before low priority tasks.
            this->pool = redisearch_thpool_create(params.num_threads, params.num_high_priority_bias, nullptr, "test");
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

/* ========================== NUM_THREADS = 1, NUM_HIGH_PRIORITY_BIAS = 1 ========================== */
THPOOL_TEST_SUITE(PriorityThpoolTestBasic, 1, 1)

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
    redisearch_thpool_terminate_pause_threads(this->pool);

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

/* ========================== NUM_THREADS = 1, NUM_HIGH_PRIORITY_BIAS = 0 ========================== */
THPOOL_TEST_SUITE(PriorityThpoolTestFunctionality, 1, 0)

void sleep_1_and_set(size_t *time_ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(*time_ms));
}

TEST_P(PriorityThpoolTestFunctionality, TestTerminateWhenEmpty) {
    // Pause the thread pool before adding tasks, to validate that jobs won't get executed before
    // all other jobs are inserted into the queue.
    redisearch_thpool_terminate_pause_threads(this->pool);

    // Add a job to the jobq
    size_t time_ms = 1;
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_1_and_set, &time_ms, THPOOL_PRIORITY_HIGH);

    redisearch_thpool_resume_threads(this->pool);

    redisearch_thpool_terminate_when_empty(this->pool);
    ASSERT_FALSE(redisearch_thpool_is_initialized(this->pool)) << "expected thread pool to be uninitialized";

    // Wait for the job to be done.
    redisearch_thpool_wait(this->pool);

    // Wait for the threads to terminate.
    while (redisearch_thpool_get_stats(this->pool).num_threads_alive) {
        usleep(1);
    }

    // Recreate threads
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_1_and_set, &time_ms, THPOOL_PRIORITY_HIGH);
    ASSERT_TRUE(redisearch_thpool_is_initialized(this->pool)) << "expected thread pool to be initialized";

    while (!redisearch_thpool_get_stats(this->pool).num_threads_alive) {
        usleep(1);
    }

}

TEST_P(PriorityThpoolTestFunctionality, TestPauseResume) {
    // Add job long enough so they won't finish until we call pause.
    size_t time_ms = 1000;
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_1_and_set, &time_ms, THPOOL_PRIORITY_HIGH);

    // Wait for the job to be pulled.
    while (redisearch_thpool_num_jobs_in_progress(this->pool) != 1) {
        usleep(1);
    }

    // Pause threads.
    redisearch_thpool_terminate_pause_threads(this->pool);

    // The job should have finished.
    ASSERT_EQ(redisearch_thpool_get_stats(this->pool).total_jobs_done, 1) << "expected 1 job done";

    // There should not be any jobs in progress
    ASSERT_EQ(redisearch_thpool_num_jobs_in_progress(this->pool), 0) << "expected 0 working threads";

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
    while (redisearch_thpool_num_jobs_in_progress(this->pool) != 2) {
        usleep(1);
    }
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
