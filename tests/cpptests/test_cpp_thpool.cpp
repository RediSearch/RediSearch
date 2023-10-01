#include "gtest/gtest.h"
#include "thpool/thpool.h"
#include <chrono>
#include <thread>

class PriorityThpoolTestBasic : public ::testing::Test {


    public:
    redisearch_threadpool pool;
        virtual void SetUp() {
            // Thread pool with a single thread which is also a "privileged thread" that
            // runs high priority tasks before low priority tasks.
            this->pool = redisearch_thpool_create(1, 1);
            redisearch_thpool_init(this->pool, nullptr);
        }

        virtual void TearDown() {
            redisearch_thpool_destroy(this->pool);
        }
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


/* The purpose of the test is to check that tasks with the same priority are handled 
 * in FIFO manner. The test adds 10 tasks with low priority and checks that the
 * tasks are handled in the order they were added.
 */
TEST_F(PriorityThpoolTestBasic, AllLowPriority) {
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
TEST_F(PriorityThpoolTestBasic, AllHighPriority) {
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
 * single thread in the pool is a privileged thread that is handling high priority tasks before low
 * priority tasks.
 */
TEST_F(PriorityThpoolTestBasic, HighLowHighTest) {
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


class PriorityThpoolTestWithoutPrivilegedThreads : public ::testing::Test {

   public:
    redisearch_threadpool pool;
    virtual void SetUp() {
        // Thread pool with one thread which is not a "privileged thread", meaning that
        // it runs high priority tasks and low priority alternately.
        this->pool = redisearch_thpool_create(1, 0);
    }

    virtual void TearDown() {
        redisearch_thpool_destroy(this->pool);
    }
};

TEST_F(PriorityThpoolTestWithoutPrivilegedThreads, CombinationTest) {
    int total_tasks = 5;
    std::chrono::time_point<std::chrono::high_resolution_clock> arr[total_tasks];

    // Initialize the test_struct array
    test_struct ts[total_tasks];
    for (int i = 0; i < total_tasks; i++) {
        ts[i].arr = arr;
        ts[i].index = i;
    }

    // Fill the job queue with tasks in advanced before initialization, to validate that jobs won't
    // get executed before all other jobs are inserted into the queue.
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[0], THPOOL_PRIORITY_LOW);
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[1], THPOOL_PRIORITY_HIGH);
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[2], THPOOL_PRIORITY_HIGH);
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[3], THPOOL_PRIORITY_HIGH);
    redisearch_thpool_add_work(this->pool, (void (*)(void *))sleep_and_set, (void *)&ts[4], THPOOL_PRIORITY_LOW);

    redisearch_thpool_init(this->pool, nullptr);
    redisearch_thpool_wait(this->pool);

    // Expect alternate high-low order: 1->0->2->4->3
    ASSERT_LT(arr[1], arr[0]);
    ASSERT_LT(arr[0], arr[2]);
    ASSERT_LT(arr[2], arr[4]);
    ASSERT_LT(arr[4], arr[3]);
}
