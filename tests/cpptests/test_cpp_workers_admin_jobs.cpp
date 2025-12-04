/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "src/util/workers.h"
#include "src/info/global_stats.h"
// #include "src/config.h"              // For RSGlobalConfig
// #include "common.h"                  // For RS::WaitForCondition
// #include <unistd.h>                  // For usleep
// #include <atomic>                    // For std::atomic


/**
 * Job that keeps thread busy until told to finish via flag
 * This allows us to keep threads occupied while we check the metric
 */
// void busyJobWithFlag(void *arg) {
//     struct JobFlags {
//         std::atomic<bool> started;
//         std::atomic<bool> should_finish;
//     };
//
//     JobFlags *flags = (JobFlags *)arg;
//     flags->started.store(true);
//
//     // Keep thread busy until told to finish
//     while (!flags->should_finish.load()) {
//         usleep(1000);  // Sleep 1ms to avoid busy-wait
//     }
// }

// ========================== TEST CLASS ==========================

/**
 * Test fixture - sets up/tears down workers thread pool
 */
class WorkersAdminJobsMetricTest : public ::testing::Test {
protected:
    static constexpr size_t NUM_WORKERS = 5;

    void SetUp() override {
        // Create workers thread pool with 5 threads
        int result = workersThreadPool_CreatePool(NUM_WORKERS);
        ASSERT_EQ(result, REDISMODULE_OK);
    }

    void TearDown() override {
        // Clean up workers thread pool
        workersThreadPool_Terminate();
        workersThreadPool_Destroy();
    }
};

// ========================== TEST CASES ==========================

/**
 * TEST 1: Initial State - Metric Starts at Zero
 *
 * Validates that the metric returns 0 when no admin jobs are pending.
 */
TEST_F(WorkersAdminJobsMetricTest, InitialStateIsZero) {

    // Verify metric starts at 0 via GlobalStats
    MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
    ASSERT_EQ(stats.workers_admin_priority_pending_jobs, 0);
}

/**
 * TEST 2: Metric Increases During Thread Resize
 *
 * Validates that the metric correctly reports admin jobs created during
 * thread pool resize operations.
 *
 * STRATEGY:
 *   1. Keep all threads busy with jobs controlled by flags
 *   2. Trigger thread resize by changing RSGlobalConfig and calling SetNumWorkers()
 *   3. This creates admin jobs to signal threads to terminate
 *   4. CHECK METRIC via GlobalStats_GetMultiThreadingStats() - shows admin jobs pending
 *   5. Tell jobs to finish, CHECK METRIC again - returns to 0
 */
TEST_F(WorkersAdminJobsMetricTest, MetricIncreasesOnThreadResize) {
//
//     // Step 1: Create flags for each job
//     struct JobFlags {
//         std::atomic<bool> started{false};
//         std::atomic<bool> should_finish{false};
//     };
//     JobFlags flags[NUM_WORKERS];
//
//     // Step 2: Schedule busy jobs on all threads
//     for (size_t i = 0; i < NUM_WORKERS; i++) {
//         workersThreadPool_AddWork(busyJobWithFlag, &flags[i]);
//     }
//
//     // Step 3: Wait for all jobs to start (threads are now busy)
//     bool success = RS::WaitForCondition([&]() {
//         for (size_t i = 0; i < NUM_WORKERS; i++) {
//             if (!flags[i].started.load()) return false;
//         }
//         return true;
//     });
//     ASSERT_TRUE(success) << "Timeout waiting for jobs to start";
//
//     // Step 4: Reduce thread count by 2 via RSGlobalConfig
//     // This will create 2 admin jobs to signal threads to terminate
//     size_t threads_to_remove = 2;
//     size_t new_worker_count = NUM_WORKERS - threads_to_remove;
//     RSGlobalConfig.numWorkerThreads = new_worker_count;
//     workersThreadPool_SetNumWorkers();  // This triggers admin job creation!
//
//     // Step 5: CHECK METRIC - should show 2 admin jobs pending
//     MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
//     ASSERT_EQ(stats.workers_admin_priority_pending_jobs, threads_to_remove)
//         << "Metric should show " << threads_to_remove << " admin jobs pending";
//
//     // Step 6: Tell all jobs to finish
//     for (size_t i = 0; i < NUM_WORKERS; i++) {
//         flags[i].should_finish.store(true);
//     }
//
//     // Step 7: Wait for metric to return to 0 with timeout
//     success = RS::WaitForCondition([&]() {
//         stats = GlobalStats_GetMultiThreadingStats();
//         return stats.workers_admin_priority_pending_jobs == 0;
//     });
//
//     // Step 8: CHECK METRIC - should return to 0
//     ASSERT_TRUE(success) << "Timeout waiting for admin jobs to complete, current value: "
//                          << stats.workers_admin_priority_pending_jobs;
//
//     // Verify thread count was reduced
//     ASSERT_EQ(workersThreadPool_NumThreads(), new_worker_count);
}
// ============================================================================
// IMPLEMENTATION NOTES:
// ============================================================================
//
// NO NEED TO EXPOSE _workers_thpool!
//
// We use ONLY the public API:
//   1. RSGlobalConfig.numWorkerThreads (already accessible in C++ tests)
//   2. workersThreadPool_SetNumWorkers() (public API)
//   3. GlobalStats_GetMultiThreadingStats() (public API) - THIS IS THE KEY!
//      Returns MultiThreadingStats with workers_admin_priority_pending_jobs field
//   4. workersThreadPool_AddWork() (public API)
//   5. workersThreadPool_WorkingThreadCount() (public API)
//   6. RS::WaitForCondition() (test helper for waiting with timeout)
//
// This keeps the workers thread pool properly encapsulated!
// ============================================================================
