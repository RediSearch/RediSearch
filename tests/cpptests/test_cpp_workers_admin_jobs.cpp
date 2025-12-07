/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "common.h"
#include "util/workers.h"
#include "info/global_stats.h"
#include "concurrent_ctx.h"
#include "config.h"
#include <unistd.h>
#include <atomic>

/**
 * Job that keeps thread busy until told to finish via flag
 * This allows us to keep threads occupied while we check the metric
 */

struct JobFlags {
    std::atomic<bool> started{false};
    std::atomic<bool> should_finish{false};
};

auto busyJobWithFlag= [](void *arg) {
    JobFlags *flags = (JobFlags *)arg;
    flags->started.store(true);

    // Keep thread busy until told to finish
    while (!flags->should_finish.load()) {
        usleep(1000);  // Sleep 1ms to avoid busy-wait
    }
};
/**
 * Test fixture - sets up/tears down workers thread pool
 */
class WorkersAdminJobsMetricTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create ConcurrentSearch required to call GlobalStats_GetMultiThreadingStats
        ConcurrentSearch_CreatePool(1);
    }

    void TearDown() override {
        ConcurrentSearch_ThreadPoolDestroy();
        // Tell any remaining jobs to finish in case test failed before telling them to finish
        for (size_t i = 0; i < initial_worker_count; i++) {
            flags[i].should_finish.store(true);
        }
    }
    constexpr static size_t initial_worker_count = 5;
    JobFlags flags[initial_worker_count];
};


/**
 * Validates that the metric correctly reports admin jobs count.
 */
TEST_F(WorkersAdminJobsMetricTest, MetricIncreasesOnThreadResize) {

    // Verify the metric starts at 0
    MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
    ASSERT_EQ(stats.workers_admin_priority_pending_jobs, 0);

    // Set configuration to 5 workers
    RSGlobalConfig.numWorkerThreads = initial_worker_count;
    workersThreadPool_SetNumWorkers();
    ASSERT_EQ(workersThreadPool_NumThreads(), 5);

    // Schedule busy jobs on all threads
    for (size_t i = 0; i < initial_worker_count; i++) {
        workersThreadPool_AddWork(busyJobWithFlag, &flags[i]);
    }

    // Wait for all jobs to start (threads are now busy)
    bool success = RS::WaitForCondition([&]() {
        for (size_t i = 0; i < initial_worker_count; i++) {
            if (!flags[i].started.load()) return false;
        }
        return true;
    });
    ASSERT_TRUE(success) << "Timeout waiting for jobs to start";

    // Reduce thread count by 2 via RSGlobalConfig
    // This will create 2 admin jobs to signal threads to terminate
    size_t threads_to_remove = 2;
    size_t new_worker_count = initial_worker_count - threads_to_remove;
    RSGlobalConfig.numWorkerThreads = new_worker_count;
    workersThreadPool_SetNumWorkers();  // This triggers admin job creation!
    ASSERT_EQ(workersThreadPool_NumThreads(), new_worker_count);

    // CHECK METRIC - should show 2 admin jobs pending
    success = RS::WaitForCondition([&]() {
        stats = GlobalStats_GetMultiThreadingStats();
        return stats.workers_admin_priority_pending_jobs == threads_to_remove;
    });
    ASSERT_TRUE(success) << "Timeout waiting for admin jobs to be created, current value: "
                         << stats.workers_admin_priority_pending_jobs;

    // Tell all jobs to finish
    for (size_t i = 0; i < initial_worker_count; i++) {
        flags[i].should_finish.store(true);
    }

    // Wait for metric to return to 0 with timeout
    success = RS::WaitForCondition([&]() {
        stats = GlobalStats_GetMultiThreadingStats();
        return stats.workers_admin_priority_pending_jobs == 0;
    });

    // CHECK METRIC - should return to 0
    ASSERT_TRUE(success) << "Timeout waiting for admin jobs to complete, current value: "
                         << stats.workers_admin_priority_pending_jobs;

}
