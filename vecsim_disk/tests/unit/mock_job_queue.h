/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

/**
 * @file mock_job_queue.h
 * @brief Mock job queue for testing TieredHNSWDiskIndex async job submission.
 *
 * This mock captures submitted jobs without executing them, allowing tests to:
 * - Verify job submission
 * - Control when jobs are executed
 * - Execute jobs concurrently from multiple threads
 */

#include "VecSim/vec_sim_common.h"
#include "algorithms/hnsw/hnsw_disk_tiered.h"

#include <mutex>
#include <thread>
#include <vector>

namespace test_utils {

/**
 * @brief Mock job queue that captures async jobs for testing.
 *
 * Thread-safe queue that stores submitted jobs without executing them.
 * Tests can then execute jobs manually using executeAll() or by calling
 * job->Execute(job) directly for fine-grained control.
 */
struct MockJobQueue {
    std::vector<AsyncJob*> jobs;
    mutable std::recursive_mutex jobs_mutex;

    void submitJob(AsyncJob* job) {
        std::lock_guard<std::recursive_mutex> lock(jobs_mutex);
        jobs.push_back(job);
    }

    void submitJobs(const std::vector<AsyncDiskJob*>& job_list) {
        std::lock_guard<std::recursive_mutex> lock(jobs_mutex);
        for (auto* job : job_list) {
            jobs.push_back(job);
        }
    }

    size_t size() const {
        std::lock_guard<std::recursive_mutex> lock(jobs_mutex);
        return jobs.size();
    }

    AsyncJob* takeLastJob() {
        std::lock_guard<std::recursive_mutex> lock(jobs_mutex);
        AsyncJob* ret = jobs.empty() ? nullptr : jobs.back();
        if (ret) {
            jobs.pop_back();
        }
        return ret;
    }

    void clear() {
        std::lock_guard<std::recursive_mutex> lock(jobs_mutex);
        jobs.clear();
    }

    /**
     * @brief Take a job from the queue, removing it.
     * Returns the raw pointer and removes it from the queue.
     * Caller is responsible for execution. Returns nullptr if index is out of bounds.
     */
    AsyncJob* takeJob(size_t index) {
        std::lock_guard<std::recursive_mutex> lock(jobs_mutex);
        if (index >= jobs.size()) {
            return nullptr;
        }
        AsyncJob* job = jobs[index];
        // Swap with back and pop - O(1) instead of O(n)
        std::swap(jobs[index], jobs.back());
        jobs.pop_back();
        return job;
    }

    /**
     * @brief Execute all pending jobs sequentially, draining the queue.
     *
     * Jobs submitted during execution (e.g., repair jobs from inserts)
     * are also executed. Execution happens without holding the lock,
     * allowing new jobs to be submitted during execution.
     */
    void executeAll() {
        while (true) {
            AsyncJob* job = nullptr;
            {
                std::lock_guard<std::recursive_mutex> lock(jobs_mutex);
                if (jobs.empty()) {
                    break;
                }
                job = jobs.front();
                jobs.erase(jobs.begin());
            }
            // Execute without holding lock - allows new jobs to be submitted
            if (job) {
                job->Execute(job);
            }
        }
    }

    /**
     * @brief Execute all pending jobs concurrently from separate threads.
     *
     * Spawns one thread per job and executes them all simultaneously.
     * Useful for testing concurrent access and lock correctness.
     * Clears the job queue before spawning threads.
     */
    void executeAllConcurrently() {
        std::vector<AsyncJob*> jobs_to_execute;
        {
            std::lock_guard<std::recursive_mutex> lock(jobs_mutex);
            jobs_to_execute = jobs;
            jobs.clear();
        }

        std::vector<std::thread> workers;
        workers.reserve(jobs_to_execute.size());
        for (auto* job : jobs_to_execute) {
            workers.emplace_back([job]() { job->Execute(job); });
        }

        for (auto& t : workers) {
            t.join();
        }
    }
};

/**
 * @brief Submit callback function matching SubmitCB signature.
 *
 * @param job_queue Unused (for compatibility with real thread pool)
 * @param index_ctx Pointer to MockJobQueue instance
 * @param jobs Array of jobs to submit
 * @param CBs Unused (job callbacks)
 * @param jobs_len Number of jobs in the array
 * @return VecSim_OK on success
 */
inline int mockSubmitCallback(void* job_queue, void* index_ctx, AsyncJob** jobs, JobCallback* CBs, size_t jobs_len) {
    auto* queue = static_cast<MockJobQueue*>(index_ctx);
    {
        std::lock_guard<std::recursive_mutex> lock(queue->jobs_mutex);
        for (size_t i = 0; i < jobs_len; ++i) {
            queue->jobs.push_back(jobs[i]);
        }
    }
    return VecSim_OK;
}

} // namespace test_utils
