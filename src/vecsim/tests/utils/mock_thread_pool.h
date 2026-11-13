/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <functional>
#include <cmath>
#include <exception>
#include <thread>
#include <condition_variable>
#include <bitset>

#include "VecSim/vec_sim.h"
#include "VecSim/algorithms/hnsw/hnsw_tiered.h"

// MAX_POOL_SIZE and pool_execution_mask_t to be adjusted to each other.
static const size_t MAX_POOL_SIZE = 16;
using pool_execution_mask_t = std::atomic_uint_fast16_t;

class tieredIndexMock {

private:
    // A bit set to help determine whether all jobs are done by checking
    // that the job queue is empty and the all the bits are 0.
    // Each thread is associated with a bit position in the bit set.
    // The thread's corresponding bit should be set to before the job is popped
    // from the queue and the execution starts.
    // We turn the bit off after the execute callback returns to mark the job is done.
    class ExecutionsStatus {
        pool_execution_mask_t executions_status = 0; // Using atomic for thread safety
    public:
        void MarkInProcess(size_t thread_index) {
            executions_status.fetch_or(1 << thread_index, std::memory_order_relaxed);
        }
        void MarkDone(size_t thread_index) {
            executions_status.fetch_and(~(1 << thread_index), std::memory_order_relaxed);
        }
        bool AllDone() const { return executions_status.load(std::memory_order_relaxed) == 0; }
    };

    ExecutionsStatus executions_status;

    typedef struct RefManagedJob {
        AsyncJob *job;
        std::weak_ptr<VecSimIndex> index_weak_ref;
    } RefManagedJob;

public:
    struct SearchJobMock : public AsyncJob {
        void *query; // The query vector. ownership is passed to the job in the constructor.
        size_t k;    // The number of results to return.
        size_t n;    // The number of vectors in the index (might be useful for the mock)
        size_t dim;  // The dimension of the vectors in the index (might be useful for the mock)
        std::atomic_int *successful_searches; // A reference to a shared counter that counts the
                                              // number of successful searches.
        size_t ef;
        size_t iter;                    // For benchmarks, the number of iteration
        VecSimQueryReply **all_results; // For benchmarks, an array to store the results in

        /* Note that some members are not relevant for certain use-cases of the SearchJobMock,
           so we use default values that indicates that the member is in use only if an actual
           value is sent to the contractor (for example, we use dim in some unit tests, but in
           benchmarks this is irrelevant). */

        // To be used currently in unit tests.
        SearchJobMock(std::shared_ptr<VecSimAllocator> allocator, JobCallback searchCB,
                      VecSimIndex *index_, size_t k_, void *query_, size_t n_, size_t dim_,
                      std::atomic_int *successful_searches_)
            : AsyncJob(allocator, HNSW_SEARCH_JOB, searchCB, index_), query(query_), k(k_), n(n_),
              dim(dim_), successful_searches(successful_searches_), ef(-1), iter(-1),
              all_results(nullptr) {}

        // To be used currently in micro-benchmarks tests.
        SearchJobMock(std::shared_ptr<VecSimAllocator> allocator, JobCallback searchCB,
                      VecSimIndex *index_, size_t k_, size_t ef_, size_t iter_,
                      VecSimQueryReply **all_results_)
            : AsyncJob(allocator, HNSW_SEARCH_JOB, searchCB, index_), query(nullptr), k(k_), n(-1),
              dim(-1), successful_searches(nullptr), ef(ef_), iter(iter_),
              all_results(all_results_) {}

        ~SearchJobMock() { this->allocator->free_allocation(query); }
    };

    struct JobQueue : public std::queue<RefManagedJob> {
        // Pops and destroys the job at the front of the queue.
        inline void kick() {
            delete this->front().job;
            this->pop();
        }
    };

    typedef struct IndexExtCtx {
        std::shared_ptr<VecSimIndex> index_strong_ref;
        tieredIndexMock *mock_thread_pool;

        explicit IndexExtCtx(tieredIndexMock *mock_tp) : mock_thread_pool(mock_tp) {}
    } IndexExtCtx;

    size_t thread_pool_size;
    std::vector<std::thread> thread_pool;
    std::mutex queue_guard;
    std::condition_variable queue_cond;
    JobQueue jobQ;
    bool run_thread;
    IndexExtCtx *ctx;

    tieredIndexMock(unsigned int num_threads = 8U);
    ~tieredIndexMock();

    void reset_ctx(IndexExtCtx *new_ctx = nullptr);

    static int submit_callback(void *job_queue, void *index_ctx, AsyncJob **jobs, JobCallback *CBs,
                               size_t jobs_len);

    int submit_callback_internal(AsyncJob **jobs, JobCallback *CBs, size_t jobs_len);

    void init_threads();

    // A single iteration of the thread main loop.
    void thread_iteration(int thread_id = 0, const bool *run_thread = nullptr);
    static void thread_main_loop(int thread_id, tieredIndexMock &mock_thread_pool);

    void thread_pool_join();
    void thread_pool_wait(size_t waiting_duration = 10);
};
