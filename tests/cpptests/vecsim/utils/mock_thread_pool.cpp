/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "mock_thread_pool.h"

/*
 * Mock callbacks and synchronization primitives for testing async tiered index.
 * We use a simple std::queue to simulate the job queue.
 */

tieredIndexMock::tieredIndexMock(unsigned int num_threads) : run_thread(true) {
    thread_pool_size = std::min(num_threads, std::thread::hardware_concurrency());
    ctx = new IndexExtCtx(this);
}

tieredIndexMock::~tieredIndexMock() {
    if (!thread_pool.empty()) {
        thread_pool_join();
    }
    if (ctx) {
        // We must hold the allocator, so it won't deallocate itself.
        auto allocator = ctx->index_strong_ref->getAllocator();
        delete ctx;
    }
}

void tieredIndexMock::reset_ctx(IndexExtCtx *new_ctx) {
    if (!thread_pool.empty()) {
        thread_pool_join();
    }
    delete ctx;
    ctx = new_ctx;
}

int tieredIndexMock::submit_callback_internal(AsyncJob **jobs, JobCallback *CBs, size_t jobs_len) {
    {
        std::unique_lock<std::mutex> lock(this->queue_guard);
        for (size_t i = 0; i < jobs_len; i++) {
            // Wrap the job with a struct that contains a weak reference to the related index.
            auto owned_job =
                RefManagedJob{.job = jobs[i], .index_weak_ref = this->ctx->index_strong_ref};
            this->jobQ.push(owned_job);
        }
    }
    if (jobs_len == 1) {
        queue_cond.notify_one();
    } else {
        queue_cond.notify_all();
    }
    return VecSim_OK;
}

int tieredIndexMock::submit_callback(void *job_queue, void *index_ctx, AsyncJob **jobs,
                                     JobCallback *CBs, size_t len) {
    return reinterpret_cast<IndexExtCtx *>(index_ctx)->mock_thread_pool->submit_callback_internal(
        jobs, CBs, len);
}

void tieredIndexMock::init_threads() {
    run_thread = true;
    for (size_t i = 0; i < thread_pool_size; i++) {
        thread_pool.emplace_back(tieredIndexMock::thread_main_loop, i, std::ref(*this));
    }
}

// If `run_thread` is null, treat it as `true`.
void tieredIndexMock::thread_iteration(int thread_id, const bool *run_thread_ptr) {
    std::unique_lock<std::mutex> lock(queue_guard);
    // Wake up and acquire the lock (atomically) ONLY if the job queue is not empty at that
    // point, or if the thread should not run anymore (and quit in that case).
    queue_cond.wait(lock, [&]() { return !jobQ.empty() || (run_thread_ptr && !*run_thread_ptr); });
    if (run_thread_ptr && !*run_thread_ptr)
        return;
    auto managed_job = jobQ.front();
    executions_status.MarkInProcess(thread_id);
    jobQ.pop();
    lock.unlock();
    // Upgrade the index weak reference to a strong ref while we run the job over the index.
    if (auto temp_ref = managed_job.index_weak_ref.lock()) {
        managed_job.job->Execute(managed_job.job);
    }
    executions_status.MarkDone(thread_id);
}

// Main loop for background worker threads that execute the jobs form the job queue.
// run_thread uses as a signal to the thread that indicates whether it should keep running or
// stop and terminate the thread.
void tieredIndexMock::thread_main_loop(int thread_id, tieredIndexMock &mock_thread_pool) {
    while (mock_thread_pool.run_thread) {
        mock_thread_pool.thread_iteration(thread_id, &(mock_thread_pool.run_thread));
    }
}

void tieredIndexMock::thread_pool_join() {
    // Check every 10 ms if queue is empty, and if so, terminate the threads loop.
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::unique_lock<std::mutex> lock(queue_guard);
        if (jobQ.empty()) {
            run_thread = false;
            queue_cond.notify_all();
            break;
        }
    }
    for (size_t i = 0; i < thread_pool_size; i++) {
        thread_pool[i].join();
    }
    thread_pool.clear();
}

void tieredIndexMock::thread_pool_wait(size_t waiting_duration) {
    while (true) {
        std::unique_lock<std::mutex> lock(queue_guard);
        if (jobQ.empty() && executions_status.AllDone()) {
            break;
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(waiting_duration));
    }
}
