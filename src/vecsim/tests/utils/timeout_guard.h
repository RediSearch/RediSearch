/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>

namespace test_utils {

/**
 * @brief A RAII-style timeout guard that forcefully terminates execution if a timeout is reached.
 *
 * This class creates a background thread that monitors execution time. If the timeout expires
 * before the guard is destroyed (or explicitly notified), it will call the configured action
 * (default: exit the process with error code).
 */
class TimeoutGuard {
public:
    /**
     * @brief Construct a timeout guard with specified duration and optional custom action.
     *
     * @param timeout Duration to wait before triggering timeout action
     * @param on_timeout Custom action to execute on timeout (default: exit with code -1)
     */
    template <typename Rep, typename Period>
    explicit TimeoutGuard(
        std::chrono::duration<Rep, Period> timeout,
        std::function<void()> on_timeout =
            []() {
                std::cerr << "TimeoutGuard: Test/Benchmark timeout! Exiting..." << std::endl;
                abort();
            })
        : timeout_action(std::move(on_timeout)), notified(false) {

        guard_thread = std::thread([this, timeout]() {
            std::unique_lock<std::mutex> lock(mutex);
            // Wait to see if we are notified before the timeout expires
            // Returns false if timeout expired, true if notified
            if (!cv.wait_for(lock, timeout, [this]() { return notified; })) {
                // Timeout expired and not notified
                timeout_action();
            }
        });
    }

    /**
     * @brief Destructor - notifies the guard thread and waits for it to finish.
     */
    ~TimeoutGuard() {
        notify();
        if (guard_thread.joinable()) {
            guard_thread.join();
        }
    }

    // Delete copy and move operations to prevent misuse
    TimeoutGuard(const TimeoutGuard &) = delete;
    TimeoutGuard &operator=(const TimeoutGuard &) = delete;
    TimeoutGuard(TimeoutGuard &&) = delete;
    TimeoutGuard &operator=(TimeoutGuard &&) = delete;

    /**
     * @brief Explicitly notify the guard that execution completed successfully.
     *
     * This cancels the timeout and allows the guard thread to exit cleanly.
     * Safe to call multiple times.
     */
    void notify() {
        std::lock_guard<std::mutex> lock(mutex);
        if (!notified) {
            notified = true;
            cv.notify_one();
        }
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    std::thread guard_thread;
    std::function<void()> timeout_action;
    bool notified;
};

/**
 * @brief Specialized timeout guard for benchmarks with a simpler API.
 *
 * This is a convenience wrapper around TimeoutGuard specifically for Google Benchmark.
 * It provides:
 * - Simpler constructor (only requires timeout duration, no custom action needed)
 * - Benchmark-specific error message on timeout
 * - Clear intent that this is for benchmarks
 *
 * Functionally identical to TimeoutGuard with default parameters, but with better
 * clarity and a more convenient API for benchmark use cases.
 *
 */
class BenchmarkTimeoutGuard : public TimeoutGuard {
public:
    template <typename Rep, typename Period>
    explicit BenchmarkTimeoutGuard(std::chrono::duration<Rep, Period> timeout)
        : TimeoutGuard(timeout, []() {
              std::cerr << "BenchmarkTimeoutGuard: Benchmark timeout! Exiting..." << std::endl;
              abort();
          }) {}
};

} // namespace test_utils
