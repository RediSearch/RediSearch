/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "gtest/gtest.h"
#include "timeout_guard.h"
#include <chrono>

namespace test_utils {

/**
 * @brief Google Test Event Listener that adds timeout protection to every test.
 *
 * This listener automatically creates a TimeoutGuard for each test and destroys it
 * when the test completes. This provides global timeout protection without modifying
 * individual tests.
 */
class TimeoutTestListener : public testing::EmptyTestEventListener {
public:
    template <typename Rep, typename Period>
    explicit TimeoutTestListener(std::chrono::duration<Rep, Period> default_timeout)
        : default_timeout_seconds(
              std::chrono::duration_cast<std::chrono::seconds>(default_timeout).count()) {}

    /**
     * @brief Called before each test starts.
     */
    void OnTestStart(const testing::TestInfo &test_info) override {
        std::string test_name = test_info.name();
        std::string suite_name = test_info.test_suite_name();

        // Skip timeout guard for death tests (tests that use EXPECT_EXIT/EXPECT_DEATH)
        // These tests use fork() which doesn't play well with background threads.
        // The TimeoutGuard should be created manually inside the TestBody lambda instead.
        if (test_name.find("MockThreadPool") != std::string::npos) {
            return;
        }
        // Get timeout for this specific test (can be customized per test)
        auto timeout = GetTimeoutForTest(test_info);

        // Create timeout guard for this test
        current_guard = std::make_unique<TimeoutGuard>(timeout, [&test_info]() {
            std::cerr << "TIMEOUT: Test " << test_info.test_suite_name() << "." << test_info.name()
                      << " exceeded timeout!" << std::endl;
            std::exit(-1);
        });
    }

    /**
     * @brief Called after each test ends.
     */
    void OnTestEnd(const testing::TestInfo & /*test_info*/) override {
        // Notify and destroy the guard
        if (current_guard) {
            current_guard->notify();
            current_guard.reset();
        }
    }

private:
    /**
     * @brief Get timeout duration for a specific test.
     *
     * This can be customized to return different timeouts based on test name,
     * test suite, or other criteria.
     */
    std::chrono::seconds GetTimeoutForTest(const testing::TestInfo &test_info) {
        // Default timeout
        auto timeout = std::chrono::seconds(default_timeout_seconds);

        // Customize timeout based on test characteristics
        std::string test_name = test_info.name();
        std::string suite_name = test_info.test_suite_name();

        return timeout;
    }

    int default_timeout_seconds;
    std::unique_ptr<TimeoutGuard> current_guard;
};

/**
 * @brief Helper function to register the timeout listener globally.
 *
 * Call this once in your test main() function.
 *
 * @param default_timeout Default timeout for all tests
 */
template <typename Rep, typename Period>
inline void RegisterGlobalTimeoutListener(std::chrono::duration<Rep, Period> default_timeout) {
    testing::TestEventListeners &listeners = testing::UnitTest::GetInstance()->listeners();
    listeners.Append(new TimeoutTestListener(default_timeout));
}

} // namespace test_utils
