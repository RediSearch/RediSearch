/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "timeout_test_environment.h"
#include <chrono>

/**
 * @brief Custom main function that registers global timeout protection for all tests.
 *
 * This replaces gtest_main and adds automatic timeout protection to every test.
 * Tests will automatically timeout after X seconds (or customized duration based on test type).
 */
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);

    test_utils::RegisterGlobalTimeoutListener(std::chrono::seconds(300));

    return RUN_ALL_TESTS();
}
