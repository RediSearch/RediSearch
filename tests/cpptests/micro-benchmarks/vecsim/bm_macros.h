/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "cpu_features_macros.h"

#define EXPAND(x)  x
#define EXPAND2(x) EXPAND(x)
// Helper for raw concatenation with varying arguments
#define BM_FUNC_NAME_HELPER1_2(a, b)          a##_##b
#define BM_FUNC_NAME_HELPER1_3(a, b, c)       a##_##b##_##c
#define BM_FUNC_NAME_HELPER1_4(a, b, c, d)    a##_##b##_##c##_##d
#define BM_FUNC_NAME_HELPER1_5(a, b, c, d, e) a##_##b##_##c##_##d##_##e

// Force expansion of macro arguments
#define BM_FUNC_NAME_HELPER_2(a, b)          BM_FUNC_NAME_HELPER1_2(a, b)
#define BM_FUNC_NAME_HELPER_3(a, b, c)       BM_FUNC_NAME_HELPER1_3(a, b, c)
#define BM_FUNC_NAME_HELPER_4(a, b, c, d)    BM_FUNC_NAME_HELPER1_4(a, b, c, d)
#define BM_FUNC_NAME_HELPER_5(a, b, c, d, e) BM_FUNC_NAME_HELPER1_5(a, b, c, d, e)

// Determine the number of arguments and select the appropriate helper
#define COUNT_ARGS(...)                             COUNT_ARGS_(__VA_ARGS__, 6, 5, 4, 3, 2, 1)
#define COUNT_ARGS_(_1, _2, _3, _4, _5, _6, N, ...) N

// Concatenate BM_FUNC_NAME_HELPER with the number of arguments
#define CONCAT_HELPER(a, b) a##_##b
#define CONCAT(a, b)        CONCAT_HELPER(a, b)

// Main macro that selects the appropriate helper based on argument count
#define CONCAT_WITH_UNDERSCORE(...)                                                                \
    EXPAND2(CONCAT(BM_FUNC_NAME_HELPER, EXPAND2(COUNT_ARGS(__VA_ARGS__)))(__VA_ARGS__))

// Modify this macro to account for the extra BENCHMARK_ARCH parameter
#define CONCAT_WITH_UNDERSCORE_ARCH(...) CONCAT_WITH_UNDERSCORE(__VA_ARGS__, BENCHMARK_ARCH)
