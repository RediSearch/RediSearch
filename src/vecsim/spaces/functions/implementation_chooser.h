/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

/*
 * This file contains macros magic to choose the implementation of a function based on the
 * dimension's remainder. It is used to collapse large and repetitive switch statements that are
 * used to choose and define the templated values of the implementation of the distance functions.
 * We assume that we are dealing with 512-bit blocks, so we define a chunk size of 32 for 16-bit
 * elements, 16 for 32-bit elements, and a chunk size of 8 for 64-bit elements. The main macro is
 * CHOOSE_IMPLEMENTATION, and it's the one that should be used.
 */

// Macro for a single case. Sets __ret_dist_func to the function with the given remainder.
#define C1(func, N)                                                                                \
    case (N):                                                                                      \
        __ret_dist_func = func<(N)>;                                                               \
        break;

// Macros for folding cases of a switch statement, for easier readability.
// Each macro expands into a sequence of cases, from 0 to N-1, doubling the previous macro.
#define C2(func, N)  C1(func, 2 * (N)) C1(func, 2 * (N) + 1)
#define C4(func, N)  C2(func, 2 * (N)) C2(func, 2 * (N) + 1)
#define C8(func, N)  C4(func, 2 * (N)) C4(func, 2 * (N) + 1)
#define C16(func, N) C8(func, 2 * (N)) C8(func, 2 * (N) + 1)
#define C32(func, N) C16(func, 2 * (N)) C16(func, 2 * (N) + 1)
#define C64(func, N) C32(func, 2 * (N)) C32(func, 2 * (N) + 1)

// Macros for 4, 8, 16, 32 and 64 cases. Used to collapse the switch statement.
// Expands into 0-3, 0-7, 0-15, 0-31 or 0-63 cases respectively.
#define CASES4(func)  C4(func, 0)
#define CASES8(func)  C8(func, 0)
#define CASES16(func) C16(func, 0)
#define CASES32(func) C32(func, 0)
#define CASES64(func) C64(func, 0)

// Main macro. Expands into a switch statement that chooses the implementation based on the
// dimension's remainder.
// @params:
// out:     The output variable that will be set to the chosen implementation.
// dim:     The dimension.
// func:    The templated function that we want to choose the implementation for.
// chunk:   The chunk size. Can be 64, 32, 16, 8 or 4. Should be the number of elements of the
//          expected type fitting in the expected register size.
#define CHOOSE_IMPLEMENTATION(out, dim, chunk, func)                                               \
    do {                                                                                           \
        decltype(out) __ret_dist_func;                                                             \
        switch ((dim) % (chunk)) { CASES##chunk(func) default : __builtin_unreachable(); }         \
        out = __ret_dist_func;                                                                     \
    } while (0)

#define SVE_CASE(base_func, N)                                                                     \
    case (N):                                                                                      \
        if (partial_chunk)                                                                         \
            __ret_dist_func = base_func<true, (N)>;                                                \
        else                                                                                       \
            __ret_dist_func = base_func<false, (N)>;                                               \
        break

#define CHOOSE_SVE_IMPLEMENTATION(out, base_func, dim, chunk_getter)                               \
    do {                                                                                           \
        decltype(out) __ret_dist_func;                                                             \
        size_t chunk = chunk_getter();                                                             \
        bool partial_chunk = dim % chunk;                                                          \
        /* Assuming `base_func` has its main loop for 4 steps */                                   \
        unsigned char additional_steps = (dim / chunk) % 4;                                        \
        switch (additional_steps) {                                                                \
            SVE_CASE(base_func, 0);                                                                \
            SVE_CASE(base_func, 1);                                                                \
            SVE_CASE(base_func, 2);                                                                \
            SVE_CASE(base_func, 3);                                                                \
        default:                                                                                   \
            __builtin_unreachable();                                                               \
        }                                                                                          \
        out = __ret_dist_func;                                                                     \
    } while (0)
