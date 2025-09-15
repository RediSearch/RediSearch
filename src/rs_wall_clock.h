/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <time.h>
#include <stdint.h>
#include "rmutil/rm_assert.h"

typedef struct timespec rs_wall_clock;

#define NANOSEC_PER_SECOND     1000000000L // 10^9
#define NANOSEC_PER_MILLISEC   (NANOSEC_PER_SECOND / 1000) // 10^6

// Using different types for nanoseconds and milliseconds to avoid confusion
typedef uint64_t rs_wall_clock_ns_t;
typedef uint64_t rs_wall_clock_ms_t;

// Initializes the clock with current time
static inline void rs_wall_clock_init(rs_wall_clock *clk) {
    clock_gettime(CLOCK_MONOTONIC, clk);
}

// Returns the time difference between two rs_wall_clock in nanoseconds.
// Assumes 'end' is sampled after 'start'.
static inline rs_wall_clock_ns_t rs_wall_clock_diff_ns(rs_wall_clock *start,
                                                       rs_wall_clock *end) {
    RS_ASSERT(end->tv_sec >= start->tv_sec); // Assert the assumption

    // We assume that the difference in seconds will not overflow int64_t.
    RS_ASSERT(end->tv_sec - start->tv_sec <= INT64_MAX);
    // Since 2^63 seconds is 292*10^9 years, it's safe to assume it won't happen.
    int64_t sec_diff = (int64_t)(end->tv_sec - start->tv_sec);

    // timespec nanoseconds can't hold more then 1 second (10^9), so the difference
    // can't overflow int64_t.
    int64_t nsec_diff = end->tv_nsec - start->tv_nsec;

    // Implicit cast to rs_wall_clock_ns_t
    // The cast should happen after the addition
    // int64_t * long -> int64_t, int64_t + int64_t -> int64_t
    return sec_diff * NANOSEC_PER_SECOND + nsec_diff;
}

// Returns time elapsed since start, in nanoseconds
static inline rs_wall_clock_ns_t rs_wall_clock_elapsed_ns(rs_wall_clock *clk) {
    rs_wall_clock now;
    rs_wall_clock_init(&now);
    return rs_wall_clock_diff_ns(clk, &now);
}

// Returns current time of the monotonic clock in nanoseconds
static inline rs_wall_clock_ns_t rs_wall_clock_now_ns() {
    rs_wall_clock now;
    rs_wall_clock_init(&now);
    return (rs_wall_clock_ns_t)now.tv_sec * NANOSEC_PER_SECOND + now.tv_nsec;
}

// Converts a duration from nanoseconds to milliseconds (floating-point result).
// Returns: elapsed time in milliseconds as a double, preserving fractional ms.
static inline double rs_wall_clock_convert_ns_to_ms_d(rs_wall_clock_ns_t ns) {
    return (double)ns / NANOSEC_PER_MILLISEC;
}

// Converts a duration from nanoseconds to milliseconds (integer result).
// Returns: elapsed time in whole milliseconds as rs_wall_clock_ms_t (uint64_t).
static inline rs_wall_clock_ms_t rs_wall_clock_convert_ns_to_ms(rs_wall_clock_ns_t ns) {
    return ns / NANOSEC_PER_MILLISEC;
}

// Undefine macros to avoid conflicts
#undef NANOSEC_PER_SECOND
#undef NANOSEC_PER_MILLISEC

#ifdef __cplusplus
}
#endif
