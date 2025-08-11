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

#define NANOSEC_PER_SECOND 1000000000ULL
#define MILLISEC_PER_SECOND (NANOSEC_PER_SECOND / 1000)

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
    uint64_t sec_diff = (uint64_t)(end->tv_sec - start->tv_sec);
    int64_t nsec_diff = end->tv_nsec - start->tv_nsec;
    if (nsec_diff < 0) {
        sec_diff -= 1;
        nsec_diff += NANOSEC_PER_SECOND;
    }

    return sec_diff * NANOSEC_PER_SECOND + nsec_diff;
}

// Returns time elapsed since start, in nanoseconds
static inline rs_wall_clock_ns_t rs_wall_clock_elapsed_ns(rs_wall_clock *clk) {
    rs_wall_clock now;
    rs_wall_clock_init(&now);
    return rs_wall_clock_diff_ns(clk, &now);
}

static inline rs_wall_clock_ns_t rs_wall_clock_now_ns() {
    rs_wall_clock now;
    rs_wall_clock_init(&now);
    return (rs_wall_clock_ns_t)now.tv_sec * NANOSEC_PER_SECOND + now.tv_nsec;
}

static inline double rs_wall_clock_convert_ns_to_ms_f(rs_wall_clock_ns_t ns) {
    return (double)ns / MILLISEC_PER_SECOND;
}

static inline rs_wall_clock_ms_t rs_wall_clock_convert_ns_to_ms(rs_wall_clock_ns_t ns) {
    return ns / MILLISEC_PER_SECOND;
}

// Undefine macros to avoid conflicts
#undef NANOSEC_PER_SECOND
#undef MILLISEC_PER_SECOND

#ifdef __cplusplus
}
#endif
