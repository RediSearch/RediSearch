/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <time.h>
#include <stdint.h>

typedef struct {
    struct timespec start;
} rs_wall_clock;

#define RS_WALL_CLOCK_PER_SEC 1000000000ULL
#define RS_WALL_CLOCK_PER_MILLISEC (RS_WALL_CLOCK_PER_SEC / 1000)

// Using different types for nanoseconds and milliseconds to avoid confusion
typedef uint64_t rs_wall_clock_ns_t;
typedef uint64_t rs_wall_clock_ms_t;

// Initializes the clock with current time
static inline void rs_wall_clock_init(rs_wall_clock *clk) {
    clock_gettime(CLOCK_MONOTONIC, &clk->start);
}

static inline rs_wall_clock_ns_t rs_wall_clock_diff_ns(rs_wall_clock *start, rs_wall_clock *end) {
    uint64_t sec_diff = (uint64_t)(end->start.tv_sec - start->start.tv_sec);
    int64_t nsec_diff = end->start.tv_nsec - start->start.tv_nsec;
    if (nsec_diff < 0) {
        sec_diff -= 1;
        nsec_diff += RS_WALL_CLOCK_PER_SEC;
    }
    return sec_diff * RS_WALL_CLOCK_PER_SEC + nsec_diff;
}

// Returns time elapsed since start, in nanoseconds
static inline rs_wall_clock_ns_t rs_wall_clock_elapsed_ns(rs_wall_clock *clk) {
    rs_wall_clock now;
    clock_gettime(CLOCK_MONOTONIC, &now.start);
    return rs_wall_clock_diff_ns(clk, &now);
}


static inline rs_wall_clock_ns_t rs_wall_clock_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (rs_wall_clock_ns_t)ts.tv_sec * RS_WALL_CLOCK_PER_SEC + ts.tv_nsec;
}

static inline double rs_wall_clock_convert_ns_to_ms_f(rs_wall_clock_ns_t ns) {
    return (double)ns / RS_WALL_CLOCK_PER_MILLISEC;
}

static inline rs_wall_clock_ms_t rs_wall_clock_convert_ns_to_ms(rs_wall_clock_ns_t ns) {
    return ns / RS_WALL_CLOCK_PER_MILLISEC;
}
