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
#define RS_WALL_CLOCK_PER_MILLISEC RS_WALL_CLOCK_PER_SEC / 1000

typedef uint64_t rs_wall_clock_ns_t;

// Initializes the clock with current time
static inline void rs_wall_clock_init(rs_wall_clock *clk) {
    clock_gettime(CLOCK_MONOTONIC, &clk->start);
}

// Returns time elapsed since start, in nanoseconds
static inline rs_wall_clock_ns_t rs_wall_clock_elapsed_ns(rs_wall_clock *clk) {
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    time_t sec_diff = end.tv_sec - clk->start.tv_sec;
    long nsec_diff = end.tv_nsec - clk->start.tv_nsec;

    if (nsec_diff < 0) {
        sec_diff -= 1;
        nsec_diff += RS_WALL_CLOCK_PER_SEC;
    }

    return (rs_wall_clock_ns_t)sec_diff * RS_WALL_CLOCK_PER_SEC + nsec_diff;
}


static inline rs_wall_clock_ns_t rs_wall_clock_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (rs_wall_clock_ns_t)ts.tv_sec * RS_WALL_CLOCK_PER_SEC + ts.tv_nsec;
}

static inline rs_wall_clock_ns_t rs_wall_clock_diff_ns(rs_wall_clock *start, rs_wall_clock *end) {
    return rs_wall_clock_elapsed_ns(end) - rs_wall_clock_elapsed_ns(start);
}

// Convert rs_wall_clock to clock_t based on the elapsed time in nanoseconds
static inline clock_t rs_wall_clock_to_clock_t(rs_wall_clock *clk) {
    rs_wall_clock_ns_t ns_total = rs_wall_clock_elapsed_ns(clk);

    // Convert nanoseconds to seconds, then to clock_t units
    // CLOCKS_PER_SEC is usually 1,000,000 (µs)
    return (clock_t)(ns_total * CLOCKS_PER_SEC / RS_WALL_CLOCK_PER_SEC);
}

static inline double rs_wall_clock_convert_ns_to_ms(rs_wall_clock_ns_t ns) {
    return (double)ns / RS_WALL_CLOCK_PER_MILLISEC;
}
