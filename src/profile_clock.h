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
} profile_clock;

#define TIMESPEC_PER_SEC 1000000000L
#define TIMESPEC_PER_MILLISEC TIMESPEC_PER_SEC / 1000

typedef uint64_t profile_clock_ns_t;

// Initializes the clock with current time
static inline void profile_clock_init(profile_clock *clk) {
    clock_gettime(CLOCK_MONOTONIC, &clk->start);
}

// Returns time elapsed since start, in nanoseconds
static inline uint64_t profile_clock_elapsed_ns(profile_clock *clk) {
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    uint64_t sec_diff = (uint64_t)(end.tv_sec - clk->start.tv_sec);
    int64_t nsec_diff = end.tv_nsec - clk->start.tv_nsec;

    return sec_diff * 1000000000ULL + nsec_diff;
}

static inline uint64_t profile_clock_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}
