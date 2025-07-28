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

typedef uint64_t profile_clock_ns_t;
typedef uint64_t profile_clock_ms_t;

static inline profile_clock_ms_t profile_clock_convert_ns_to_ms(profile_clock_ns_t ns) {
    return ns / 1000000;
}

static inline profile_clock_ms_t profile_clock_convert_ms_to_ns(profile_clock_ns_t ms) {
    return ns * 1000000;
}

// Initializes the clock with current time
static inline void profile_clock_start(profile_clock *clk) {
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

// Returns time elapsed since start, in milliseconds
static inline uint64_t profile_clock_elapsed_ms(profile_clock *clk) {
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    uint64_t sec_diff = (uint64_t)(end.tv_sec - clk->start.tv_sec);
    int64_t nsec_diff = end.tv_nsec - clk->start.tv_nsec;

    return sec_diff * 1000ULL + nsec_diff / 1000000;
}

static inline uint64_t profile_clock_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

// Subtract time in lhs from time in rhs and store the result in out
static inline profile_clock profile_clock_sub_time(profile_clock lhs, profile_clock_ns_t rhs, profile_clock *out) {
    out->start.tv_sec = lhs.start.tv_sec;
    out->start.tv_nsec = lhs.start.tv_nsec - rhs;
    return *out;
}
