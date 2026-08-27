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
#include "query_request.h"
#include "redisearch.h"
#include "version.h"

#ifdef __cplusplus
extern "C" {
#endif

// suppress warning
// "'struct timespec' declared inside parameter list will not be visible outside of this
// definition or declaration"
struct timespec;

/*****************************************
 *            Timeout API
 ****************************************/

static inline int rs_timer_ge(const struct timespec *a, const struct timespec *b) {
  if (a->tv_sec == b->tv_sec) {
    return a->tv_nsec >= b->tv_nsec;
  }
  return a->tv_sec >= b->tv_sec;
}

static inline void rs_timeradd(struct timespec *a, struct timespec *b, struct timespec *result) {
  result->tv_sec = a->tv_sec + b->tv_sec;
  result->tv_nsec = a->tv_nsec + b->tv_nsec;
  if (result->tv_nsec >= 1000000000) {
    result->tv_sec  += 1;
    result->tv_nsec -= 1000000000;
  }
}

static inline void rs_timersub(struct timespec *a, struct timespec *b, struct timespec *result) {
  result->tv_sec = a->tv_sec - b->tv_sec;
  result->tv_nsec = a->tv_nsec - b->tv_nsec;
  if (result->tv_nsec < 0) {
    result->tv_sec  -= 1;
    result->tv_nsec += 1000000000;
  }
}

static inline void rs_timerremaining(struct timespec *a, struct timespec *b, struct timespec *result) {
  rs_timersub(a, b, result);
  // If we ended up with a negative result, set to 0
  if (result->tv_sec < 0) {
    result->tv_sec = 0;
    result->tv_nsec = 0;
  }
}

#define NOT_TIMED_OUT 0
#define TIMED_OUT 1

#define TIMEOUT_COUNTER_LIMIT QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT

static inline int TimedOut(const struct timespec *timeout) {
  static struct timespec now;
  clock_gettime(CLOCK_MONOTONIC_RAW, &now);
  if (__builtin_expect(rs_timer_ge(&now, timeout), 0)) {
    return TIMED_OUT;
  }
  return NOT_TIMED_OUT;
}

// VecSim timeout callback adapter. The request timeout owns the active source and decides how it
// is checked; its shared counter amortizes clock reads without delaying blocked-client checks.
static inline int VecSim_TimedOut(QueryRequestTimeout *timeout) {
  RS_ASSERT(timeout);
  if (RS_IsMock) return NOT_TIMED_OUT;
  return QueryRequestTimeout_IsTimedOut(timeout);
}

#ifdef __cplusplus
}
#endif
