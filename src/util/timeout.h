/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <time.h>
#include "redisearch.h"
#include "version.h"
#include "query_error.h"

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

#define NOT_TIMED_OUT 0
#define TIMED_OUT 1

typedef struct TimeoutCtx {
  size_t counter;
  struct timespec timeout;
} TimeoutCtx;

typedef int(*TimeoutCb)(TimeoutCtx *);

static inline int TimedOut(const struct timespec *timeout) {
  static struct timespec now;
  clock_gettime(CLOCK_MONOTONIC_RAW, &now);
  if (__builtin_expect(rs_timer_ge(&now, timeout), 0)) {
    return TIMED_OUT;
  }
  return NOT_TIMED_OUT;
}

// Check if time has been reached (run once every 100 calls)
static inline int TimedOut_WithCounter(const struct timespec *timeout, size_t *counter) {
  if (RS_IsMock) return 0;

  if (*counter != REDISEARCH_UNINITIALIZED && ++(*counter) == 100) {
    *counter = 0;
    return TimedOut(timeout);
  }
  return NOT_TIMED_OUT;
}

// Check if time has been reached (run once every `gran` calls)
static inline int TimedOut_WithCounter_Gran(const struct timespec *timeout, size_t *counter, uint32_t gran) {
  if (RS_IsMock) return 0;

  if (*counter != REDISEARCH_UNINITIALIZED && ++(*counter) == gran) {
    *counter = 0;
    return TimedOut(timeout);
  }
  return NOT_TIMED_OUT;
}

// Check if time has been reached (run once every 100 calls)
static inline int TimedOut_WithCtx(TimeoutCtx *ctx) {
  return TimedOut_WithCounter(&ctx->timeout, &ctx->counter);
}

// Check if time has been reached (run once every `gran` calls)
static inline int TimedOut_WithCtx_Gran(TimeoutCtx *ctx, uint32_t gran) {
  return TimedOut_WithCounter_Gran(&ctx->timeout, &ctx->counter, gran);
}

// Check if time has been reached
static inline int TimedOut_WithStatus(struct timespec *timeout, QueryError *status) {
  int rc = TimedOut(timeout);
  if (status && rc == TIMED_OUT) {
    QueryError_SetCode(status, QUERY_ETIMEDOUT);
  }
  return rc;
}

#ifdef __cplusplus
}
#endif
