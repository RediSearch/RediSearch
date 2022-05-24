#pragma once

#include <time.h>
#include "version.h"
#include "query_error.h"

#ifdef __cplusplus
extern "C" {
#endif

/*****************************************
 *            Timeout API
 ****************************************/

static inline int rs_timer_ge(struct timespec *a, struct timespec *b) {
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

// Check if time has been reached (run once every 100 calls) 
static inline int TimedOut(struct timespec *timeout, size_t *counter) {
  if (RS_IsMock) return 0;

  if (++(*counter) == 100) {
    *counter = 0;
    static struct timespec now;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);
    if (__builtin_expect(rs_timer_ge(&now, timeout), 0)) {
      return TIMED_OUT;
    }
  }
  return NOT_TIMED_OUT;
}

// Check if time has been reached
static inline int TimedOut_WithStatus(struct timespec *timeout, QueryError *status) {
  size_t counter = 99;
  int rc = TimedOut(timeout, &counter);
  if (status && rc == TIMED_OUT) {
    QueryError_SetCode(status, QUERY_TIMEDOUT);
  }
  return rc; 
}

// Check if time has been reached (run once every 100 calls) 
static inline int TimedOutWithCtx(TimeoutCtx *ctx) {
  return TimedOut(&ctx->timeout, &ctx->counter);
}

static inline void updateTimeout(struct timespec *timeout, int durationNS) {
  if (RS_IsMock) return;

  // 0 disables the timeout
  if (durationNS == 0) {
    durationNS = INT32_MAX;
  }

  struct timespec now = { .tv_sec = 0, .tv_nsec = 0 };
  struct timespec duration = { .tv_sec = durationNS / 1000,
                               .tv_nsec = ((durationNS % 1000) * 1000000) };
  clock_gettime(CLOCK_MONOTONIC_RAW, &now);
  rs_timeradd(&now, &duration, timeout);
}

#ifdef __cplusplus
}
#endif
