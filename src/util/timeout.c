/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "timeout.h"
#include <errno.h>

// Platform-specific timed wait on condition variable
// Returns: true if timed out, false if signaled (or spurious wakeup)
// abstimeMono is an absolute time in CLOCK_MONOTONIC_RAW
// macOS: uses pthread_cond_timedwait_relative_np with relative timeout
// Linux/FreeBSD: converts to CLOCK_MONOTONIC for pthread_cond_timedwait
bool condTimedWait(pthread_cond_t *cond, pthread_mutex_t *lock,
                   const struct timespec *abstimeMono) {
  // Calculate remaining time from CLOCK_MONOTONIC_RAW
  struct timespec nowRaw, remaining;
  clock_gettime(CLOCK_MONOTONIC_RAW, &nowRaw);
  rs_timerremaining((struct timespec *)abstimeMono, &nowRaw, &remaining);
  // Check if already past deadline
  if (remaining.tv_sec == 0 && remaining.tv_nsec == 0) {
    return true;  // timed out
  }
#if defined(__APPLE__) && defined(__MACH__)
  return pthread_cond_timedwait_relative_np(cond, lock, &remaining) == ETIMEDOUT;
#else
  // Convert to CLOCK_MONOTONIC absolute time for the condition variable
  struct timespec nowMono, absMono;
  clock_gettime(CLOCK_MONOTONIC, &nowMono);
  rs_timeradd(&nowMono, &remaining, &absMono);
  return pthread_cond_timedwait(cond, lock, &absMono) == ETIMEDOUT;
#endif
}

