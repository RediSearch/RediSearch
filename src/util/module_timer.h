/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef MODULE_TIMER_H__
#define MODULE_TIMER_H__

#include <limits.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Redis runs every module timer behind one event-loop timer whose handler (`moduleTimerHandler`,
// an `aeTimeProc`) returns the ms until the earliest pending module timer as an `int`. A longer
// delay truncates there: a negative result makes that handler run on every loop pass, and -1
// (`AE_NOMORE`) deletes it, so no module timer fires until one is armed sooner than every other
// pending one. MOD-21060.
#define RS_MAX_TIMER_DELAY_MS ((long long)INT_MAX)

// The delay to arm a module timer for `deadlineMs`, from 0 for a deadline already past up to
// RS_MAX_TIMER_DELAY_MS. A timer whose deadline is further out fires early, and must re-arm
// itself with this until the deadline is reached.
static inline long long RS_TimerDelayToDeadlineMs(uint64_t deadlineMs, uint64_t nowMs) {
  if (deadlineMs <= nowMs) {
    return 0;
  }
  const uint64_t remaining = deadlineMs - nowMs;
  return remaining > (uint64_t)RS_MAX_TIMER_DELAY_MS ? RS_MAX_TIMER_DELAY_MS : (long long)remaining;
}

#ifdef __cplusplus
}
#endif
#endif  // MODULE_TIMER_H__
