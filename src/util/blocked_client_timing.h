/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef BLOCKED_CLIENT_TIMING_H__
#define BLOCKED_CLIENT_TIMING_H__

#include <pthread.h>
#include <stdbool.h>
#include "redismodule.h"

typedef enum {
  BLOCKED_CLIENT_TIMING_PENDING,
  BLOCKED_CLIENT_TIMING_RUNNING,
  BLOCKED_CLIENT_TIMING_FINISHED,
} BlockedClientTimingState;

/* One background interval per blocked-client cycle. The owner must outlive
 * the worker and timeout callback; neither may access it after UnblockClient.
 * Init, Begin, and Destroy require exclusive access between cycles. */
typedef struct {
  pthread_mutex_t lock;
  RedisModuleBlockedClient* bc;
  BlockedClientTimingState state;
} BlockedClientTiming;

static inline void BlockedClientTiming_Init(BlockedClientTiming* timing) {
  pthread_mutex_init(&timing->lock, NULL);
  timing->bc = NULL;
  timing->state = BLOCKED_CLIENT_TIMING_FINISHED;
}

static inline void BlockedClientTiming_Begin(BlockedClientTiming* timing,
                                             RedisModuleBlockedClient* bc) {
  timing->bc = bc;
  timing->state = BLOCKED_CLIENT_TIMING_PENDING;
}

static inline void BlockedClientTiming_Start(BlockedClientTiming* timing) {
  pthread_mutex_lock(&timing->lock);
  if (timing->state == BLOCKED_CLIENT_TIMING_PENDING) {
    RedisModule_BlockedClientMeasureTimeStart(timing->bc);
    timing->state = BLOCKED_CLIENT_TIMING_RUNNING;
  }
  pthread_mutex_unlock(&timing->lock);
}

/* Redis snapshots duration after the timeout callback and End does not clear
 * its timestamp. Finalizing also forbids a queued worker from starting later. */
static inline void BlockedClientTiming_Finish(BlockedClientTiming* timing) {
  pthread_mutex_lock(&timing->lock);
  if (timing->state == BLOCKED_CLIENT_TIMING_RUNNING) {
    RedisModule_BlockedClientMeasureTimeEnd(timing->bc);
  }
  timing->state = BLOCKED_CLIENT_TIMING_FINISHED;
  pthread_mutex_unlock(&timing->lock);
}

static inline void BlockedClientTiming_Destroy(BlockedClientTiming* timing) {
  pthread_mutex_destroy(&timing->lock);
}

#endif
