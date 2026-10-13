/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef PROFILE_TIMEOUT_H__
#define PROFILE_TIMEOUT_H__

#include <stdbool.h>
#include "redismodule.h"

/* Owned by blocked-client private data. Start and stop run on the main thread;
 * stopping before free-data cleanup releases the request keeps timer data alive. */
typedef struct ProfileTimeout {
  RedisModuleTimerID id;
  bool armed;
  void (*signal)(void *);
  void *data;
#ifdef ENABLE_ASSERT
  unsigned long long clientId;
  struct ProfileTimeout *next;
#endif
} ProfileTimeout;

#ifdef ENABLE_ASSERT
void ProfileTimeoutDebug_Register(RedisModuleCtx *ctx, ProfileTimeout *timeout);
void ProfileTimeoutDebug_Unregister(ProfileTimeout *timeout);
void ProfileTimeoutDebug_Increment(void);
#endif

static inline void ProfileTimeout_Fire(RedisModuleCtx *ctx, void *data) {
  (void)ctx;
  ProfileTimeout *timeout = data;
  timeout->armed = false;
#ifdef ENABLE_ASSERT
  ProfileTimeoutDebug_Unregister(timeout);
#endif
  timeout->signal(timeout->data);
#ifdef ENABLE_ASSERT
  ProfileTimeoutDebug_Increment();
#endif
}

static inline void ProfileTimeout_Start(RedisModuleCtx *ctx, ProfileTimeout *timeout,
                                        mstime_t duration, void (*signal)(void *), void *data) {
  if (duration == 0) return;
  timeout->signal = signal;
  timeout->data = data;
  timeout->id = RedisModule_CreateTimer(ctx, duration, ProfileTimeout_Fire, timeout);
  timeout->armed = true;
#ifdef ENABLE_ASSERT
  ProfileTimeoutDebug_Register(ctx, timeout);
#endif
}

static inline void ProfileTimeout_Stop(RedisModuleCtx *ctx, ProfileTimeout *timeout) {
  if (timeout->armed) {
    RedisModule_StopTimer(ctx, timeout->id, NULL);
#ifdef ENABLE_ASSERT
    ProfileTimeoutDebug_Unregister(timeout);
#endif
    timeout->armed = false;
  }
}

#endif
