/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include <stdbool.h>
#include <time.h>

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

// Lets the jobs of a thread pool stop cleanly when the main thread joins the pool at shutdown.
//
// The main thread holds the GIL then, and cannot drop it: an Unlock needs a Lock on the same
// thread to set up the client Redis expects, and shutdown has none. A job blocking on the GIL
// would never return, so a job takes the GIL through the gate instead. Once the join begins the
// gate stops waiting and GilGate_Lock returns false: the job does not hold the GIL, and must make
// no Redis keyspace or server-state call from then on. It only frees memory the module owns and
// exits. Nothing is lost: Redis fires the shutdown event after the AOF flush and RDB save, so work
// done during the join would never be persisted or replicated. Use one gate per pool.
typedef struct GilGate {
  bool joining;  // accessed only through the __atomic builtins
} GilGate;

// Call on the main thread, holding the GIL, right before joining the pool.
static inline void GilGate_BeginJoin(GilGate *gate) {
  __atomic_store_n(&gate->joining, true, __ATOMIC_RELEASE);
}

// Call when (re)creating the pool.
static inline void GilGate_Reset(GilGate *gate) {
  __atomic_store_n(&gate->joining, false, __ATOMIC_RELEASE);
}

static inline bool GilGate_IsJoining(GilGate *gate) {
  return __atomic_load_n(&gate->joining, __ATOMIC_ACQUIRE);
}

// Takes the GIL. Returns false, without it, once the join began (and never takes it after): the
// caller must stop its Redis work. Polls rather than blocks, since a blocked job could not notice
// the join.
static inline bool GilGate_Lock(GilGate *gate, RedisModuleCtx *ctx) {
  const struct timespec backoff = {.tv_sec = 0, .tv_nsec = 100000};
  while (!GilGate_IsJoining(gate)) {
    if (RedisModule_ThreadSafeContextTryLock(ctx) == REDISMODULE_OK) {
      return true;
    }
    nanosleep(&backoff, NULL);
  }
  return false;
}

// Pairs with GilGate_Lock. The join cannot begin while a job holds the GIL, so a job that took it
// for real always releases it for real, and one that did not never does.
static inline void GilGate_Unlock(GilGate *gate, RedisModuleCtx *ctx) {
  if (!GilGate_IsJoining(gate)) {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}

#ifdef __cplusplus
}
#endif
