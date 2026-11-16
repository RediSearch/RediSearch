/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <time.h>

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

// Lets the jobs of a thread pool keep taking the GIL while the main thread joins the pool at
// shutdown.
//
// The main thread holds the GIL then and cannot drop it: an Unlock needs a Lock on the same thread
// to set up the client Redis expects, and shutdown has none. A job blocking on the GIL would never
// return, so jobs take it through a gate. Once the join begins the gate stops waiting and the job
// borrows the main thread's GIL. That keeps mutual exclusion because the main thread sits in the
// join and touches nothing, the borrowers of a gate take turns, and pools are joined one at a time.
// Use one gate per pool, initialized with GIL_GATE_INIT.
//
// Accepted limitation: a borrowing job calls Redis without the bookkeeping Lock sets up (current
// client, execution nesting). This path only runs with RS_GLOBAL_DTORS, i.e. tests and sanitizer
// runs, never in production.
typedef struct GilGate {
  bool joining;               // accessed only through the __atomic builtins
  pthread_mutex_t borrowers;  // held while borrowing, since a pool may have several threads
} GilGate;

#define GIL_GATE_INIT {.joining = false, .borrowers = PTHREAD_MUTEX_INITIALIZER}

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

// Takes the GIL, or borrows the main thread's once the join began. Returns whether it was really
// taken. Polls rather than blocks, since a blocked job could not notice the join.
static inline bool GilGate_Lock(GilGate *gate, RedisModuleCtx *ctx) {
  const struct timespec backoff = {.tv_sec = 0, .tv_nsec = 100000};
  while (RedisModule_ThreadSafeContextTryLock(ctx) != REDISMODULE_OK) {
    if (GilGate_IsJoining(gate)) {
      pthread_mutex_lock(&gate->borrowers);
      return false;
    }
    nanosleep(&backoff, NULL);
  }
  return true;
}

// Pairs with GilGate_Lock. The join cannot begin while a job holds the GIL, so a job that took it
// for real always releases it for real, and a borrowing one only ends its turn.
static inline void GilGate_Unlock(GilGate *gate, RedisModuleCtx *ctx) {
  if (GilGate_IsJoining(gate)) {
    pthread_mutex_unlock(&gate->borrowers);
  } else {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}

#ifdef __cplusplus
}
#endif
