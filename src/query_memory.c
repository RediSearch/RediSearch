/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "query_memory.h"

#include <math.h>
#include <stdatomic.h>

#include "config.h"
#include "util/redis_mem_info.h"

static _Atomic(uint32_t) ratioReadInterval = QUERY_MEMORY_RATIO_READ_INTERVAL;
static _Atomic(float) forcedRatio = -1;
static _Atomic(uint64_t) pressureAtCall = 0;
static _Atomic(uint64_t) checkpointCalls = 0;

void QueryMemory_SetRatioReadInterval(uint32_t interval) {
  atomic_store_explicit(&ratioReadInterval, interval ? interval : QUERY_MEMORY_RATIO_READ_INTERVAL,
                        memory_order_relaxed);
}

uint32_t QueryMemory_GetRatioReadInterval(void) {
  return atomic_load_explicit(&ratioReadInterval, memory_order_relaxed);
}

void QueryMemory_SetForcedRatio(float ratio) {
  // signbit, so that -0 restores the real read too.
  atomic_store_explicit(&forcedRatio, signbit(ratio) ? -1 : ratio, memory_order_relaxed);
}

float QueryMemory_GetForcedRatio(void) {
  return atomic_load_explicit(&forcedRatio, memory_order_relaxed);
}

void QueryMemory_SetPressureAtCall(uint64_t n) {
  atomic_store_explicit(&pressureAtCall, 0, memory_order_relaxed);
  atomic_store_explicit(&checkpointCalls, 0, memory_order_relaxed);
  atomic_store_explicit(&pressureAtCall, n, memory_order_relaxed);
}

uint64_t QueryMemory_GetPressureAtCall(void) {
  return atomic_load_explicit(&pressureAtCall, memory_order_relaxed);
}

uint64_t QueryMemory_GetCheckpointCalls(void) {
  return atomic_load_explicit(&checkpointCalls, memory_order_relaxed);
}

void QueryMemory_ResetFakePressure(void) {
  QueryMemory_SetForcedRatio(-1);
  QueryMemory_SetPressureAtCall(0);
  QueryMemory_SetRatioReadInterval(0);
}

// Counts the call only while positional pressure is armed, so production pays one relaxed load.
static bool isPressureAtThisCall(void) {
  const uint64_t at = atomic_load_explicit(&pressureAtCall, memory_order_relaxed);
  if (!at) {
    return false;
  }
  return atomic_fetch_add_explicit(&checkpointCalls, 1, memory_order_relaxed) + 1 == at;
}

static float readRatio(void) {
  const float forced = QueryMemory_GetForcedRatio();
  return forced >= 0 ? forced : RedisMemory_GetRawUsedMemoryRatio();
}

static bool oomPolicyIsIgnore(void) {
  // Global like QueryMemoryGuard's, not the request's snapshot. A plain enum read racing
  // CONFIG SET sees the old or the new policy, both valid.
  return RSGlobalConfig.requestConfigParams.oomPolicy == OomPolicy_Ignore;
}

bool QueryMemory_ShouldStop(QueryRequestTimeout *timeout, uint32_t *counter) {
  if (timeout && QueryRequest_ShouldStop(timeout)) {
    return true;
  }
  const bool forcedPressure = isPressureAtThisCall();
  if (!forcedPressure && ++*counter < QueryMemory_GetRatioReadInterval()) {
    return false;
  }
  *counter = 0;
  if (oomPolicyIsIgnore() || !(forcedPressure || readRatio() >= REDIS_MEMORY_OOM_RATIO)) {
    return false;
  }
  if (timeout) {
    // A lost race keeps the earlier reason; the query stops either way.
    QueryRequestTimeout_RequestStop(timeout, QUERY_STOP_REASON_OOM);
  }
  return true;
}
