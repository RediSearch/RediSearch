/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "query_memory.h"

#include <stdatomic.h>

#include "config.h"
#include "util/redis_mem_info.h"

static _Atomic(uint32_t) ratioReadInterval = QUERY_MEMORY_RATIO_READ_INTERVAL;

void QueryMemory_SetRatioReadInterval(uint32_t interval) {
  atomic_store_explicit(&ratioReadInterval, interval ? interval : QUERY_MEMORY_RATIO_READ_INTERVAL,
                        memory_order_relaxed);
}

uint32_t QueryMemory_GetRatioReadInterval(void) {
  return atomic_load_explicit(&ratioReadInterval, memory_order_relaxed);
}

static bool underMemoryPressure(void) {
  // Global like QueryMemoryGuard's, not the request's snapshot. Atomic because this runs on any
  // thread while CONFIG SET writes the policy on the main thread.
  if (__atomic_load_n(&RSGlobalConfig.requestConfigParams.oomPolicy, __ATOMIC_RELAXED) ==
      OomPolicy_Ignore) {
    return false;
  }
  return RedisMemory_GetRawUsedMemoryRatio() >= REDIS_MEMORY_OOM_RATIO;
}

static bool readMemoryAndStop(QueryRequestLimits *limits) {
  if (!underMemoryPressure()) {
    return false;
  }
  if (limits) {
    QueryRequestLimits_RequestMemoryStop(limits);
  }
  return true;
}

bool QueryMemory_ShouldStop(QueryRequestLimits *limits, uint32_t *counter) {
  if (limits && QueryRequest_ShouldStop(limits)) {
    return true;
  }
  const uint32_t interval = QueryMemory_GetRatioReadInterval();
  // Both counters advance on every call, so a site that is called too rarely to reach the
  // interval alone still triggers a read through the request-wide count.
  bool due = ++*counter >= interval;
  if (limits) {
    due |= ++limits->memory.counter >= interval;
  }
  if (!due) {
    return false;
  }
  *counter = 0;
  if (limits) {
    limits->memory.counter = 0;
  }
  return readMemoryAndStop(limits);
}

bool QueryMemory_ShouldStopNow(QueryRequestLimits *limits) {
  if (limits && QueryRequest_ShouldStop(limits)) {
    return true;
  }
  return readMemoryAndStop(limits);
}
