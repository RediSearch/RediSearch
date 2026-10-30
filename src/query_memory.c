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
  // Global like QueryMemoryGuard's, not the request's snapshot. A plain enum read racing
  // CONFIG SET sees the old or the new policy, both valid.
  if (RSGlobalConfig.requestConfigParams.oomPolicy == OomPolicy_Ignore) {
    return false;
  }
  return RedisMemory_GetRawUsedMemoryRatio() >= REDIS_MEMORY_OOM_RATIO;
}

bool QueryMemory_ShouldStop(QueryRequestTimeout *timeout, uint32_t *counter) {
  if (timeout && QueryRequest_ShouldStop(timeout)) {
    return true;
  }
  if (++*counter < QueryMemory_GetRatioReadInterval()) {
    return false;
  }
  *counter = 0;
  if (!underMemoryPressure()) {
    return false;
  }
  if (timeout) {
    // A lost race keeps the earlier reason; the query stops either way.
    QueryRequestTimeout_RequestStop(timeout, QUERY_STOP_REASON_OOM);
  }
  return true;
}
