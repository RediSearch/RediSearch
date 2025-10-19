/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "slot_ranges.h"
#include "rmalloc.h"

extern RedisModuleCtx *RSDummyContext;
static SharedSlotRangeArray *localSlots = NULL;

SharedSlotRangeArray *GetLocalSlots(void) {
  if (!localSlots) {
    RedisModuleSlotRangeArray *ranges = RedisModule_ClusterGetLocalSlotRanges(RSDummyContext);
    if (!ranges) return NULL; // Not in cluster mode

    localSlots = rm_calloc(1, sizeof(SharedSlotRangeArray) + sizeof(RedisModuleSlotRange) * ranges->num_ranges);
    localSlots->array.num_ranges = ranges->num_ranges;
    memcpy(localSlots->array.ranges, ranges->ranges, sizeof(RedisModuleSlotRange) * ranges->num_ranges);
    atomic_init(&localSlots->refcount, 1);
    RedisModule_ClusterFreeSlotRanges(RSDummyContext, ranges);
  } else {
    atomic_fetch_add_explicit(&localSlots->refcount, 1, memory_order_acquire);
  }
  return localSlots;
}

void FreeLocalSlots(SharedSlotRangeArray *slots) {
  if (slots && atomic_fetch_sub_explicit(&slots->refcount, 1, memory_order_release) == 1) {
    rm_free(slots);
    localSlots = NULL;
  }
}

// Drops the cached local
void DropCachedLocalSlots(void) {
  FreeLocalSlots(localSlots);
  localSlots = NULL;
}
