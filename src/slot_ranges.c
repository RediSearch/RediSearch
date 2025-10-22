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
#include "redismodule.h"
#include "rmutil/rm_assert.h"

#include <stdatomic.h>

extern RedisModuleCtx *RSDummyContext;

struct SharedSlotRangeArray {
  atomic_uint refcount;
  RedisModuleSlotRangeArray array;
};

// Cached local slots. Initially NULL
// Set to allocated SharedSlotRangeArray when Slots_GetLocalSlots is called (if NULL)
// Dropped when Slots_DropCachedLocalSlots is called (when we know local slots have changed)
static SharedSlotRangeArray *localSlots = NULL;

const SharedSlotRangeArray *Slots_GetLocalSlots(void) {
  if (!localSlots) {
    RedisModuleSlotRangeArray *ranges = RedisModule_ClusterGetLocalSlotRanges(RSDummyContext);
    RS_LOG_ASSERT(ranges != NULL, "Expected non-NULL ranges from ClusterGetLocalSlotRanges in any mode");

    localSlots = rm_malloc(sizeof(SharedSlotRangeArray) + sizeof(RedisModuleSlotRange) * ranges->num_ranges);
    localSlots->array.num_ranges = ranges->num_ranges;
    memcpy(localSlots->array.ranges, ranges->ranges, sizeof(RedisModuleSlotRange) * ranges->num_ranges);
    atomic_init(&localSlots->refcount, 2); // One for the caller, one for the cache
    RedisModule_ClusterFreeSlotRanges(RSDummyContext, ranges);
  } else {
    atomic_fetch_add_explicit(&localSlots->refcount, 1, memory_order_acquire);
  }
  return localSlots;
}

void Slots_FreeLocalSlots(const SharedSlotRangeArray *slots) {
  SharedSlotRangeArray *slots_ = (SharedSlotRangeArray *)slots; // Cast away constness for refcount management
  if (slots_ && atomic_fetch_sub_explicit(&slots_->refcount, 1, memory_order_release) == 1) {
    rm_free(slots_);
  }
}

// Drops the cached info - used when we know local slots have changed (or might have changed)
void Slots_DropCachedLocalSlots(void) {
  Slots_FreeLocalSlots(localSlots);
  localSlots = NULL;
}

inline bool Slots_CanAccessKeysInSlot(const SharedSlotRangeArray *slotRanges, uint16_t slot) {
  const RedisModuleSlotRange *ranges = slotRanges->array.ranges;
  for (int i = 0; i < slotRanges->array.num_ranges; i++) {
    if (ranges[i].start <= slot && slot <= ranges[i].end) {
      return true;
    }
  }
  return false;
}


// Helper function to check if a slot is covered by any range in the array
static inline bool SlotInRanges(const RedisModuleSlotRangeArray *ranges, uint16_t slot) {
  for (int i = 0; i < ranges->num_ranges; i++) {
    if (ranges->ranges[i].start <= slot && slot <= ranges->ranges[i].end) {
      return true;
    }
  }
  return false;
}

SlotRangesComparisonResult CompareSlotRanges(const RedisModuleSlotRangeArray *ranges_expected,
                                             const RedisModuleSlotRangeArray *ranges_actual) {
  RS_ASSERT(ranges_expected)
  RS_ASSERT(ranges_actual)
  RS_ASSERT(ranges_expected->num_ranges > 0)
  RS_ASSERT(ranges_actual->num_ranges > 0)

  bool all_slots_covered = true;
  bool exact_match = true;

  // Check if all expected ranges are covered by actual ranges
  for (int i = 0; i < ranges_expected->num_ranges; i++) {
    const RedisModuleSlotRange *expected_range = &ranges_expected->ranges[i];

    // Check every slot in this expected range
    for (uint16_t slot = expected_range->start; slot <= expected_range->end; slot++) {
      if (!SlotInRanges(ranges_actual, slot)) {
        return SLOT_RANGES_DOES_NOT_INCLUDE;
      }
    }
  }

  // At this point, we know all expected slots are covered by actual ranges
  // Now check if it's an exact match by verifying actual doesn't have extra slots
  for (int i = 0; i < ranges_actual->num_ranges; i++) {
    const RedisModuleSlotRange *actual_range = &ranges_actual->ranges[i];

    // Check every slot in this actual range
    for (uint16_t slot = actual_range->start; slot <= actual_range->end; slot++) {
      if (!SlotInRanges(ranges_expected, slot)) {
        exact_match = false;
        break;
      }
    }

    if (!exact_match) {
      break;
    }
  }

  return exact_match ? SLOT_RANGES_MATCH : SLOT_RANGES_SUBSET;
}
