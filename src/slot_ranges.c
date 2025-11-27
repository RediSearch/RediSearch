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

inline size_t SlotRangeArray_SizeOf(uint32_t num_ranges) {
  return sizeof(RedisModuleSlotRangeArray) + num_ranges * sizeof(RedisModuleSlotRange);
}

// Protocol helpers for endianness conversion.
// SlotRangeArray are always serialized in little-endian format.
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define SlotRangesArray_ToLittleEndian(arr)
#define SlotRangesArray_FromLittleEndian(arr)
#define SlotRangesArray_NumRanges_FromLE(arr) ((arr)->num_ranges)
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define SlotRangesArray_ToLittleEndian(arr) SlotRangesArray_SwapEndian(arr, true)
#define SlotRangesArray_FromLittleEndian(arr) SlotRangesArray_SwapEndian(arr, false)
#define SlotRangesArray_NumRanges_FromLE(arr) (__builtin_bswap32((arr)->num_ranges))
static inline void SlotRangesArray_SwapEndian(RedisModuleSlotRangeArray *slot_range_array, bool native_input) {
  // Extract the original number of ranges
  int32_t num_ranges = native_input ? slot_range_array->num_ranges : __builtin_bswap32(slot_range_array->num_ranges);
  // Convert each element to little-endian by type sizing and swapping
  slot_range_array->num_ranges = __builtin_bswap32(slot_range_array->num_ranges);
  for (int i = 0; i < num_ranges; i++) {
    slot_range_array->ranges[i].start = __builtin_bswap16(slot_range_array->ranges[i].start);
    slot_range_array->ranges[i].end = __builtin_bswap16(slot_range_array->ranges[i].end);
  }
}
#endif

RedisModuleSlotRangeArray *SlotRangeArray_Clone(const RedisModuleSlotRangeArray *src) {
  size_t total_size = SlotRangeArray_SizeOf(src->num_ranges);
  RedisModuleSlotRangeArray *copy = rm_malloc(total_size);
  memcpy(copy, src, total_size);
  return copy;
}

// Serialize a slot range array to a newly allocated buffer in little-endian format
// The caller is responsible for freeing the returned buffer with rm_free
char *SlotRangesArray_Serialize(const RedisModuleSlotRangeArray *slot_range_array) {
  RedisModuleSlotRangeArray *copy = SlotRangeArray_Clone(slot_range_array);
  SlotRangesArray_ToLittleEndian(copy);
  return (char *)copy;
}

// Deserialize a slot range array from a buffer in little-endian format
// The caller is responsible for freeing the returned structure with rm_free
RedisModuleSlotRangeArray *SlotRangesArray_Deserialize(const char *buf, size_t buf_len) {
  if (buf_len < sizeof(RedisModuleSlotRangeArray)) {
    return NULL; // Buffer too small to contain header
  }
  const RedisModuleSlotRangeArray *maybe_array = (const RedisModuleSlotRangeArray *)buf;
  if (buf_len != SlotRangeArray_SizeOf(SlotRangesArray_NumRanges_FromLE(maybe_array))) {
    return NULL; // Size mismatch - cannot parse
  }

  // Copy the input buffer to a new allocation
  RedisModuleSlotRangeArray *slot_range_array = rm_malloc(buf_len);
  memcpy(slot_range_array, buf, buf_len);

  // Convert from little-endian to host-endian
  SlotRangesArray_FromLittleEndian(slot_range_array);

  return slot_range_array;
}
