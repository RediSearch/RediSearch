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

bool SlotRangeArray_ContainsSlot(const RedisModuleSlotRangeArray *slotRanges, uint16_t slot) {
  const RedisModuleSlotRange *ranges = slotRanges->ranges;
  for (int i = 0; i < slotRanges->num_ranges; i++) {
    if (ranges[i].start <= slot && slot <= ranges[i].end) {
      return true;
    }
  }
  return false;
}
