/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SLOTS_STR "_SLOTS_INFO"

inline bool SlotRangeArray_ContainsSlot(const RedisModuleSlotRangeArray *slotRanges, uint16_t slot) {
  const RedisModuleSlotRange *ranges = slotRanges->ranges;
  for (int i = 0; i < slotRanges->num_ranges; i++) {
    if (ranges[i].start <= slot && slot <= ranges[i].end) {
      return true;
    }
  }
  return false;
}

/// @brief Get the memory size of a slot range array with the given number of ranges
/// @param num_ranges The number of ranges in the array
/// @return The memory size in bytes
size_t SlotRangeArray_SizeOf(uint32_t num_ranges);

/// @brief Serialize a slot range array to a newly allocated buffer in little-endian format
/// @param slot_range_array The slot range array to serialize
/// @returns A pointer to the serialized buffer
/// @note The caller is responsible for freeing the returned buffer with rm_free
char *SlotRangesArray_Serialize(const RedisModuleSlotRangeArray *slot_range_array);

/// @brief Deserialize a slot range array from a buffer in little-endian format
/// @param buf The buffer to deserialize from
/// @param buf_len The length of the buffer
/// @returns A pointer to the deserialized slot range array
/// @note The caller is responsible for freeing the returned structure with rm_free
RedisModuleSlotRangeArray *SlotRangesArray_Deserialize(const char *buf, size_t buf_len);

/// @brief Clone a slot range array
/// @param src The slot range array to clone
/// @return A pointer to the cloned slot range array
/// @note The caller is responsible for freeing the returned structure with rm_free
RedisModuleSlotRangeArray *SlotRangeArray_Clone(const RedisModuleSlotRangeArray *src);

#ifdef __cplusplus
}
#endif
