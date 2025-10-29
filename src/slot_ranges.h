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
#include <stddef.h>
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SharedSlotRangeArray SharedSlotRangeArray;
typedef struct RedisModuleSlotRangeArray RedisModuleSlotRangeArray;

typedef enum {
  SLOT_RANGES_MATCH,
  SLOT_RANGES_SUBSET,
  SLOT_RANGES_DOES_NOT_INCLUDE
} SlotRangesComparisonResult;

/// @brief Get slot ranges for the local node. The returned value must be freed using FreeLocalSlots
/// @returns A pointer to the shared slot range array, or NULL if not in cluster mode
/// @warning MUST be called from the main thread
const SharedSlotRangeArray *Slots_GetLocalSlots(void);

/// @brief Free the shared slot range array
/// @param slots The slot range array to free (can be NULL)
/// @note Safe to call from any thread
void Slots_FreeLocalSlots(const SharedSlotRangeArray *slots);

/// Drops the cached local slot ranges
/// @warning MUST be called from the main thread
void Slots_DropCachedLocalSlots(void);

/// @brief Check if the given slot can be accessed according to the given slot ranges
/// @param slotRanges The slot ranges to check against
/// @param slot The slot to check
/// @returns true if the slot is in one of the ranges, false otherwise
bool Slots_CanAccessKeysInSlot(const SharedSlotRangeArray *slotRanges, uint16_t slot);

/* Size helper for binary wire format: 4 + 4*n bytes */
size_t RedisModuleSlotRangeArray_SerializedSize_Binary(uint32_t num_ranges);

/* -------- Binary -------- */
/// @brief Serialize slot range array to binary format using client-provided buffer
/// - It is expected that the client has called RedisModuleSlotRangeArray_SerializedSize_Binary to determine the required buffer size and allocate the buffer
/// @param a The slot range array to serialize
/// @param out_buf Client-allocated buffer to write to
/// @param buf_len Size of the client-allocated buffer
/// @returns true on success, false on error (including insufficient buffer size)
bool RedisModuleSlotRangeArray_SerializeBinary(
      const RedisModuleSlotRangeArray *a,
      uint8_t *out_buf,
      size_t buf_len);

/// @brief Deserialize slot range array from binary format using client-provided buffer
/// @param in_buf Input buffer containing serialized data
/// @param in_len Size of input buffer
/// @param out Client-allocated RedisModuleSlotRangeArray with sufficient space for ranges
/// @returns true on success, false on error
bool RedisModuleSlotRangeArray_DeserializeBinary(
      const uint8_t *in_buf,
      size_t in_len,
      RedisModuleSlotRangeArray *out);

/// @brief Compare two slot range arrays.
/// This has some assumptions:
///
/// - The ranges are sorted
/// - The ranges are non-overlapping
/// - The ranges come in merged form (adjacent ranges are merged into a single range)
///
/// @param ranges1 The slot range array from which we expect the keys to be
/// @param ranges2 The slot range from which we actually have keys
/// @returns SLOT_RANGES_MATCH if the ranges are identical, SLOT_RANGES_SUBSET if ranges_expected is a subset of ranges_actual,
/// and SLOT_RANGES_DOES_NOT_INCLUDE if any of the range in ranges_expected is not in ranges_actual
SlotRangesComparisonResult CompareSlotRanges(const RedisModuleSlotRangeArray *ranges_expected,
                                             const RedisModuleSlotRangeArray *ranges_actual);

/// @brief Create a RedisModuleSlotRangeArray from a cluster shards reply slots array
/// @param slots The Redis module call reply containing slot ranges (array of start/end pairs)
/// @param slotRanges Output parameter to store the created slot range array
/// @returns true on success, false on error
bool RedisModuleSlotRangeArray_FromClusterShardsReply(RedisModuleCallReply *slots, RedisModuleSlotRangeArray **slotRanges);

#ifdef __cplusplus
}
#endif
