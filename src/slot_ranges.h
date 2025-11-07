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

typedef struct SharedSlotRangeArray SharedSlotRangeArray;

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

/// @brief Get the serialized size of a slot range array
/// @param array The slot range array to measure
/// @return The serialized size in bytes
size_t SlotRangeArray_SerializedSize(const RedisModuleSlotRangeArray *array);

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
