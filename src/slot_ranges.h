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

/// @brief Compare two slot range arrays
/// @param ranges1 The slot range array from which we expect the keys to be
/// @param ranges2 The slot range from which we actually have keys
/// @returns SLOT_RANGES_MATCH if the ranges are identical, SLOT_RANGES_SUBSET if ranges_expected is a subset of ranges_actual,
/// and SLOT_RANGES_DOES_NOT_INCLUDE if any of the range in ranges_expected is not in ranges_actual
SlotRangesComparisonResult CompareSlotRanges(const RedisModuleSlotRangeArray *ranges_expected,
                                             const RedisModuleSlotRangeArray *ranges_actual);

#ifdef __cplusplus
}
#endif
