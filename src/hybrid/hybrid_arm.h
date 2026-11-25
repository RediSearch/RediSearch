/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "rmutil/rm_assert.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * The arms of an FT.HYBRID query. Each kind of arm has a fixed slot, and a request records the
 * slots it uses in an arm mask. The slot order is the arm order everywhere arms are listed: the
 * request's array of sub-requests, the fusion inputs, EXPLAINSCORE and the shard cursor mapping.
 * The mask is derived from the parsed arms on every node; it never goes on the wire.
 */
typedef enum {
  SEARCH_INDEX = 0,  // the text arm, `SEARCH`
  VECTOR_INDEX = 1,  // a `VSIM` arm on a dense VECTOR field
  SPARSE_INDEX = 2,  // a `VSIM` arm on a SPARSEVECTOR field
  HYBRID_NUM_ARM_SLOTS,
} HybridArmSlot;

typedef uint8_t HybridArmMask;

#ifdef __cplusplus
static_assert(HYBRID_NUM_ARM_SLOTS <= 8 * sizeof(HybridArmMask),
              "every arm slot needs a bit in HybridArmMask");
#else
_Static_assert(HYBRID_NUM_ARM_SLOTS <= 8 * sizeof(HybridArmMask),
               "every arm slot needs a bit in HybridArmMask");
#endif

static inline HybridArmMask HybridArm_Bit(HybridArmSlot slot) {
  return (HybridArmMask)(1u << slot);
}

static inline bool HybridArmMask_Has(HybridArmMask mask, HybridArmSlot slot) {
  return (mask & HybridArm_Bit(slot)) != 0;
}

static inline size_t HybridArmMask_Count(HybridArmMask mask) {
  return (size_t)__builtin_popcount(mask);
}

/* The position of `slot`'s arm among the active arms, which are stored in slot order. */
static inline size_t HybridArmMask_Position(HybridArmMask mask, HybridArmSlot slot) {
  RS_ASSERT(HybridArmMask_Has(mask, slot));
  return HybridArmMask_Count(mask & (HybridArmMask)(HybridArm_Bit(slot) - 1));
}

/* The slot of the arm at `position` among the active arms. */
static inline HybridArmSlot HybridArmMask_SlotAt(HybridArmMask mask, size_t position) {
  for (int slot = 0; slot < HYBRID_NUM_ARM_SLOTS; slot++) {
    if (HybridArmMask_Has(mask, (HybridArmSlot)slot) && position-- == 0) {
      return (HybridArmSlot)slot;
    }
  }
  RS_ABORT("position is past the last active arm");
  return HYBRID_NUM_ARM_SLOTS;
}

#ifdef __cplusplus
}
#endif
