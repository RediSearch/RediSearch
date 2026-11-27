/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "hybrid/hybrid_arm.h"

// Arms are stored in slot order, so a slot's position counts the active slots before it.
TEST(HybridArmMaskTest, PositionAndSlotRoundTripForEverySubset) {
  for (unsigned mask = 1; mask < (1u << HYBRID_NUM_ARM_SLOTS); mask++) {
    const HybridArmMask m = (HybridArmMask)mask;
    size_t position = 0;
    for (int slot = 0; slot < HYBRID_NUM_ARM_SLOTS; slot++) {
      if (!HybridArmMask_Has(m, (HybridArmSlot)slot)) {
        continue;
      }
      EXPECT_EQ(HybridArmMask_Position(m, (HybridArmSlot)slot), position) << "mask " << mask;
      EXPECT_EQ(HybridArmMask_SlotAt(m, position), slot) << "mask " << mask;
      position++;
    }
    EXPECT_EQ(HybridArmMask_Count(m), position) << "mask " << mask;
  }
}

TEST(HybridArmMaskTest, TextAndSparse) {
  const HybridArmMask m = HybridArm_Bit(SEARCH_INDEX) | HybridArm_Bit(SPARSE_INDEX);
  EXPECT_FALSE(HybridArmMask_Has(m, VECTOR_INDEX));
  EXPECT_EQ(HybridArmMask_Position(m, SPARSE_INDEX), 1);
  EXPECT_EQ(HybridArmMask_SlotAt(m, 1), SPARSE_INDEX);
}
