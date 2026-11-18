/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "redismock/redismock.h"

#include "asm_import_window.h"
#include "asm_state_machine.h"
#include "spec.h"
#include "rmalloc.h"

#include <initializer_list>
#include <string>
#include <utility>

class ASMImportWindowTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    ASM_ImportWindow_Reset();
  }

  void TearDown() override {
    ASM_ImportWindow_Reset();
    RedisModule_FreeThreadSafeContext(ctx);
  }

  static RedisModuleSlotRangeArray *ranges(std::initializer_list<std::pair<uint16_t, uint16_t>> rs) {
    auto *array = static_cast<RedisModuleSlotRangeArray *>(
        rm_malloc(sizeof(RedisModuleSlotRangeArray) + rs.size() * sizeof(RedisModuleSlotRange)));
    array->num_ranges = static_cast<int32_t>(rs.size());
    int32_t i = 0;
    for (auto [start, end] : rs) {
      array->ranges[i++] = {start, end};
    }
    return array;
  }

  void apply(void (*transition)(const RedisModuleSlotRangeArray *),
             std::initializer_list<std::pair<uint16_t, uint16_t>> rs) {
    RedisModuleSlotRangeArray *array = ranges(rs);
    transition(array);
    rm_free(array);
  }

  // A key in `slot`, through a hash tag whose slot is known.
  RedisModuleString *keyInSlot(unsigned int slot) {
    for (unsigned int i = 0;; i++) {
      std::string name = "{t" + std::to_string(i) + "}:k";
      if (RedisModule_ClusterKeySlotC(name.c_str(), name.size()) == slot) {
        return RedisModule_CreateString(ctx, name.c_str(), name.size());
      }
    }
  }

  RedisModuleCtx *ctx = nullptr;
};

TEST_F(ASMImportWindowTest, ClosedByDefault) {
  RedisModuleString *key = keyInSlot(100);
  EXPECT_FALSE(ASM_ImportWindow_IsOpen());
  EXPECT_FALSE(ASM_ImportWindow_IgnoresNotification(key));
  EXPECT_FALSE(ASM_ImportWindow_AbsorbsUnlink(key));
  EXPECT_FALSE(ASM_ImportWindow_AbsorbsUnlink(nullptr));
  RedisModule_FreeString(ctx, key);
}

TEST_F(ASMImportWindowTest, OverlayIgnoresNotificationsAndAbsorbsUnlinksInItsSlotsOnly) {
  RedisModuleString *inside = keyInSlot(100);
  RedisModuleString *outside = keyInSlot(200);
  apply(ASM_ImportWindow_OnOverlayStart, {{50, 150}});

  EXPECT_EQ(ASM_ImportWindow_SlotPhase(100), ASM_IMPORT_SLOT_OVERLAY);
  EXPECT_TRUE(ASM_ImportWindow_IgnoresNotification(inside));
  EXPECT_TRUE(ASM_ImportWindow_AbsorbsUnlink(inside));
  EXPECT_FALSE(ASM_ImportWindow_IgnoresNotification(outside));
  EXPECT_FALSE(ASM_ImportWindow_AbsorbsUnlink(outside));
  EXPECT_FALSE(ASM_ImportWindow_AbsorbsUnlink(nullptr));
  EXPECT_EQ(ASM_ImportWindow_UnexpectedUnlinks(), 0);

  RedisModule_FreeString(ctx, inside);
  RedisModule_FreeString(ctx, outside);
}

TEST_F(ASMImportWindowTest, OverlayEndKeepsUnlinkGuardOnly) {
  RedisModuleString *key = keyInSlot(100);
  apply(ASM_ImportWindow_OnOverlayStart, {{100, 100}});
  apply(ASM_ImportWindow_OnOverlayEnd, {{100, 100}});

  EXPECT_EQ(ASM_ImportWindow_SlotPhase(100), ASM_IMPORT_SLOT_REBUILD);
  EXPECT_TRUE(ASM_ImportWindow_IsOpen());
  EXPECT_FALSE(ASM_ImportWindow_IgnoresNotification(key));
#ifndef ENABLE_ASSERT
  // An unlink once the overlay ended is unexpected: it still keeps the documents, and is counted.
  EXPECT_TRUE(ASM_ImportWindow_AbsorbsUnlink(key));
  EXPECT_EQ(ASM_ImportWindow_UnexpectedUnlinks(), 1);
#endif
  RedisModule_FreeString(ctx, key);
}

TEST_F(ASMImportWindowTest, OverlayEndLeavesSlotsOutsideTheOverlayAlone) {
  apply(ASM_ImportWindow_OnOverlayEnd, {{0, 16383}});
  EXPECT_FALSE(ASM_ImportWindow_IsOpen());

  apply(ASM_ImportWindow_OnOverlayStart, {{10, 20}});
  apply(ASM_ImportWindow_OnOverlayEnd, {{0, 16383}});
  EXPECT_EQ(ASM_ImportWindow_SlotPhase(9), ASM_IMPORT_SLOT_NONE);
  EXPECT_EQ(ASM_ImportWindow_SlotPhase(10), ASM_IMPORT_SLOT_REBUILD);
  EXPECT_EQ(ASM_ImportWindow_SlotPhase(20), ASM_IMPORT_SLOT_REBUILD);
  EXPECT_EQ(ASM_ImportWindow_SlotPhase(21), ASM_IMPORT_SLOT_NONE);
}

TEST_F(ASMImportWindowTest, CloseEndsTheWindowFromAnyPhase) {
  RedisModuleString *key = keyInSlot(100);
  apply(ASM_ImportWindow_OnOverlayStart, {{0, 99}, {100, 199}});
  apply(ASM_ImportWindow_OnOverlayEnd, {{0, 99}});

  apply(ASM_ImportWindow_Close, {{0, 199}});

  EXPECT_FALSE(ASM_ImportWindow_IsOpen());
  EXPECT_EQ(ASM_ImportWindow_SlotPhase(50), ASM_IMPORT_SLOT_NONE);
  EXPECT_EQ(ASM_ImportWindow_SlotPhase(150), ASM_IMPORT_SLOT_NONE);
  EXPECT_FALSE(ASM_ImportWindow_IgnoresNotification(key));
  EXPECT_FALSE(ASM_ImportWindow_AbsorbsUnlink(key));
  RedisModule_FreeString(ctx, key);
}

TEST_F(ASMImportWindowTest, ConcurrentImportsCloseIndependently) {
  RedisModuleString *first = keyInSlot(100);
  RedisModuleString *second = keyInSlot(9000);
  apply(ASM_ImportWindow_OnOverlayStart, {{0, 4999}});
  apply(ASM_ImportWindow_OnOverlayStart, {{5000, 16383}});

  apply(ASM_ImportWindow_Close, {{0, 4999}});

  EXPECT_FALSE(ASM_ImportWindow_IgnoresNotification(first));
  EXPECT_TRUE(ASM_ImportWindow_IgnoresNotification(second));
  apply(ASM_ImportWindow_Close, {{5000, 16383}});
  EXPECT_FALSE(ASM_ImportWindow_IsOpen());

  RedisModule_FreeString(ctx, first);
  RedisModule_FreeString(ctx, second);
}

TEST_F(ASMImportWindowTest, OverlapsAreCountedOnce) {
  apply(ASM_ImportWindow_OnOverlayStart, {{0, 10}, {5, 15}});
  apply(ASM_ImportWindow_OnOverlayStart, {{0, 15}});
  apply(ASM_ImportWindow_Close, {{0, 15}});
  EXPECT_FALSE(ASM_ImportWindow_IsOpen());
  EXPECT_EQ(ASM_ImportWindow_overlayWindowSlots_g, 0);
}

TEST_F(ASMImportWindowTest, OverlayEndOrCloseNeverOpenTheWindow) {
  apply(ASM_ImportWindow_OnOverlayEnd, {{0, 15}});
  apply(ASM_ImportWindow_Close, {{0, 15}});
  EXPECT_FALSE(ASM_ImportWindow_IsOpen());
  EXPECT_EQ(ASM_ImportWindow_SlotPhase(5), ASM_IMPORT_SLOT_NONE);
}

TEST_F(ASMImportWindowTest, StateMachineOpensTheWindowOnlyOnDisk) {
  const bool savedFlex = isFlex;

  isFlex = false;
  apply(ASM_StateMachine_StartImportOverlay, {{0, 15}});
  EXPECT_FALSE(ASM_ImportWindow_IsOpen());

  isFlex = true;
  apply(ASM_StateMachine_StartImportOverlay, {{0, 15}});
  EXPECT_EQ(ASM_ImportWindow_SlotPhase(5), ASM_IMPORT_SLOT_OVERLAY);
  apply(ASM_StateMachine_EndImportOverlay, {{0, 15}});
  EXPECT_EQ(ASM_ImportWindow_SlotPhase(5), ASM_IMPORT_SLOT_REBUILD);
  apply(ASM_StateMachine_FailImport, {{0, 15}});
  EXPECT_FALSE(ASM_ImportWindow_IsOpen());

  isFlex = savedFlex;
}
