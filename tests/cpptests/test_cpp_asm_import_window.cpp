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
  EXPECT_EQ(ASM_ImportWindow_numSlots_g, 0);
  EXPECT_FALSE(ASM_ImportWindow_ContainsSlot(100));
  EXPECT_FALSE(ASM_ImportWindow_ContainsKey(key));
  EXPECT_FALSE(ASM_ImportWindow_ContainsKey(nullptr));
  RedisModule_FreeString(ctx, key);
}

TEST_F(ASMImportWindowTest, CoversItsSlotsOnly) {
  RedisModuleString *inside = keyInSlot(100);
  RedisModuleString *outside = keyInSlot(200);
  apply(ASM_ImportWindow_Open, {{50, 150}});

  EXPECT_EQ(ASM_ImportWindow_numSlots_g, 101);
  EXPECT_TRUE(ASM_ImportWindow_ContainsSlot(50));
  EXPECT_TRUE(ASM_ImportWindow_ContainsSlot(150));
  EXPECT_FALSE(ASM_ImportWindow_ContainsSlot(49));
  EXPECT_FALSE(ASM_ImportWindow_ContainsSlot(151));
  EXPECT_TRUE(ASM_ImportWindow_ContainsKey(inside));
  EXPECT_FALSE(ASM_ImportWindow_ContainsKey(outside));

  RedisModule_FreeString(ctx, inside);
  RedisModule_FreeString(ctx, outside);
}

TEST_F(ASMImportWindowTest, CloseNeverOpensTheWindow) {
  apply(ASM_ImportWindow_Close, {{0, 15}});
  EXPECT_EQ(ASM_ImportWindow_numSlots_g, 0);
  EXPECT_FALSE(ASM_ImportWindow_ContainsSlot(5));
}

TEST_F(ASMImportWindowTest, ConcurrentImportsCloseIndependently) {
  RedisModuleString *first = keyInSlot(100);
  RedisModuleString *second = keyInSlot(9000);
  apply(ASM_ImportWindow_Open, {{0, 4999}});
  apply(ASM_ImportWindow_Open, {{5000, 16383}});

  apply(ASM_ImportWindow_Close, {{0, 4999}});

  EXPECT_FALSE(ASM_ImportWindow_ContainsKey(first));
  EXPECT_TRUE(ASM_ImportWindow_ContainsKey(second));
  apply(ASM_ImportWindow_Close, {{5000, 16383}});
  EXPECT_EQ(ASM_ImportWindow_numSlots_g, 0);

  RedisModule_FreeString(ctx, first);
  RedisModule_FreeString(ctx, second);
}

TEST_F(ASMImportWindowTest, OverlapsAreCountedOnce) {
  apply(ASM_ImportWindow_Open, {{0, 10}, {5, 15}});
  apply(ASM_ImportWindow_Open, {{0, 15}});
  EXPECT_EQ(ASM_ImportWindow_numSlots_g, 16);

  apply(ASM_ImportWindow_Close, {{0, 7}, {4, 15}});
  EXPECT_EQ(ASM_ImportWindow_numSlots_g, 0);
}

TEST_F(ASMImportWindowTest, StateMachineOpensTheWindowOnlyOnDisk) {
  const bool savedFlex = isFlex;

  isFlex = false;
  apply(ASM_StateMachine_StartImportOverlay, {{0, 15}});
  EXPECT_EQ(ASM_ImportWindow_numSlots_g, 0);

  isFlex = true;
  apply(ASM_StateMachine_StartImportOverlay, {{0, 15}});
  EXPECT_TRUE(ASM_ImportWindow_ContainsSlot(5));
  apply(ASM_StateMachine_EndImport, {{0, 15}});
  EXPECT_EQ(ASM_ImportWindow_numSlots_g, 0);

  isFlex = savedFlex;
}
