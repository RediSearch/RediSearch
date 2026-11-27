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

#include "disk_asm_import_window.h"
#include "asm_state_machine.h"
#include "spec.h"
#include "rmalloc.h"

#include <initializer_list>
#include <string>
#include <utility>

class DiskASMImportWindowTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    DiskASM_ImportWindow_Reset();
  }

  void TearDown() override {
    DiskASM_ImportWindow_Reset();
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

TEST_F(DiskASMImportWindowTest, ClosedByDefault) {
  RedisModuleString *key = keyInSlot(100);
  EXPECT_EQ(DiskASM_ImportWindow_numSlots_g, 0);
  EXPECT_FALSE(DiskASM_IsSlotBeingImported(100));
  EXPECT_FALSE(DiskASM_IsKeyBeingImported(key));
  EXPECT_FALSE(DiskASM_IsKeyBeingImported(nullptr));
  RedisModule_FreeString(ctx, key);
}

TEST_F(DiskASMImportWindowTest, CoversItsSlotsOnly) {
  RedisModuleString *inside = keyInSlot(100);
  RedisModuleString *outside = keyInSlot(200);
  apply(DiskASM_ImportWindow_Open, {{50, 150}});

  EXPECT_EQ(DiskASM_ImportWindow_numSlots_g, 101);
  EXPECT_TRUE(DiskASM_IsSlotBeingImported(50));
  EXPECT_TRUE(DiskASM_IsSlotBeingImported(150));
  EXPECT_FALSE(DiskASM_IsSlotBeingImported(49));
  EXPECT_FALSE(DiskASM_IsSlotBeingImported(151));
  EXPECT_TRUE(DiskASM_IsKeyBeingImported(inside));
  EXPECT_FALSE(DiskASM_IsKeyBeingImported(outside));

  RedisModule_FreeString(ctx, inside);
  RedisModule_FreeString(ctx, outside);
}

TEST_F(DiskASMImportWindowTest, CloseNeverOpensTheWindow) {
  apply(DiskASM_ImportWindow_Close, {{0, 15}});
  EXPECT_EQ(DiskASM_ImportWindow_numSlots_g, 0);
  EXPECT_FALSE(DiskASM_IsSlotBeingImported(5));
}

TEST_F(DiskASMImportWindowTest, ConcurrentImportsCloseIndependently) {
  RedisModuleString *first = keyInSlot(100);
  RedisModuleString *second = keyInSlot(9000);
  apply(DiskASM_ImportWindow_Open, {{0, 4999}});
  apply(DiskASM_ImportWindow_Open, {{5000, 16383}});

  apply(DiskASM_ImportWindow_Close, {{0, 4999}});

  EXPECT_FALSE(DiskASM_IsKeyBeingImported(first));
  EXPECT_TRUE(DiskASM_IsKeyBeingImported(second));
  apply(DiskASM_ImportWindow_Close, {{5000, 16383}});
  EXPECT_EQ(DiskASM_ImportWindow_numSlots_g, 0);

  RedisModule_FreeString(ctx, first);
  RedisModule_FreeString(ctx, second);
}

TEST_F(DiskASMImportWindowTest, OverlapsAreCountedOnce) {
  apply(DiskASM_ImportWindow_Open, {{0, 10}, {5, 15}});
  apply(DiskASM_ImportWindow_Open, {{0, 15}});
  EXPECT_EQ(DiskASM_ImportWindow_numSlots_g, 16);

  apply(DiskASM_ImportWindow_Close, {{0, 7}, {4, 15}});
  EXPECT_EQ(DiskASM_ImportWindow_numSlots_g, 0);
}

TEST_F(DiskASMImportWindowTest, StateMachineOpensTheWindowOnlyOnDisk) {
  const bool savedFlex = isFlex;

  isFlex = false;
  apply(ASM_StateMachine_StartImportOverlay, {{0, 15}});
  EXPECT_EQ(DiskASM_ImportWindow_numSlots_g, 0);

  isFlex = true;
  apply(ASM_StateMachine_StartImportOverlay, {{0, 15}});
  EXPECT_TRUE(DiskASM_IsSlotBeingImported(5));
  apply(ASM_StateMachine_FailImport, {{0, 15}});
  EXPECT_EQ(DiskASM_ImportWindow_numSlots_g, 0);

  isFlex = savedFlex;
}
