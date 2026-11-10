/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "gtest/gtest.h"

#include <atomic>
#include <thread>

#include "redismock/redismock.h"
#include "util/gil_gate.h"

class GilGateTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx = nullptr;
  GilGate gate;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    GilGate_Reset(&gate);
  }

  void TearDown() override {
    RedisModule_FreeThreadSafeContext(ctx);
  }
};

TEST_F(GilGateTest, UncontendedLockReallyLocks) {
  ASSERT_TRUE(GilGate_Lock(&gate, ctx));
  // Held for real: nobody else can take it.
  std::thread([&] { EXPECT_NE(RedisModule_ThreadSafeContextTryLock(ctx), REDISMODULE_OK); }).join();
  GilGate_Unlock(&gate, ctx);
  // Released for real.
  std::thread([&] {
    ASSERT_EQ(RedisModule_ThreadSafeContextTryLock(ctx), REDISMODULE_OK);
    RedisModule_ThreadSafeContextUnlock(ctx);
  }).join();
}

TEST_F(GilGateTest, JoiningLetsWorkerBorrowTheGil) {
  // The main thread holds the GIL, as it does at shutdown.
  RedisModule_ThreadSafeContextLock(ctx);

  std::atomic<bool> started{false};
  std::atomic<int> result{-1};
  std::thread worker([&] {
    started = true;
    result = GilGate_Lock(&gate, ctx) ? 1 : 0;  // 0: borrowed, not really taken
    GilGate_Unlock(&gate, ctx);                 // must not release the main thread's GIL
  });
  while (!started) std::this_thread::yield();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_EQ(result, -1) << "worker returned before the join began";

  GilGate_BeginJoin(&gate);
  worker.join();
  EXPECT_EQ(result, 0);

  // Still held by this thread: a worker's Unlock was a no-op.
  std::thread([&] { EXPECT_NE(RedisModule_ThreadSafeContextTryLock(ctx), REDISMODULE_OK); }).join();
  RedisModule_ThreadSafeContextUnlock(ctx);
}
