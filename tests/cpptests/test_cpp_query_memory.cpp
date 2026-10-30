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
#include <vector>

#include "config.h"
#include "query_memory.h"
#include "query_request.h"
#include "redismodule.h"

namespace {

float fakeRatio = 0;
std::atomic<unsigned> fakeRatioReads = 0;

float FakeGetRawUsedMemoryRatio() {
  fakeRatioReads.fetch_add(1, std::memory_order_relaxed);
  return fakeRatio;
}

class QueryMemoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    previousApi_ = RedisModule_GetRawUsedMemoryRatio;
    previousPolicy_ = RSGlobalConfig.requestConfigParams.oomPolicy;
    RedisModule_GetRawUsedMemoryRatio = FakeGetRawUsedMemoryRatio;
    RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Return;
    fakeRatio = 0;
    fakeRatioReads = 0;

    TimeoutConfig timeoutConfig = {0, TimeoutPolicy_Return};
    QueryRequestTimeout_Init(&timeout_, &timeoutConfig);
  }

  void TearDown() override {
    RedisModule_GetRawUsedMemoryRatio = previousApi_;
    RSGlobalConfig.requestConfigParams.oomPolicy = previousPolicy_;
    QueryMemory_ResetFakePressure();
  }

  QueryRequestTimeout timeout_ = {};

 private:
  decltype(RedisModule_GetRawUsedMemoryRatio) previousApi_;
  RSOomPolicy previousPolicy_;
};

TEST_F(QueryMemoryTest, ReadsMemoryOncePerInterval) {
  ASSERT_EQ(QueryMemory_GetRatioReadInterval(), QUERY_MEMORY_RATIO_READ_INTERVAL);
  uint32_t counter = 0;
  for (int round = 1; round <= 3; ++round) {
    for (uint32_t i = 0; i < QUERY_MEMORY_RATIO_READ_INTERVAL; ++i) {
      EXPECT_FALSE(QueryMemory_ShouldStop(&timeout_, &counter));
    }
    EXPECT_EQ(fakeRatioReads.load(), (unsigned)round);
  }
}

TEST_F(QueryMemoryTest, PressureRequestsAnOomStop) {
  QueryMemory_SetRatioReadInterval(1);
  uint32_t counter = 0;
  fakeRatio = 0.99f;
  EXPECT_FALSE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout_), QUERY_STOP_REASON_NONE);

  fakeRatio = 1.0f;
  EXPECT_TRUE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout_), QUERY_STOP_REASON_OOM);
  // The stop is what every other check of this request now observes.
  EXPECT_TRUE(QueryRequestTimeout_IsTimedOut(&timeout_));

  // Sticky without reading memory again, even if pressure is gone.
  fakeRatio = 0;
  const unsigned reads = fakeRatioReads.load();
  EXPECT_TRUE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_EQ(fakeRatioReads.load(), reads);
}

TEST_F(QueryMemoryTest, AnEarlierStopKeepsItsReason) {
  QueryMemory_SetRatioReadInterval(1);
  uint32_t counter = 0;
  QueryRequestTimeout_RequestStop(&timeout_, QUERY_STOP_REASON_TIMEOUT);
  fakeRatio = 2.0f;
  EXPECT_TRUE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout_), QUERY_STOP_REASON_TIMEOUT);
}

TEST_F(QueryMemoryTest, ReportsTheBlockedClientTimeout) {
  uint32_t counter = 0;
  QueryRequestTimeout_BeginCycle(&timeout_, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestTimeout_MarkTimedOut(&timeout_);
  EXPECT_TRUE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_EQ(fakeRatioReads.load(), 0u);
}

TEST_F(QueryMemoryTest, OffWhenOnOomIsIgnore) {
  QueryMemory_SetRatioReadInterval(1);
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
  fakeRatio = 5.0f;
  uint32_t counter = 0;
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_ShouldStop(&timeout_, &counter));
  }
  EXPECT_FALSE(QueryRequestTimeout_IsStopRequested(&timeout_));
}

TEST_F(QueryMemoryTest, OffWithoutTheMemoryApi) {
  QueryMemory_SetRatioReadInterval(1);
  RedisModule_GetRawUsedMemoryRatio = nullptr;
  uint32_t counter = 0;
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_ShouldStop(&timeout_, &counter));
  }
  EXPECT_FALSE(QueryRequestTimeout_IsStopRequested(&timeout_));
}

TEST_F(QueryMemoryTest, OffWithoutAMemoryLimit) {
  QueryMemory_SetRatioReadInterval(1);
  fakeRatio = 0;  // The API's answer when no limit is configured.
  uint32_t counter = 0;
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_ShouldStop(&timeout_, &counter));
  }
  EXPECT_EQ(fakeRatioReads.load(), 10u);
}

TEST_F(QueryMemoryTest, WithoutARequestReportsPressureOnly) {
  QueryMemory_SetRatioReadInterval(1);
  fakeRatio = 1.5f;
  uint32_t counter = 0;
  EXPECT_TRUE(QueryMemory_ShouldStop(nullptr, &counter));
}

TEST_F(QueryMemoryTest, ConcurrentCheckpointsStopTheSameRequest) {
  QueryMemory_SetRatioReadInterval(1);
  fakeRatio = 1.0f;
  std::vector<std::thread> checkpoints;
  std::atomic<int> stopped = 0;
  for (int t = 0; t < 4; ++t) {
    checkpoints.emplace_back([&] {
      uint32_t counter = 0;
      if (QueryMemory_ShouldStop(&timeout_, &counter)) {
        stopped.fetch_add(1);
      }
    });
  }
  for (auto &checkpoint : checkpoints) {
    checkpoint.join();
  }
  EXPECT_EQ(stopped.load(), 4);
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout_), QUERY_STOP_REASON_OOM);
}

TEST_F(QueryMemoryTest, ForcedRatioReplacesTheMemoryRead) {
  QueryMemory_SetRatioReadInterval(1);
  RedisModule_GetRawUsedMemoryRatio = nullptr;
  uint32_t counter = 0;
  QueryMemory_SetForcedRatio(0.5f);
  EXPECT_FALSE(QueryMemory_ShouldStop(&timeout_, &counter));
  QueryMemory_SetForcedRatio(1.0f);
  EXPECT_TRUE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout_), QUERY_STOP_REASON_OOM);
}

TEST_F(QueryMemoryTest, NegativeForcedRatioRestoresTheRealRead) {
  QueryMemory_SetRatioReadInterval(1);
  uint32_t counter = 0;
  QueryMemory_SetForcedRatio(3.0f);
  QueryMemory_SetForcedRatio(-1);
  EXPECT_LT(QueryMemory_GetForcedRatio(), 0);
  EXPECT_FALSE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_EQ(fakeRatioReads.load(), 1u);
}

TEST_F(QueryMemoryTest, PressureAtCallStopsOnlyTheNthCall) {
  TimeoutConfig timeoutConfig = {0, TimeoutPolicy_Return};
  QueryRequestTimeout other = {};
  QueryRequestTimeout_Init(&other, &timeoutConfig);
  uint32_t counter = 0, otherCounter = 0;

  QueryMemory_SetPressureAtCall(3);
  EXPECT_FALSE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_FALSE(QueryMemory_ShouldStop(&other, &otherCounter));
  // The third call reports pressure without waiting for the read interval or reading memory.
  EXPECT_TRUE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout_), QUERY_STOP_REASON_OOM);
  EXPECT_EQ(fakeRatioReads.load(), 0u);

  // Calls that return on the requested stop are not counted.
  EXPECT_TRUE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_EQ(QueryMemory_GetCheckpointCalls(), 3u);

  EXPECT_FALSE(QueryMemory_ShouldStop(&other, &otherCounter));
  EXPECT_FALSE(QueryRequestTimeout_IsStopRequested(&other));

  // Re-arming restarts the count.
  QueryMemory_SetPressureAtCall(1);
  EXPECT_EQ(QueryMemory_GetCheckpointCalls(), 0u);
  EXPECT_TRUE(QueryMemory_ShouldStop(&other, &otherCounter));
}

TEST_F(QueryMemoryTest, PressureAtCallHonorsOnOomIgnore) {
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
  uint32_t counter = 0;
  QueryMemory_SetPressureAtCall(1);
  EXPECT_FALSE(QueryMemory_ShouldStop(&timeout_, &counter));
  EXPECT_FALSE(QueryRequestTimeout_IsStopRequested(&timeout_));
}

TEST_F(QueryMemoryTest, ResetClearsEveryFake) {
  QueryMemory_SetForcedRatio(2.0f);
  QueryMemory_SetPressureAtCall(5);
  QueryMemory_SetRatioReadInterval(1);
  QueryMemory_ResetFakePressure();
  EXPECT_LT(QueryMemory_GetForcedRatio(), 0);
  EXPECT_EQ(QueryMemory_GetPressureAtCall(), 0u);
  EXPECT_EQ(QueryMemory_GetRatioReadInterval(), (uint32_t)QUERY_MEMORY_RATIO_READ_INTERVAL);
}

}  // namespace
