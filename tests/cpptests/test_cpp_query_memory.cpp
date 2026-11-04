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

float FakeGetUsedMemoryRatioEstimate() {
  fakeRatioReads.fetch_add(1, std::memory_order_relaxed);
  return fakeRatio;
}

class QueryMemoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    previousApi_ = RedisModule_GetUsedMemoryRatioEstimate;
    previousPolicy_ = RSGlobalConfig.requestConfigParams.oomPolicy;
    RedisModule_GetUsedMemoryRatioEstimate = FakeGetUsedMemoryRatioEstimate;
    RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Return;
    fakeRatio = 0;
    fakeRatioReads = 0;

    TimeoutConfig timeoutConfig = {0, TimeoutPolicy_Return};
    QueryRequestLimits_Init(&timeout_, &timeoutConfig);
  }

  void TearDown() override {
    RedisModule_GetUsedMemoryRatioEstimate = previousApi_;
    RSGlobalConfig.requestConfigParams.oomPolicy = previousPolicy_;
    QueryMemoryDebug_SetRatioReadInterval(0);
  }

  QueryRequestLimits timeout_ = {};

 private:
  decltype(RedisModule_GetUsedMemoryRatioEstimate) previousApi_;
  RSOomPolicy previousPolicy_;
};

TEST_F(QueryMemoryTest, ReadsMemoryOncePerInterval) {
  ASSERT_EQ(QueryMemory_GetRatioReadInterval(), QUERY_MEMORY_RATIO_READ_INTERVAL);
  uint32_t counter = 0;
  for (int round = 1; round <= 3; ++round) {
    for (uint32_t i = 0; i < QUERY_MEMORY_RATIO_READ_INTERVAL; ++i) {
      EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_, &counter));
    }
    EXPECT_EQ(fakeRatioReads.load(), (unsigned)round);
  }
}

TEST_F(QueryMemoryTest, PressureRequestsAMemoryStop) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  uint32_t counter = 0;
  fakeRatio = 0.99f;
  EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_, &counter));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  fakeRatio = 1.0f;
  EXPECT_TRUE(QueryMemory_Checkpoint(&timeout_, &counter));
  // The stop is what every stop-check site of this request now observes.
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  // Sticky without reading memory again, even if pressure is gone.
  fakeRatio = 0;
  const unsigned reads = fakeRatioReads.load();
  EXPECT_TRUE(QueryMemory_Checkpoint(&timeout_, &counter));
  EXPECT_EQ(fakeRatioReads.load(), reads);
}

TEST_F(QueryMemoryTest, IgnoresTheBlockedClientTimeout) {
  uint32_t counter = 0;
  QueryRequestLimits_BeginCycle(&timeout_, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestLimits_MarkTimedOut(&timeout_);
  EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_, &counter));
  EXPECT_FALSE(QueryMemory_CheckpointNow(&timeout_));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, OffWhenOnOomIsIgnore) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
  fakeRatio = 5.0f;
  uint32_t counter = 0;
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_, &counter));
  }
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, OffWithoutTheMemoryApi) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  RedisModule_GetUsedMemoryRatioEstimate = nullptr;
  uint32_t counter = 0;
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_, &counter));
  }
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, OffWithoutAMemoryLimit) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  fakeRatio = 0;  // The API's answer when no limit is configured.
  uint32_t counter = 0;
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_, &counter));
  }
  EXPECT_EQ(fakeRatioReads.load(), 10u);
}

TEST_F(QueryMemoryTest, WithoutARequestReportsPressureOnly) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  fakeRatio = 1.5f;
  uint32_t counter = 0;
  EXPECT_TRUE(QueryMemory_Checkpoint(nullptr, &counter));
}

TEST_F(QueryMemoryTest, SitesBelowTheIntervalShareTheRequestCount) {
  QueryMemoryDebug_SetRatioReadInterval(8);
  fakeRatio = 1.0f;
  uint32_t siteA = 0, siteB = 0;
  // Each site alone stays under the interval; together the eighth call reads.
  for (int i = 0; i < 4; ++i) {
    EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_, &siteA));
  }
  for (int i = 0; i < 3; ++i) {
    EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_, &siteB));
  }
  EXPECT_EQ(fakeRatioReads.load(), 0u);
  EXPECT_TRUE(QueryMemory_Checkpoint(&timeout_, &siteB));
  EXPECT_EQ(fakeRatioReads.load(), 1u);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));
  EXPECT_EQ(timeout_.memory.counter, 0u);
  EXPECT_EQ(siteB, 0u);
}

TEST_F(QueryMemoryTest, APrimedCounterStillReadsImmediately) {
  QueryMemoryDebug_SetRatioReadInterval(8);
  fakeRatio = 1.0f;
  uint32_t counter = 7;
  EXPECT_TRUE(QueryMemory_Checkpoint(&timeout_, &counter));
  EXPECT_EQ(fakeRatioReads.load(), 1u);
  EXPECT_EQ(counter, 0u);
  EXPECT_EQ(timeout_.memory.counter, 0u);
}

TEST_F(QueryMemoryTest, WithoutARequestEachSiteCountsAlone) {
  QueryMemoryDebug_SetRatioReadInterval(4);
  uint32_t siteA = 0, siteB = 0;
  for (int i = 0; i < 3; ++i) {
    EXPECT_FALSE(QueryMemory_Checkpoint(nullptr, &siteA));
    EXPECT_FALSE(QueryMemory_Checkpoint(nullptr, &siteB));
  }
  EXPECT_EQ(fakeRatioReads.load(), 0u);
  EXPECT_FALSE(QueryMemory_Checkpoint(nullptr, &siteA));
  EXPECT_EQ(fakeRatioReads.load(), 1u);
}

TEST_F(QueryMemoryTest, ShouldStopNowReadsWithoutTouchingCounters) {
  timeout_.memory.counter = 5;
  fakeRatio = 0.5f;
  EXPECT_FALSE(QueryMemory_CheckpointNow(&timeout_));
  EXPECT_EQ(fakeRatioReads.load(), 1u);
  EXPECT_EQ(timeout_.memory.counter, 5u);

  fakeRatio = 1.0f;
  EXPECT_TRUE(QueryMemory_CheckpointNow(&timeout_));
  EXPECT_EQ(fakeRatioReads.load(), 2u);
  EXPECT_EQ(timeout_.memory.counter, 5u);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  // An already requested stop is reported without another read.
  EXPECT_TRUE(QueryMemory_CheckpointNow(&timeout_));
  EXPECT_EQ(fakeRatioReads.load(), 2u);
}

TEST_F(QueryMemoryTest, ConcurrentCheckpointsStopTheSameRequest) {
  fakeRatio = 1.0f;
  std::vector<std::thread> checkpoints;
  std::atomic<int> stopped = 0;
  for (int t = 0; t < 4; ++t) {
    checkpoints.emplace_back([&] {
      if (QueryMemory_CheckpointNow(&timeout_)) {
        stopped.fetch_add(1);
      }
    });
  }
  for (auto &checkpoint : checkpoints) {
    checkpoint.join();
  }
  EXPECT_EQ(stopped.load(), 4);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

}  // namespace
