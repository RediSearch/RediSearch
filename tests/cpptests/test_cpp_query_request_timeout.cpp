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
#include <chrono>
#include <thread>

#include "query_request.h"
#include "redismodule.h"

namespace {

RedisModuleTimerID FakeCreateTimer(RedisModuleCtx *, mstime_t, RedisModuleTimerProc, void *) {
  return 0;
}

class ScopedRealClockChecks {
 public:
  ScopedRealClockChecks() : previous_(RedisModule_CreateTimer) {
    RedisModule_CreateTimer = FakeCreateTimer;
  }

  ~ScopedRealClockChecks() {
    RedisModule_CreateTimer = previous_;
  }

 private:
  decltype(RedisModule_CreateTimer) previous_;
};

class QueryRequestTimeoutTest : public ::testing::Test {};

TEST_F(QueryRequestTimeoutTest, InitializationIsUnarmedAndRetainsConfiguration) {
  QueryRequestTimeout timeout = {};

  QueryRequestTimeout_Init(&timeout, TimeoutPolicy_Fail, 1234);

  EXPECT_EQ(timeout.policy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.timeoutMS, 1234);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOut(&timeout));
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutWithCounter(&timeout));
}

TEST_F(QueryRequestTimeoutTest, ConfigUpdateIsStickyAndDoesNotChangeActiveCycle) {
  QueryRequestTimeout timeout = {};
  QueryRequestTimeout_Init(&timeout, TimeoutPolicy_Return, 100);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestTimeout_MarkTimedOut(&timeout);

  QueryRequestTimeout_UpdateConfig(&timeout, TimeoutPolicy_Fail, 250);

  EXPECT_EQ(timeout.policy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.timeoutMS, 250);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_TRUE(QueryRequestTimeout_IsTimedOut(&timeout));

  QueryRequestTimeout_Reset(&timeout);
  EXPECT_EQ(timeout.policy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.timeoutMS, 250);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.policy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.timeoutMS, 250);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
}

TEST_F(QueryRequestTimeoutTest, ResetAndRearmClearCycleState) {
  QueryRequestTimeout timeout = {};
  QueryRequestTimeout_Init(&timeout, TimeoutPolicy_Return, 1000);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestTimeout_MarkTimedOut(&timeout);

  QueryRequestTimeout_Reset(&timeout);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOut(&timeout));

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOut(&timeout));

  QueryRequestTimeout_Reset(&timeout);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.source.clock.counter, 0);
}

TEST_F(QueryRequestTimeoutTest, MarkingPublishesOnlyTheBlockedClientSource) {
  QueryRequestTimeout timeout = {};
  QueryRequestTimeout_Init(&timeout, TimeoutPolicy_ReturnStrict, 100);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);

  EXPECT_FALSE(QueryRequestTimeout_IsTimedOut(&timeout));
  EXPECT_FALSE(QueryRequestTimeout_IsBlockedClientTimedOut(&timeout));

  QueryRequestTimeout_MarkTimedOut(&timeout);

  EXPECT_TRUE(QueryRequestTimeout_IsTimedOut(&timeout));
  EXPECT_TRUE(QueryRequestTimeout_IsBlockedClientTimedOut(&timeout));
  EXPECT_TRUE(QueryRequestTimeout_IsTimedOutWithCounter(&timeout));
}

TEST_F(QueryRequestTimeoutTest, CounterChecksClockAtTheConfiguredCadence) {
  ScopedRealClockChecks enableClockChecks;
  QueryRequestTimeout timeout = {};
  QueryRequestTimeout_Init(&timeout, TimeoutPolicy_Return, 1000);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  *QueryRequestTimeout_GetClockDeadlineForUpdate(&timeout) = {0, 0};

  ASSERT_TRUE(QueryRequestTimeout_IsTimedOut(&timeout));
  for (uint32_t i = 1; i < QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT; ++i) {
    EXPECT_FALSE(QueryRequestTimeout_IsTimedOutWithCounter(&timeout));
    EXPECT_EQ(timeout.source.clock.counter, i);
  }

  EXPECT_TRUE(QueryRequestTimeout_IsTimedOutWithCounter(&timeout));
  EXPECT_EQ(timeout.source.clock.counter, 0);
}

TEST_F(QueryRequestTimeoutTest, MainThreadMarkIsObservedByWorker) {
  QueryRequestTimeout timeout = {};
  QueryRequestTimeout_Init(&timeout, TimeoutPolicy_ReturnStrict, 1000);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);

  std::atomic<bool> workerReady = false;
  bool workerObservedTimeout = false;
  std::thread worker([&] {
    workerReady.store(true, std::memory_order_release);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
      if (QueryRequestTimeout_IsTimedOut(&timeout)) {
        workerObservedTimeout = true;
        return;
      }
      std::this_thread::yield();
    }
  });

  while (!workerReady.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  QueryRequestTimeout_MarkTimedOut(&timeout);
  worker.join();

  EXPECT_TRUE(workerObservedTimeout);
}

}  // namespace
