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

  RequestConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Fail;
  timeoutConfig.queryTimeoutMS = 1234;
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);

  EXPECT_EQ(timeout.config->timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.config->queryTimeoutMS, 1234);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOut(&timeout));
}

TEST_F(QueryRequestTimeoutTest, ConfigUpdateIsStickyAndDoesNotChangeActiveCycle) {
  QueryRequestTimeout timeout = {};
  RequestConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
  timeoutConfig.queryTimeoutMS = 100;
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestTimeout_MarkTimedOut(&timeout);

  timeoutConfig.timeoutPolicy = TimeoutPolicy_Fail;
  timeoutConfig.queryTimeoutMS = 250;

  EXPECT_EQ(timeout.config->timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.config->queryTimeoutMS, 250);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_TRUE(QueryRequestTimeout_IsTimedOutExact(&timeout));

  QueryRequestTimeout_Reset(&timeout);
  EXPECT_EQ(timeout.config->timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.config->queryTimeoutMS, 250);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.config->timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.config->queryTimeoutMS, 250);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
}

TEST_F(QueryRequestTimeoutTest, ResetAndRearmClearCycleState) {
  QueryRequestTimeout timeout = {};
  RequestConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
  timeoutConfig.queryTimeoutMS = 1000;
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestTimeout_MarkTimedOut(&timeout);

  QueryRequestTimeout_Reset(&timeout);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));

  QueryRequestTimeout_Reset(&timeout);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.source.clock.counter, 0);
}

TEST_F(QueryRequestTimeoutTest, MarkingPublishesOnlyTheBlockedClientSource) {
  QueryRequestTimeout timeout = {};
  RequestConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  timeoutConfig.queryTimeoutMS = 100;
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);

  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  EXPECT_FALSE(QueryRequestTimeout_IsBlockedClientTimedOut(&timeout));

  QueryRequestTimeout_MarkTimedOut(&timeout);

  EXPECT_TRUE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  EXPECT_TRUE(QueryRequestTimeout_IsBlockedClientTimedOut(&timeout));
  EXPECT_TRUE(QueryRequestTimeout_IsTimedOut(&timeout));
}

TEST_F(QueryRequestTimeoutTest, PrimaryOperationAmortizesClockChecks) {
  ScopedRealClockChecks enableClockChecks;
  QueryRequestTimeout timeout = {};
  RequestConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
  timeoutConfig.queryTimeoutMS = 1000;
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  *QueryRequestTimeout_GetClockDeadlineForUpdate(&timeout) = {0, 0};

  ASSERT_TRUE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  for (uint32_t i = 1; i < QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT; ++i) {
    EXPECT_FALSE(QueryRequestTimeout_IsTimedOut(&timeout));
    EXPECT_EQ(timeout.source.clock.counter, i);
  }

  EXPECT_TRUE(QueryRequestTimeout_IsTimedOut(&timeout));
  EXPECT_EQ(timeout.source.clock.counter, 0);
}

TEST_F(QueryRequestTimeoutTest, MainThreadMarkIsObservedByWorker) {
  QueryRequestTimeout timeout = {};
  RequestConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  timeoutConfig.queryTimeoutMS = 1000;
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
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

TEST_F(QueryRequestTimeoutTest, RequestOwnsConfigurationAcrossCycles) {
  for (auto kind :
       {QUERY_REQUEST_KIND_AREQ, QUERY_REQUEST_KIND_HYBRID, QUERY_REQUEST_KIND_COORD_SEARCH}) {
    RequestConfig defaults = {};
    defaults.dialectVersion = 4;
    defaults.queryTimeoutMS = 1234;
    defaults.timeoutPolicy = TimeoutPolicy_Fail;
    defaults.printProfileClock = true;
    defaults.BM25STD_TanhFactor = 7;
    defaults.oomPolicy = OomPolicy_Fail;
    QueryRequest request = {};
    QueryRequest_Init(&request, kind, &defaults, nullptr, 0);

    defaults = {};
    EXPECT_EQ(request.timeout.config, &request.reqConfig);
    EXPECT_EQ(request.reqConfig.dialectVersion, 4);
    EXPECT_EQ(request.reqConfig.queryTimeoutMS, 1234);
    EXPECT_EQ(request.reqConfig.timeoutPolicy, TimeoutPolicy_Fail);
    EXPECT_TRUE(request.reqConfig.printProfileClock);
    EXPECT_EQ(request.reqConfig.BM25STD_TanhFactor, 7);
    EXPECT_EQ(request.reqConfig.oomPolicy, OomPolicy_Fail);

    request.reqConfig.queryTimeoutMS = 0;
    request.reqConfig.timeoutPolicy = TimeoutPolicy_Return;
    QueryRequestTimeout_Reset(&request.timeout);
    QueryRequest_ResetReply(&request);
    QueryRequestTimeout_BeginCycle(&request.timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    EXPECT_EQ(request.timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
    EXPECT_EQ(request.reqConfig.dialectVersion, 4);
    EXPECT_EQ(request.reqConfig.BM25STD_TanhFactor, 7);
    EXPECT_EQ(request.reqConfig.oomPolicy, OomPolicy_Fail);

    request.reqConfig.queryTimeoutMS = 5000;
    QueryRequestTimeout_BeginCycle(&request.timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    EXPECT_EQ(request.timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    const auto deadline = *QueryRequestTimeout_GetClockDeadline(&request.timeout);
    request.reqConfig.queryTimeoutMS = 0;
    EXPECT_EQ(QueryRequestTimeout_GetClockDeadline(&request.timeout)->tv_sec, deadline.tv_sec);
    EXPECT_EQ(QueryRequestTimeout_GetClockDeadline(&request.timeout)->tv_nsec, deadline.tv_nsec);
    QueryRequestTimeout_Reset(&request.timeout);
    QueryRequestTimeout_BeginCycle(&request.timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    EXPECT_EQ(request.timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
    QueryRequest_Destroy(&request);
  }
}

TEST_F(QueryRequestTimeoutTest, ExecutionPhaseTracksOnlyUnsignaledBlockedClientCycles) {
  RequestConfig config = {};
  config.timeoutPolicy = TimeoutPolicy_Return;
  config.queryTimeoutMS = 1000;
  QueryRequest request = {};
  QueryRequest_Init(&request, QUERY_REQUEST_KIND_AREQ, &config, nullptr, 0);

  constexpr int queuePhase = 0;
  constexpr int pipelinePhase = 1;
  constexpr int replyPhase = 2;

  QueryRequest_SetExecutionPhase(&request, pipelinePhase);
  EXPECT_EQ(QueryRequest_GetExecutionPhase(&request), pipelinePhase);

  QueryRequestTimeout_MarkTimedOut(&request.timeout);
  QueryRequest_SetExecutionPhase(&request, replyPhase);
  EXPECT_EQ(QueryRequest_GetExecutionPhase(&request), pipelinePhase);

  QueryRequestTimeout_BeginCycle(&request.timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  QueryRequest_SetExecutionPhase(&request, queuePhase);
  EXPECT_EQ(QueryRequest_GetExecutionPhase(&request), pipelinePhase);

  QueryRequestTimeout_Reset(&request.timeout);
  QueryRequest_SetExecutionPhase(&request, replyPhase);
  EXPECT_EQ(QueryRequest_GetExecutionPhase(&request), pipelinePhase);

  QueryRequest_Destroy(&request);
}

}  // namespace
