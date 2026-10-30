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

  TimeoutConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Fail;
  timeoutConfig.queryTimeoutMS = 1234;
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);

  timeoutConfig = {};

  EXPECT_EQ(timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.config.queryTimeoutMS, 1234);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOut(&timeout));
}

TEST_F(QueryRequestTimeoutTest, ConfigUpdateIsStickyAndDoesNotChangeActiveCycle) {
  QueryRequestTimeout timeout = {};
  TimeoutConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
  timeoutConfig.queryTimeoutMS = 100;
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestTimeout_MarkTimedOut(&timeout);

  timeout.config.timeoutPolicy = TimeoutPolicy_Fail;
  timeout.config.queryTimeoutMS = 250;

  EXPECT_EQ(timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.config.queryTimeoutMS, 250);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_TRUE(QueryRequestTimeout_IsTimedOutExact(&timeout));

  QueryRequestTimeout_Reset(&timeout);
  EXPECT_EQ(timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.config.queryTimeoutMS, 250);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.config.queryTimeoutMS, 250);
  EXPECT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
}

TEST_F(QueryRequestTimeoutTest, ResetAndRearmClearCycleState) {
  QueryRequestTimeout timeout = {};
  TimeoutConfig timeoutConfig = {};
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
  TimeoutConfig timeoutConfig = {};
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
  TimeoutConfig timeoutConfig = {};
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
  TimeoutConfig timeoutConfig = {};
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

TEST_F(QueryRequestTimeoutTest, RequestedStopIsReportedUnamortizedInEveryKind) {
  ScopedRealClockChecks enableClockChecks;
  for (auto kind : {QUERY_REQUEST_TIMEOUT_UNARMED, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT,
                    QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE}) {
    QueryRequestTimeout timeout = {};
    TimeoutConfig timeoutConfig = {};
    timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
    timeoutConfig.queryTimeoutMS = 60000;
    QueryRequestTimeout_Init(&timeout, &timeoutConfig);
    if (kind != QUERY_REQUEST_TIMEOUT_UNARMED) {
      QueryRequestTimeout_BeginCycle(&timeout, kind);
    }
    ASSERT_EQ(timeout.kind, kind);
    EXPECT_FALSE(QueryRequestTimeout_IsTimedOut(&timeout));
    EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));
    EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout), QUERY_STOP_REASON_NONE);

    struct timespec deadline = {};
    uint8_t counter = 0;
    if (kind == QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
      deadline = *QueryRequestTimeout_GetClockDeadline(&timeout);
      counter = timeout.source.clock.counter;
    }

    EXPECT_TRUE(QueryRequestTimeout_RequestStop(&timeout, QUERY_STOP_REASON_OOM));

    EXPECT_TRUE(QueryRequestTimeout_IsTimedOut(&timeout));
    EXPECT_TRUE(QueryRequestTimeout_IsTimedOutExact(&timeout));
    EXPECT_TRUE(RS_AtomicBoolLoadRelaxed(QueryRequestTimeout_GetStopFlag(&timeout)));
    // Ownership checks must not mistake a stop for a fired timeout callback.
    EXPECT_FALSE(QueryRequestTimeout_IsBlockedClientTimedOut(&timeout));
    EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout), QUERY_STOP_REASON_OOM);
    EXPECT_EQ(timeout.kind, kind);
    if (kind == QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
      // The stop lives outside the source union and bypasses the amortization counter.
      EXPECT_EQ(QueryRequestTimeout_GetClockDeadline(&timeout)->tv_sec, deadline.tv_sec);
      EXPECT_EQ(QueryRequestTimeout_GetClockDeadline(&timeout)->tv_nsec, deadline.tv_nsec);
      EXPECT_EQ(timeout.source.clock.counter, counter);
    }
  }
}

TEST_F(QueryRequestTimeoutTest, FirstStopReasonWins) {
  QueryRequestTimeout timeout = {};
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Fail};
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);

  EXPECT_TRUE(QueryRequestTimeout_RequestStop(&timeout, QUERY_STOP_REASON_TIMEOUT));
  EXPECT_FALSE(QueryRequestTimeout_RequestStop(&timeout, QUERY_STOP_REASON_OOM));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout), QUERY_STOP_REASON_TIMEOUT);
}

TEST_F(QueryRequestTimeoutTest, MarkingTheBlockedClientDoesNotRecordAStopReason) {
  QueryRequestTimeout timeout = {};
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Fail};
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);

  QueryRequestTimeout_MarkTimedOut(&timeout);
  EXPECT_TRUE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout), QUERY_STOP_REASON_NONE);
  EXPECT_FALSE(RS_AtomicBoolLoadRelaxed(QueryRequestTimeout_GetStopFlag(&timeout)));
}

TEST_F(QueryRequestTimeoutTest, StopSurvivesResetAndIsClearedByTheNextCycle) {
  QueryRequestTimeout timeout = {};
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Return};
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  ASSERT_TRUE(QueryRequestTimeout_RequestStop(&timeout, QUERY_STOP_REASON_OOM));

  QueryRequestTimeout_Reset(&timeout);
  EXPECT_TRUE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout), QUERY_STOP_REASON_OOM);

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout), QUERY_STOP_REASON_NONE);

  ASSERT_TRUE(QueryRequestTimeout_RequestStop(&timeout, QUERY_STOP_REASON_OOM));
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  EXPECT_EQ(QueryRequestTimeout_GetStopReason(&timeout), QUERY_STOP_REASON_NONE);
}

TEST_F(QueryRequestTimeoutTest, StopRequestedOnAnotherThreadIsObservedWithItsReason) {
  QueryRequestTimeout timeout = {};
  TimeoutConfig timeoutConfig = {0, TimeoutPolicy_Return};
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  // TIMEOUT 0: the cycle stays UNARMED, which on its own never times out.
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  ASSERT_EQ(timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);

  std::atomic<bool> workerReady = false;
  QueryStopReason observedReason = QUERY_STOP_REASON_NONE;
  std::thread worker([&] {
    workerReady.store(true, std::memory_order_release);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
      if (QueryRequestTimeout_IsTimedOut(&timeout)) {
        observedReason = QueryRequestTimeout_GetStopReason(&timeout);
        return;
      }
      std::this_thread::yield();
    }
  });

  while (!workerReady.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  QueryRequestTimeout_RequestStop(&timeout, QUERY_STOP_REASON_OOM);
  worker.join();

  EXPECT_EQ(observedReason, QUERY_STOP_REASON_OOM);
}

TEST_F(QueryRequestTimeoutTest, RequestOwnsConfigurationAcrossCycles) {
  for (auto kind : {QUERY_REQUEST_KIND_AREQ, QUERY_REQUEST_KIND_HYBRID, QUERY_REQUEST_KIND_COORD_SEARCH}) {
    RequestConfig defaults = {};
    defaults.dialectVersion = 4;
    TimeoutConfig timeoutDefaults = {1234, TimeoutPolicy_Fail};
    defaults.printProfileClock = true;
    defaults.BM25STD_TanhFactor = 7;
    defaults.oomPolicy = OomPolicy_Fail;
    QueryRequest request = {};
    QueryRequest_Init(&request, kind, &defaults, &timeoutDefaults, nullptr, 0);

    defaults = {};
    timeoutDefaults = {};
    EXPECT_EQ(request.cursorConfig.maxIdle, 0);
    EXPECT_EQ(request.cursorConfig.chunkSize, 0);
    request.cursorConfig = {1000, 42};
    EXPECT_EQ(request.reqConfig.dialectVersion, 4);
    EXPECT_EQ(request.timeout.config.queryTimeoutMS, 1234);
    EXPECT_EQ(request.timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
    EXPECT_TRUE(request.reqConfig.printProfileClock);
    EXPECT_EQ(request.reqConfig.BM25STD_TanhFactor, 7);
    EXPECT_EQ(request.reqConfig.oomPolicy, OomPolicy_Fail);

    request.timeout.config.queryTimeoutMS = 0;
    request.timeout.config.timeoutPolicy = TimeoutPolicy_Return;
    QueryRequestTimeout_Reset(&request.timeout);
    QueryRequest_ResetReply(&request);
    QueryRequestTimeout_BeginCycle(&request.timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    EXPECT_EQ(request.timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
    EXPECT_EQ(request.cursorConfig.maxIdle, 1000);
    EXPECT_EQ(request.cursorConfig.chunkSize, 42);
    EXPECT_EQ(request.reqConfig.dialectVersion, 4);
    EXPECT_EQ(request.reqConfig.BM25STD_TanhFactor, 7);
    EXPECT_EQ(request.reqConfig.oomPolicy, OomPolicy_Fail);

    request.timeout.config.queryTimeoutMS = 5000;
    QueryRequestTimeout_BeginCycle(&request.timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    EXPECT_EQ(request.timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    const auto deadline = *QueryRequestTimeout_GetClockDeadline(&request.timeout);
    request.timeout.config.queryTimeoutMS = 0;
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
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Return};
  QueryRequest request = {};
  QueryRequest_Init(&request, QUERY_REQUEST_KIND_AREQ, &config, &timeoutConfig, nullptr, 0);

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
