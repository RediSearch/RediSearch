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

class QueryRequestLimitsTest : public ::testing::Test {};

TEST_F(QueryRequestLimitsTest, InitializationIsUnarmedAndRetainsConfiguration) {
  QueryRequestLimits timeout = {};

  TimeoutConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Fail;
  timeoutConfig.queryTimeoutMS = 1234;
  QueryRequestLimits_Init(&timeout, &timeoutConfig);

  timeoutConfig = {};

  EXPECT_EQ(timeout.timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.timeout.config.queryTimeoutMS, 1234);
  EXPECT_EQ(timeout.timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
  EXPECT_FALSE(QueryRequestLimits_IsTimedOutExact(&timeout));
  EXPECT_FALSE(QueryRequestLimits_IsTimedOut(&timeout));
}

TEST_F(QueryRequestLimitsTest, ConfigUpdateIsStickyAndDoesNotChangeActiveCycle) {
  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
  timeoutConfig.queryTimeoutMS = 100;
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestLimits_MarkTimedOut(&timeout);

  timeout.timeout.config.timeoutPolicy = TimeoutPolicy_Fail;
  timeout.timeout.config.queryTimeoutMS = 250;

  EXPECT_EQ(timeout.timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.timeout.config.queryTimeoutMS, 250);
  EXPECT_EQ(timeout.timeout.kind, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_TRUE(QueryRequestLimits_IsTimedOutExact(&timeout));

  QueryRequestLimits_Reset(&timeout);
  EXPECT_EQ(timeout.timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.timeout.config.queryTimeoutMS, 250);
  EXPECT_EQ(timeout.timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);

  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
  EXPECT_EQ(timeout.timeout.config.queryTimeoutMS, 250);
  EXPECT_EQ(timeout.timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
}

TEST_F(QueryRequestLimitsTest, ResetAndRearmClearCycleState) {
  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
  timeoutConfig.queryTimeoutMS = 1000;
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestLimits_MarkTimedOut(&timeout);

  QueryRequestLimits_Reset(&timeout);
  EXPECT_EQ(timeout.timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
  EXPECT_FALSE(QueryRequestLimits_IsTimedOutExact(&timeout));

  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_EQ(timeout.timeout.kind, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_FALSE(QueryRequestLimits_IsTimedOutExact(&timeout));

  QueryRequestLimits_Reset(&timeout);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(timeout.timeout.source.clock.counter, 0);
}

TEST_F(QueryRequestLimitsTest, MarkingPublishesOnlyTheBlockedClientSource) {
  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  timeoutConfig.queryTimeoutMS = 100;
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);

  EXPECT_FALSE(QueryRequestLimits_IsTimedOutExact(&timeout));
  EXPECT_FALSE(QueryRequestLimits_IsBlockedClientTimedOut(&timeout));

  QueryRequestLimits_MarkTimedOut(&timeout);

  EXPECT_TRUE(QueryRequestLimits_IsTimedOutExact(&timeout));
  EXPECT_TRUE(QueryRequestLimits_IsBlockedClientTimedOut(&timeout));
  EXPECT_TRUE(QueryRequestLimits_IsTimedOut(&timeout));
}

TEST_F(QueryRequestLimitsTest, PrimaryOperationAmortizesClockChecks) {
  ScopedRealClockChecks enableClockChecks;
  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
  timeoutConfig.queryTimeoutMS = 1000;
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  *QueryRequestLimits_GetClockDeadlineForUpdate(&timeout) = {0, 0};

  ASSERT_TRUE(QueryRequestLimits_IsTimedOutExact(&timeout));
  for (uint32_t i = 1; i < QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT; ++i) {
    EXPECT_FALSE(QueryRequestLimits_IsTimedOut(&timeout));
    EXPECT_EQ(timeout.timeout.source.clock.counter, i);
  }

  EXPECT_TRUE(QueryRequestLimits_IsTimedOut(&timeout));
  EXPECT_EQ(timeout.timeout.source.clock.counter, 0);
}

TEST_F(QueryRequestLimitsTest, MainThreadMarkIsObservedByWorker) {
  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {};
  timeoutConfig.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  timeoutConfig.queryTimeoutMS = 1000;
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);

  std::atomic<bool> workerReady = false;
  bool workerObservedTimeout = false;
  std::thread worker([&] {
    workerReady.store(true, std::memory_order_release);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
      if (QueryRequestLimits_IsTimedOut(&timeout)) {
        workerObservedTimeout = true;
        return;
      }
      std::this_thread::yield();
    }
  });

  while (!workerReady.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  QueryRequestLimits_MarkTimedOut(&timeout);
  worker.join();

  EXPECT_TRUE(workerObservedTimeout);
}

TEST_F(QueryRequestLimitsTest, MemoryStopIsNotATimeoutInEveryKind) {
  ScopedRealClockChecks enableClockChecks;
  for (auto kind : {QUERY_REQUEST_TIMEOUT_UNARMED, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT,
                    QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE}) {
    QueryRequestLimits timeout = {};
    TimeoutConfig timeoutConfig = {};
    timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
    timeoutConfig.queryTimeoutMS = 60000;
    QueryRequestLimits_Init(&timeout, &timeoutConfig);
    if (kind != QUERY_REQUEST_TIMEOUT_UNARMED) {
      QueryRequestLimits_BeginCycle(&timeout, kind);
    }
    ASSERT_EQ(timeout.timeout.kind, kind);
    EXPECT_FALSE(QueryRequestLimits_IsTimedOut(&timeout));
    EXPECT_FALSE(QueryRequestLimits_IsTimedOutExact(&timeout));
    EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout));

    struct timespec deadline = {};
    if (kind == QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
      deadline = *QueryRequestLimits_GetClockDeadline(&timeout);
    }

    QueryRequestLimits_RequestMemoryStop(&timeout);

    EXPECT_FALSE(QueryRequestLimits_IsTimedOut(&timeout));
    EXPECT_FALSE(QueryRequestLimits_IsTimedOutExact(&timeout));
    EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout));
    EXPECT_TRUE(RS_AtomicBoolLoadRelaxed(QueryRequestLimits_GetMemoryStopFlag(&timeout)));
    // Ownership checks must not mistake a memory stop for a fired timeout callback.
    EXPECT_FALSE(QueryRequestLimits_IsBlockedClientTimedOut(&timeout));
    EXPECT_EQ(timeout.timeout.kind, kind);
    if (kind == QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE) {
      // The memory stop lives outside the source union.
      EXPECT_EQ(QueryRequestLimits_GetClockDeadline(&timeout)->tv_sec, deadline.tv_sec);
      EXPECT_EQ(QueryRequestLimits_GetClockDeadline(&timeout)->tv_nsec, deadline.tv_nsec);
    }
  }
}

TEST_F(QueryRequestLimitsTest, MarkingTheBlockedClientDoesNotSetTheMemoryStop) {
  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Fail};
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);

  QueryRequestLimits_MarkTimedOut(&timeout);
  EXPECT_TRUE(QueryRequestLimits_IsTimedOutExact(&timeout));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout));
}

TEST_F(QueryRequestLimitsTest, MemoryStopSurvivesResetAndBeginCycle) {
  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Return};
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  QueryRequestLimits_RequestMemoryStop(&timeout);

  QueryRequestLimits_Reset(&timeout);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout));

  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout));

  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout));
}

TEST_F(QueryRequestLimitsTest, ShouldStopCombinesTheMemoryStopAndTheBlockedClientFlag) {
  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Fail};
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  EXPECT_FALSE(QueryRequestLimits_ShouldStop(&timeout));

  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_FALSE(QueryRequestLimits_ShouldStop(&timeout));
  QueryRequestLimits_MarkTimedOut(&timeout);
  EXPECT_TRUE(QueryRequestLimits_ShouldStop(&timeout));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout));

  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_FALSE(QueryRequestLimits_ShouldStop(&timeout));
  QueryRequestLimits_RequestMemoryStop(&timeout);
  EXPECT_TRUE(QueryRequestLimits_ShouldStop(&timeout));
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout));
  EXPECT_FALSE(QueryRequestLimits_IsBlockedClientTimedOut(&timeout));
}

TEST_F(QueryRequestLimitsTest, ShouldStopExactReportsEitherInEveryKind) {
  for (auto kind : {QUERY_REQUEST_TIMEOUT_UNARMED, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT,
                    QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE}) {
    QueryRequestLimits timeout = {};
    TimeoutConfig timeoutConfig = {60000, TimeoutPolicy_Return};
    QueryRequestLimits_Init(&timeout, &timeoutConfig);
    if (kind != QUERY_REQUEST_TIMEOUT_UNARMED) {
      QueryRequestLimits_BeginCycle(&timeout, kind);
    }
    ASSERT_EQ(timeout.timeout.kind, kind);
    EXPECT_FALSE(QueryRequestLimits_ShouldStopExact(&timeout)) << kind;
    QueryRequestLimits_RequestMemoryStop(&timeout);
    EXPECT_TRUE(QueryRequestLimits_ShouldStopExact(&timeout)) << kind;
  }

  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Fail};
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestLimits_MarkTimedOut(&timeout);
  EXPECT_TRUE(QueryRequestLimits_ShouldStopExact(&timeout));

  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  *QueryRequestLimits_GetClockDeadlineForUpdate(&timeout) = {0, 0};
  EXPECT_TRUE(QueryRequestLimits_ShouldStopExact(&timeout));
}

TEST_F(QueryRequestLimitsTest, ShouldStopNeverReadsTheClock) {
  ScopedRealClockChecks enableClockChecks;
  QueryRequestLimits timeout = {};
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Return};
  QueryRequestLimits_Init(&timeout, &timeoutConfig);
  QueryRequestLimits_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  *QueryRequestLimits_GetClockDeadlineForUpdate(&timeout) = {0, 0};
  ASSERT_TRUE(QueryRequestLimits_IsTimedOutExact(&timeout));

  EXPECT_FALSE(QueryRequestLimits_ShouldStop(&timeout));
  QueryRequestLimits_RequestMemoryStop(&timeout);
  EXPECT_TRUE(QueryRequestLimits_ShouldStop(&timeout));
}

TEST_F(QueryRequestLimitsTest, RequestOwnsConfigurationAcrossCycles) {
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
    EXPECT_EQ(request.limits.timeout.config.queryTimeoutMS, 1234);
    EXPECT_EQ(request.limits.timeout.config.timeoutPolicy, TimeoutPolicy_Fail);
    EXPECT_TRUE(request.reqConfig.printProfileClock);
    EXPECT_EQ(request.reqConfig.BM25STD_TanhFactor, 7);
    EXPECT_EQ(request.reqConfig.oomPolicy, OomPolicy_Fail);

    request.limits.timeout.config.queryTimeoutMS = 0;
    request.limits.timeout.config.timeoutPolicy = TimeoutPolicy_Return;
    QueryRequestLimits_Reset(&request.limits);
    QueryRequest_ResetReply(&request);
    QueryRequestLimits_BeginCycle(&request.limits, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    EXPECT_EQ(request.limits.timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
    EXPECT_EQ(request.cursorConfig.maxIdle, 1000);
    EXPECT_EQ(request.cursorConfig.chunkSize, 42);
    EXPECT_EQ(request.reqConfig.dialectVersion, 4);
    EXPECT_EQ(request.reqConfig.BM25STD_TanhFactor, 7);
    EXPECT_EQ(request.reqConfig.oomPolicy, OomPolicy_Fail);

    request.limits.timeout.config.queryTimeoutMS = 5000;
    QueryRequestLimits_BeginCycle(&request.limits, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    EXPECT_EQ(request.limits.timeout.kind, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    const auto deadline = *QueryRequestLimits_GetClockDeadline(&request.limits);
    request.limits.timeout.config.queryTimeoutMS = 0;
    EXPECT_EQ(QueryRequestLimits_GetClockDeadline(&request.limits)->tv_sec, deadline.tv_sec);
    EXPECT_EQ(QueryRequestLimits_GetClockDeadline(&request.limits)->tv_nsec, deadline.tv_nsec);
    QueryRequestLimits_Reset(&request.limits);
    QueryRequestLimits_BeginCycle(&request.limits, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    EXPECT_EQ(request.limits.timeout.kind, QUERY_REQUEST_TIMEOUT_UNARMED);
    QueryRequest_Destroy(&request);
  }
}

TEST_F(QueryRequestLimitsTest, ExecutionPhaseTracksOnlyUnsignaledBlockedClientCycles) {
  RequestConfig config = {};
  TimeoutConfig timeoutConfig = {1000, TimeoutPolicy_Return};
  QueryRequest request = {};
  QueryRequest_Init(&request, QUERY_REQUEST_KIND_AREQ, &config, &timeoutConfig, nullptr, 0);

  constexpr int queuePhase = 0;
  constexpr int pipelinePhase = 1;
  constexpr int replyPhase = 2;

  QueryRequest_SetExecutionPhase(&request, pipelinePhase);
  EXPECT_EQ(QueryRequest_GetExecutionPhase(&request), pipelinePhase);

  QueryRequestLimits_MarkTimedOut(&request.limits);
  QueryRequest_SetExecutionPhase(&request, replyPhase);
  EXPECT_EQ(QueryRequest_GetExecutionPhase(&request), pipelinePhase);

  QueryRequestLimits_BeginCycle(&request.limits, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  QueryRequest_SetExecutionPhase(&request, queuePhase);
  EXPECT_EQ(QueryRequest_GetExecutionPhase(&request), pipelinePhase);

  QueryRequestLimits_Reset(&request.limits);
  QueryRequest_SetExecutionPhase(&request, replyPhase);
  EXPECT_EQ(QueryRequest_GetExecutionPhase(&request), pipelinePhase);

  QueryRequest_Destroy(&request);
}

}  // namespace
