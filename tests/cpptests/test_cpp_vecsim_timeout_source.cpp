/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"

#include "query_request.h"
#include "redismodule.h"
#include "util/timeout.h"

namespace {

RedisModuleTimerID nonNullTimerStub(RedisModuleCtx *, mstime_t, RedisModuleTimerProc, void *) {
  return 0;
}

class NonMockTimeoutChecks {
 public:
  NonMockTimeoutChecks() : saved_(RedisModule_CreateTimer) {
    // VecSim_TimedOut deliberately skips clock access in the unit-test mock. A non-null timer
    // hook enables the real adapter for this isolated test; nothing calls the stub itself.
    RedisModule_CreateTimer = nonNullTimerStub;
  }

  ~NonMockTimeoutChecks() {
    RedisModule_CreateTimer = saved_;
  }

 private:
  decltype(RedisModule_CreateTimer) saved_;
};

TEST(VecSimTimeoutSourceTest, RetainedContextFollowsSourceChangesBetweenCursorCycles) {
  NonMockTimeoutChecks enableTimeoutChecks;
  QueryRequestTimeout timeout = {};
  QueryRequestTimeout_Init(&timeout, TimeoutPolicy_Fail, 60'000);

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  for (size_t i = 0; i < QUERY_REQUEST_TIMEOUT_COUNTER_LIMIT; ++i) {
    EXPECT_EQ(NOT_TIMED_OUT, VecSim_TimedOut(&timeout));
  }

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  EXPECT_EQ(TIMED_OUT, VecSim_TimedOut(&timeout));

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
  EXPECT_EQ(NOT_TIMED_OUT, VecSim_TimedOut(&timeout));

  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  EXPECT_EQ(NOT_TIMED_OUT, VecSim_TimedOut(&timeout));
}

}  // namespace
