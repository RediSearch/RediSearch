/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "common.h"
#include "index_utils.h"
#include "spec.h"
#include "util/module_timer.h"
#include "util/references.h"

#include <vector>

namespace {

// The mock has no event loop, so timers are recorded here and fired by hand.
struct ArmedTimer {
  mstime_t delay;
  RedisModuleTimerProc callback;
  void *data;
  bool pending;
};
std::vector<ArmedTimer> armed;

RedisModuleTimerID recordTimer(RedisModuleCtx *, mstime_t delay, RedisModuleTimerProc callback,
                               void *data) {
  armed.push_back({delay, callback, data, true});
  return armed.size();  // ids start at 1, so 0 stays "no timer"
}

int stopTimer(RedisModuleCtx *, RedisModuleTimerID id, void **data) {
  if (id == 0 || id > armed.size() || !armed[id - 1].pending) {
    return REDISMODULE_ERR;
  }
  armed[id - 1].pending = false;
  if (data) {
    *data = armed[id - 1].data;
  }
  return REDISMODULE_OK;
}

void keepObject(void *) {
}

}  // namespace

class TemporaryTimerTest : public ::testing::Test {
 protected:
  RMCK::Context ctx;
  RefManager *ism = nullptr;
  const decltype(RedisModule_CreateTimer) savedCreateTimer = RedisModule_CreateTimer;
  const decltype(RedisModule_StopTimer) savedStopTimer = RedisModule_StopTimer;

  // Any CreateTimer turns off RS_IsMock, so the fake is swapped in only around the TEMPORARY timer:
  // the spec is made and dropped the mock way.
  void SetUp() override {
    armed.clear();
    ism = createSpec(ctx);
    RedisModule_CreateTimer = recordTimer;
    RedisModule_StopTimer = stopTimer;
  }

  void TearDown() override {
    RedisModule_CreateTimer = savedCreateTimer;
    freeSpec(ism);  // stops the pending timer, which releases its reference
    RedisModule_StopTimer = savedStopTimer;
  }

  IndexSpec *spec() {
    return get_spec(ism);
  }

  void armFor(long long timeoutMs) {
    spec()->timeout = timeoutMs;
    IndexSpec_SetTimeoutTimer(spec(), StrongRef_Demote({ism}));
  }

  void fire(size_t i) {
    ASSERT_TRUE(armed[i].pending);
    armed[i].pending = false;
    armed[i].callback(ctx, armed[i].data);
  }
};

TEST_F(TemporaryTimerTest, testFiresEarlyAndReArmsUntilTheDeadline) {
  armFor(2 * RS_MAX_TIMER_DELAY_MS + 1);
  ASSERT_EQ(armed.size(), (size_t)1);
  EXPECT_EQ(armed[0].delay, RS_MAX_TIMER_DELAY_MS);

  fire(0);
  ASSERT_EQ(armed.size(), (size_t)2) << "a timer that fired before the deadline must re-arm";
  EXPECT_EQ(armed[1].delay, RS_MAX_TIMER_DELAY_MS);
  EXPECT_EQ(spec()->timerId, (RedisModuleTimerID)2);
  EXPECT_TRUE(spec()->isTimerSet);
  // What is left counts to the deadline, past what the timer covers.
  EXPECT_GT(IndexSpec_TimeoutRemainingMs(spec()), (uint64_t)(2 * RS_MAX_TIMER_DELAY_MS - 60000));

  // As if a cap and a minute had passed: what is left of the deadline is now under the cap.
  spec()->timeoutDeadlineMs -= RS_MAX_TIMER_DELAY_MS + 60000;
  fire(1);
  ASSERT_EQ(armed.size(), (size_t)3);
  EXPECT_GT(armed[2].delay, RS_MAX_TIMER_DELAY_MS - 120000);
  EXPECT_LT(armed[2].delay, RS_MAX_TIMER_DELAY_MS);
  EXPECT_EQ(spec()->timerId, (RedisModuleTimerID)3);
}

TEST_F(TemporaryTimerTest, testTimerOfADroppedIndexDoesNothing) {
  armFor(1000);
  ASSERT_EQ(armed.size(), (size_t)1);
  const RedisModuleTimerProc callback = armed[0].callback;

  // A reference that outlived its object, as after a drop.
  StrongRef strong = StrongRef_New(spec(), keepObject);
  WeakRef dropped = StrongRef_Demote(strong);
  StrongRef_Invalidate(strong);
  StrongRef_Release(strong);

  callback(ctx, dropped.rm);  // releases `dropped`
  EXPECT_EQ(armed.size(), (size_t)1) << "nothing to re-arm for";
}

TEST_F(TemporaryTimerTest, testNegativeTimeoutExpiresAtOnce) {
  armFor(-1000);
  ASSERT_EQ(armed.size(), (size_t)1);
  EXPECT_EQ(armed[0].delay, 0);
  EXPECT_EQ(IndexSpec_TimeoutRemainingMs(spec()), (uint64_t)0);
}
