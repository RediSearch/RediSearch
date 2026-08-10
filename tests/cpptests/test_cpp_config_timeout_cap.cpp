/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "config.h"
#include "query_error_ffi.h"

#include <cstring>

class TimeoutCapTest : public ::testing::Test {
 protected:
  // Save/restore the fields we mutate so tests do not bleed into each other.
  long long savedMaxForegroundTimeoutLimitMS;
  size_t savedNumWorkerThreads;

  void SetUp() override {
    savedMaxForegroundTimeoutLimitMS = RSGlobalConfig.maxForegroundTimeoutLimitMS;
    savedNumWorkerThreads = RSGlobalConfig.numWorkerThreads;
  }

  void TearDown() override {
    RSGlobalConfig.maxForegroundTimeoutLimitMS = savedMaxForegroundTimeoutLimitMS;
    RSGlobalConfig.numWorkerThreads = savedNumWorkerThreads;
  }
};

TEST_F(TimeoutCapTest, NullPointer) {
  RSGlobalConfig.maxForegroundTimeoutLimitMS = 1000;
  RSGlobalConfig.numWorkerThreads = 0;
  ASSERT_FALSE(RSConfig_CapQueryTimeoutToForegroundLimit(nullptr));
}

TEST_F(TimeoutCapTest, LimitDisabledByZero) {
  // limit == 0 means "unlimited": never cap.
  RSGlobalConfig.maxForegroundTimeoutLimitMS = 0;
  RSGlobalConfig.numWorkerThreads = 0;
  long long t = 1'000'000;
  ASSERT_FALSE(RSConfig_CapQueryTimeoutToForegroundLimit(&t));
  ASSERT_EQ(t, 1'000'000);
}

TEST_F(TimeoutCapTest, LimitDisabledByWorkersEnabled) {
  // numWorkerThreads != 0 means workers are enabled, the limit does not apply.
  RSGlobalConfig.maxForegroundTimeoutLimitMS = 1000;
  RSGlobalConfig.numWorkerThreads = 4;
  long long t = 60'000;
  ASSERT_FALSE(RSConfig_CapQueryTimeoutToForegroundLimit(&t));
  ASSERT_EQ(t, 60'000);
}

TEST_F(TimeoutCapTest, WithinBudgetNoCap) {
  RSGlobalConfig.maxForegroundTimeoutLimitMS = 1000;
  RSGlobalConfig.numWorkerThreads = 0;
  long long t = 500;
  ASSERT_FALSE(RSConfig_CapQueryTimeoutToForegroundLimit(&t));
  ASSERT_EQ(t, 500);
}

TEST_F(TimeoutCapTest, EqualToLimitNoCap) {
  // Boundary: equal to the limit is allowed, capping is strict-greater-than.
  RSGlobalConfig.maxForegroundTimeoutLimitMS = 1000;
  RSGlobalConfig.numWorkerThreads = 0;
  long long t = 1000;
  ASSERT_FALSE(RSConfig_CapQueryTimeoutToForegroundLimit(&t));
  ASSERT_EQ(t, 1000);
}

TEST_F(TimeoutCapTest, CapAppliedWhenAboveLimit) {
  RSGlobalConfig.maxForegroundTimeoutLimitMS = 1000;
  RSGlobalConfig.numWorkerThreads = 0;
  long long t = 60'000;
  ASSERT_TRUE(RSConfig_CapQueryTimeoutToForegroundLimit(&t));
  ASSERT_EQ(t, 1000);
}

TEST_F(TimeoutCapTest, UnlimitedQueryTimeoutCappedToLimit) {
  // TIMEOUT 0 (unlimited) is semantically above any configured maximum, so
  // when the cap is active the helper clamps it to the configured limit.
  RSGlobalConfig.maxForegroundTimeoutLimitMS = 1000;
  RSGlobalConfig.numWorkerThreads = 0;
  long long t = 0;
  ASSERT_TRUE(RSConfig_CapQueryTimeoutToForegroundLimit(&t));
  ASSERT_EQ(t, 1000);
}

TEST_F(TimeoutCapTest, NegativeTimeoutCappedToLimit) {
  // A negative value (e.g. from a size_t that wrapped to a negative long long)
  // is also semantically above the limit and must be capped, not silently
  // treated as "unlimited".
  RSGlobalConfig.maxForegroundTimeoutLimitMS = 1000;
  RSGlobalConfig.numWorkerThreads = 0;
  long long t = -1;
  ASSERT_TRUE(RSConfig_CapQueryTimeoutToForegroundLimit(&t));
  ASSERT_EQ(t, 1000);
}

TEST_F(TimeoutCapTest, MaxTimeoutCappedWarningString) {
  // Sanity check the new warning code is wired through QueryWarning_Strwarning
  // and surfaces a meaningful message rather than the "unknown" sentinel.
  const char *msg = QueryWarning_Strwarning(QUERY_WARNING_CODE_MAX_TIMEOUT_CAPPED);
  ASSERT_STRNE(msg, "Unknown warning code");
  ASSERT_NE(strstr(msg, "search-_max-foreground-timeout-limit"), nullptr);
}
