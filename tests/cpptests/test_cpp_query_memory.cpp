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
#include <memory>
#include <thread>
#include <vector>

#include "common.h"
#include "config.h"
#include "iterators_ffi.h"
#include "iterators/vector_top_k.h"
#include "query_error.h"
#include "query_eval_ffi.h"
#include "query_flags.h"
#include "query_memory.h"
#include "query_request.h"
#include "query_test_utils.h"
#include "redismodule.h"
#include "result_processor.h"
#include "tag_index.h"

// aggregate_exec_common.h is not C++-clean; the signature is restated here.
extern "C" int ApplyOomStop(const QueryRequestLimits *limits, RSOomPolicy oomPolicy,
                            QueryError *err, int rc);

extern "C" int (*vecsimTimeoutCallback)(QueryRequestLimits *timeout);

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
    QueryMemory_ResetFakePressure();
  }

  QueryRequestLimits timeout_ = {};

 private:
  decltype(RedisModule_GetUsedMemoryRatioEstimate) previousApi_;
  RSOomPolicy previousPolicy_;
};

TEST_F(QueryMemoryTest, ReadsMemoryOncePerInterval) {
  ASSERT_EQ(QueryMemory_GetRatioReadInterval(), QUERY_MEMORY_RATIO_READ_INTERVAL);
  for (int round = 1; round <= 3; ++round) {
    for (uint32_t i = 0; i < QUERY_MEMORY_RATIO_READ_INTERVAL; ++i) {
      EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
    }
    EXPECT_EQ(fakeRatioReads.load(), (unsigned)round);
    EXPECT_EQ(RS_AtomicUintLoadRelaxed(&timeout_.memory.counter), 0u);
  }
}

TEST_F(QueryMemoryTest, PressureRequestsAMemoryStop) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  fakeRatio = 0.99f;
  EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  fakeRatio = 1.0f;
  EXPECT_TRUE(QueryMemory_Checkpoint(&timeout_));
  // The stop is what every stop-check site of this request now observes.
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  // Sticky without reading memory again, even if pressure is gone.
  fakeRatio = 0;
  const unsigned reads = fakeRatioReads.load();
  EXPECT_TRUE(QueryMemory_Checkpoint(&timeout_));
  EXPECT_EQ(fakeRatioReads.load(), reads);
}

TEST_F(QueryMemoryTest, IgnoresTheBlockedClientTimeout) {
  QueryRequestLimits_BeginCycle(&timeout_, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestLimits_MarkTimedOut(&timeout_);
  EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
  EXPECT_FALSE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, OffWhenOnOomIsIgnore) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
  fakeRatio = 5.0f;
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
  }
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, OffWithoutTheMemoryApi) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  RedisModule_GetUsedMemoryRatioEstimate = nullptr;
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
  }
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, OffWithoutAMemoryLimit) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  fakeRatio = 0;  // The API's answer when no limit is configured.
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
  }
  EXPECT_EQ(fakeRatioReads.load(), 10u);
}

TEST_F(QueryMemoryTest, ShouldStopNowReadsWithoutTouchingCounters) {
  RS_AtomicUintStoreRelaxed(&timeout_.memory.counter, 5);
  fakeRatio = 0.5f;
  EXPECT_FALSE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_EQ(fakeRatioReads.load(), 1u);
  EXPECT_EQ(RS_AtomicUintLoadRelaxed(&timeout_.memory.counter), 5u);

  fakeRatio = 1.0f;
  EXPECT_TRUE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_EQ(fakeRatioReads.load(), 2u);
  EXPECT_EQ(RS_AtomicUintLoadRelaxed(&timeout_.memory.counter), 5u);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  // An already requested stop is reported without another read.
  EXPECT_TRUE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_EQ(fakeRatioReads.load(), 2u);
}

TEST_F(QueryMemoryTest, ConcurrentCheckpointsStopTheSameRequest) {
  fakeRatio = 1.0f;
  std::vector<std::thread> checkpoints;
  std::atomic<int> stopped = 0;
  for (int t = 0; t < 4; ++t) {
    checkpoints.emplace_back([&] {
      if (QueryMemory_CheckpointExact(&timeout_)) {
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

TEST_F(QueryMemoryTest, ForcedRatioReplacesTheMemoryRead) {
  QueryMemoryDebug_SetRatioReadInterval(1);
  RedisModule_GetUsedMemoryRatioEstimate = nullptr;
  QueryMemory_SetForcedRatio(0.5f);
  EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
  QueryMemory_SetForcedRatio(1.0f);
  EXPECT_TRUE(QueryMemory_Checkpoint(&timeout_));
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, ResetRestoresTheRealRead) {
  QueryMemory_SetForcedRatio(3.0f);
  QueryMemory_ResetFakePressure();
  QueryMemoryDebug_SetRatioReadInterval(1);
  EXPECT_LT(QueryMemory_GetForcedRatio(), 0);
  EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
  EXPECT_EQ(fakeRatioReads.load(), 1u);
}

TEST_F(QueryMemoryTest, PressureAtCallStopsOnlyTheNthCall) {
  TimeoutConfig timeoutConfig = {0, TimeoutPolicy_Return};
  QueryRequestLimits other = {};
  QueryRequestLimits_Init(&other, &timeoutConfig);

  QueryMemory_SetPressureAtCall(3);
  EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
  EXPECT_FALSE(QueryMemory_Checkpoint(&other));
  // The third call reports pressure without waiting for the read interval or reading memory.
  EXPECT_TRUE(QueryMemory_Checkpoint(&timeout_));
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));
  EXPECT_EQ(fakeRatioReads.load(), 0u);

  // Calls that return on the requested stop are not counted.
  EXPECT_TRUE(QueryMemory_Checkpoint(&timeout_));
  EXPECT_EQ(QueryMemory_GetCheckpointCalls(), 3u);

  EXPECT_FALSE(QueryMemory_Checkpoint(&other));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&other));

  // Re-arming restarts the count.
  QueryMemory_SetPressureAtCall(1);
  EXPECT_EQ(QueryMemory_GetCheckpointCalls(), 0u);
  EXPECT_TRUE(QueryMemory_Checkpoint(&other));
}

TEST_F(QueryMemoryTest, PressureAtCallHonorsOnOomIgnore) {
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
  QueryMemory_SetPressureAtCall(1);
  EXPECT_FALSE(QueryMemory_Checkpoint(&timeout_));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, StopsBetweenTagAlternatives) {
  std::unique_ptr<RedisModuleCtx, decltype(RedisModule_FreeThreadSafeContext)> redisCtx(
      RedisModule_GetThreadSafeContext(nullptr), RedisModule_FreeThreadSafeContext);
  QueryError status = QueryError_Default();
  std::unique_ptr<QueryError, decltype(&QueryError_ClearError)> clearStatus{&status,
                                                                            QueryError_ClearError};
  const char *schema[] = {"SCHEMA", "tag", "TAG"};
  StrongRef ref = IndexSpec_ParseC(redisCtx.get(), "tag_alternatives", schema, 3, &status);
  auto unlinkSpec = [ref](IndexSpec *) { IndexSpec_Unlink(ref, false); };
  std::unique_ptr<IndexSpec, decltype(unlinkSpec)> spec(
      static_cast<IndexSpec *>(StrongRef_Get(ref)), unlinkSpec);
  ASSERT_NE(spec, nullptr) << QueryError_GetUserError(&status);

  RedisSearchCtx ctx = SEARCH_CTX_STATIC(redisCtx.get(), spec.get());
  ctx.limits = &timeout_;
  TagIndex *index = TagIndex_Ensure(&spec->fields[0], nullptr, false);
  const char *tags[] = {"apple", "apricot", "banana"};
  for (t_docId id = 1; id <= 3; ++id) {
    const TagIndexIndexCtx entry = {nullptr, &tags[id - 1], 1, id, false, &spec->stats};
    ASSERT_TRUE(TagIndex_Index(redisCtx.get(), index, &entry));
  }

  QASTCXX ast(ctx);
  ASSERT_TRUE(ast.parse("@tag:{ap*|banana}", 2)) << ast.getError();
  iteratorsConfig_init(&ast.config);
  SearchOptionsCXX options;
  // The prefix opens one reader before its second checkpoint requests a stop.
  QueryMemory_SetPressureAtCall(2);
  std::unique_ptr<QueryIteratorTree, decltype(&QueryIteratorTree_Free)> tree(
      QAST_Iterate(&ast, &options, &ctx, QEXEC_F_IS_SEARCH, &status), QueryIteratorTree_Free);
  ASSERT_NE(tree, nullptr) << QueryError_GetUserError(&status);
  ASSERT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  QueryIterator *root = QueryIteratorTree_Root(tree.get());
  ASSERT_EQ(root->Read(root), ITERATOR_OK);
  EXPECT_EQ(root->current->docId, 1u);
  EXPECT_EQ(root->Read(root), ITERATOR_EOF);
}

// VecSim calls the stop callback at every search step, so memory is read once per interval of
// calls, not on each of them.
TEST_F(QueryMemoryTest, VecSimStopCallbackReadsMemoryOncePerInterval) {
  const uint32_t interval = QueryMemory_GetRatioReadInterval();
  fakeRatio = 0.5f;
  for (uint32_t i = 0; i < interval; ++i) {
    EXPECT_EQ(RS_VecSimShouldStop(&timeout_), 0);
  }
  EXPECT_EQ(fakeRatioReads.load(), 1u);
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  fakeRatio = 1.0f;
  bool stopped = false;
  for (uint32_t i = 0; i < interval && !stopped; ++i) {
    stopped = RS_VecSimShouldStop(&timeout_) != 0;
  }
  EXPECT_TRUE(stopped);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, VecSimStopCallbackStopsOnTimeoutWithoutReadingMemory) {
  auto *saved = vecsimTimeoutCallback;
  vecsimTimeoutCallback = [](QueryRequestLimits *) -> int { return 1; };
  EXPECT_NE(RS_VecSimShouldStop(&timeout_), 0);
  vecsimTimeoutCallback = saved;
  EXPECT_EQ(fakeRatioReads.load(), 0u);
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, VecSimStopCallbackReportsAnEarlierStopWithoutReading) {
  QueryRequestLimits_RequestMemoryStop(&timeout_);
  EXPECT_NE(RS_VecSimShouldStop(&timeout_), 0);
  EXPECT_EQ(fakeRatioReads.load(), 0u);
}

TEST_F(QueryMemoryTest, VecSimStopCallbackIsOffWhenOnOomIsIgnore) {
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
  fakeRatio = 2.0f;
  EXPECT_EQ(RS_VecSimShouldStop(&timeout_), 0);
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, ShouldStopNowForcedRatioRequestsAnOomStop) {
  RS_AtomicUintStoreRelaxed(&timeout_.memory.counter, 5);
  QueryMemory_SetForcedRatio(0.5f);
  EXPECT_FALSE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  QueryMemory_SetForcedRatio(1.0f);
  EXPECT_TRUE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));
  // The forced ratio replaces the memory read entirely.
  EXPECT_EQ(fakeRatioReads.load(), 0u);
  EXPECT_EQ(RS_AtomicUintLoadRelaxed(&timeout_.memory.counter), 5u);
}

TEST_F(QueryMemoryTest, ShouldStopNowForcedRatioHonorsOnOomIgnore) {
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
  QueryMemory_SetForcedRatio(5.0f);
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(QueryMemory_CheckpointExact(&timeout_));
  }
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, ShouldStopNowPressureAtCallStopsOnlyTheNthCall) {
  RS_AtomicUintStoreRelaxed(&timeout_.memory.counter, 5);
  QueryMemory_SetPressureAtCall(3);
  EXPECT_FALSE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_FALSE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_EQ(QueryMemory_GetCheckpointCalls(), 2u);
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));

  EXPECT_TRUE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_EQ(QueryMemory_GetCheckpointCalls(), 3u);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));
  EXPECT_EQ(RS_AtomicUintLoadRelaxed(&timeout_.memory.counter), 5u);

  // Calls that return on the requested stop are not counted.
  EXPECT_TRUE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_EQ(QueryMemory_GetCheckpointCalls(), 3u);
}

TEST_F(QueryMemoryTest, ShouldStopNowPressureAtCallHonorsOnOomIgnore) {
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
  QueryMemory_SetPressureAtCall(1);
  EXPECT_FALSE(QueryMemory_CheckpointExact(&timeout_));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}

TEST_F(QueryMemoryTest, ResetClearsEveryFake) {
  QueryMemory_SetForcedRatio(2.0f);
  QueryMemory_SetPressureAtCall(5);
  QueryMemoryDebug_SetRatioReadInterval(1);
  QueryMemory_ResetFakePressure();
  EXPECT_LT(QueryMemory_GetForcedRatio(), 0);
  EXPECT_EQ(QueryMemory_GetPressureAtCall(), 0u);
  EXPECT_EQ(QueryMemory_GetRatioReadInterval(), (uint32_t)QUERY_MEMORY_RATIO_READ_INTERVAL);
}

class ApplyOomStopTest : public ::testing::Test {
 protected:
  void SetUp() override {
    TimeoutConfig timeoutConfig = {0, TimeoutPolicy_Return};
    QueryRequestLimits_Init(&limits_, &timeoutConfig);
  }
  void TearDown() override {
    QueryError_ClearError(&err_);
  }
  QueryRequestLimits limits_ = {};
  QueryError err_ = QueryError_Default();
};

TEST_F(ApplyOomStopTest, LeavesOtherOutcomesAlone) {
  EXPECT_EQ(ApplyOomStop(&limits_, OomPolicy_Fail, &err_, RS_RESULT_STOPPED), RS_RESULT_STOPPED);
  EXPECT_TRUE(QueryError_IsOk(&err_));

  QueryRequestLimits_RequestMemoryStop(&limits_);
  EXPECT_EQ(ApplyOomStop(&limits_, OomPolicy_Fail, &err_, RS_RESULT_EOF), RS_RESULT_EOF);
  EXPECT_TRUE(QueryError_IsOk(&err_));
}

TEST_F(ApplyOomStopTest, FailRepliesTheOomErrorAndKeepsAnEarlierOne) {
  QueryRequestLimits_RequestMemoryStop(&limits_);
  EXPECT_EQ(ApplyOomStop(&limits_, OomPolicy_Fail, &err_, RS_RESULT_STOPPED), RS_RESULT_ERROR);
  EXPECT_EQ(QueryError_GetCode(&err_), QUERY_ERROR_CODE_OUT_OF_MEMORY);

  QueryError_ClearError(&err_);
  QueryError_SetCode(&err_, QUERY_ERROR_CODE_SYNTAX);
  EXPECT_EQ(ApplyOomStop(&limits_, OomPolicy_Fail, &err_, RS_RESULT_STOPPED), RS_RESULT_ERROR);
  EXPECT_EQ(QueryError_GetCode(&err_), QUERY_ERROR_CODE_SYNTAX);
}

TEST_F(ApplyOomStopTest, ReturnEndsTheResultWithTheOomWarning) {
  QueryRequestLimits_RequestMemoryStop(&limits_);
  EXPECT_EQ(ApplyOomStop(&limits_, OomPolicy_Return, &err_, RS_RESULT_STOPPED), RS_RESULT_EOF);
  EXPECT_TRUE(QueryError_IsOk(&err_));
  EXPECT_TRUE(QueryError_HasQueryOOMWarning(&err_));
}

TEST_F(ApplyOomStopTest, ABlockedClientTimeoutKeepsItsOutcome) {
  TimeoutConfig config = {1000, TimeoutPolicy_ReturnStrict};
  QueryRequestLimits_Init(&limits_, &config);
  QueryRequestLimits_BeginCycle(&limits_, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestLimits_RequestMemoryStop(&limits_);
  QueryRequestLimits_MarkTimedOut(&limits_);
  EXPECT_EQ(ApplyOomStop(&limits_, OomPolicy_Return, &err_, RS_RESULT_STOPPED), RS_RESULT_STOPPED);
  EXPECT_EQ(ApplyOomStop(&limits_, OomPolicy_Fail, &err_, RS_RESULT_STOPPED), RS_RESULT_STOPPED);
  EXPECT_TRUE(QueryError_IsOk(&err_));
  EXPECT_FALSE(QueryError_HasQueryOOMWarning(&err_));
}

// Stops the request for memory and, when `timedOut`, fires its timeout too.
static void stopForMemory(QueryRequestLimits *limits, RSTimeoutPolicy policy, bool timedOut) {
  TimeoutConfig config = {0, policy};
  QueryRequestLimits_Init(limits, &config);
  if (timedOut) {
    // An elapsed clock deadline: a blocked-client timeout owns the reply and is covered apart.
    *QueryRequestLimits_GetClockDeadlineForUpdate(limits) = {0, 0};
  }
  QueryRequestLimits_RequestMemoryStop(limits);
}

TEST_F(ApplyOomStopTest, OomReturnKeepsATimeoutThatAlsoFired) {
  // The timeout error (ON_TIMEOUT FAIL) or warning (RETURN) is derived from the STOPPED rc;
  // the OOM warning rides along.
  for (RSTimeoutPolicy policy : {TimeoutPolicy_Fail, TimeoutPolicy_Return}) {
    QueryRequestLimits limits = {};
    stopForMemory(&limits, policy, true);
    EXPECT_EQ(ApplyOomStop(&limits, OomPolicy_Return, &err_, RS_RESULT_STOPPED), RS_RESULT_STOPPED)
        << "timeout policy " << policy;
    EXPECT_TRUE(QueryError_IsOk(&err_));
    EXPECT_TRUE(QueryError_HasQueryOOMWarning(&err_));
    QueryError_ClearError(&err_);
  }
}

TEST_F(ApplyOomStopTest, OomReturnEndsWithEofWhenOnlyMemoryStopped) {
  for (RSTimeoutPolicy policy : {TimeoutPolicy_Fail, TimeoutPolicy_Return}) {
    QueryRequestLimits limits = {};
    stopForMemory(&limits, policy, false);
    EXPECT_EQ(ApplyOomStop(&limits, OomPolicy_Return, &err_, RS_RESULT_STOPPED), RS_RESULT_EOF)
        << "timeout policy " << policy;
    EXPECT_TRUE(QueryError_HasQueryOOMWarning(&err_));
    QueryError_ClearError(&err_);
  }
}

TEST_F(ApplyOomStopTest, OomFailBeatsATimeoutThatAlsoFired) {
  for (RSTimeoutPolicy policy : {TimeoutPolicy_Fail, TimeoutPolicy_Return}) {
    QueryRequestLimits limits = {};
    stopForMemory(&limits, policy, true);
    EXPECT_EQ(ApplyOomStop(&limits, OomPolicy_Fail, &err_, RS_RESULT_STOPPED), RS_RESULT_ERROR)
        << "timeout policy " << policy;
    EXPECT_EQ(QueryError_GetCode(&err_), QUERY_ERROR_CODE_OUT_OF_MEMORY);
    QueryError_ClearError(&err_);
  }
}

}  // namespace

TEST_F(QueryMemoryTest, BuildCheckReadsMemoryOnEveryCall) {
  // At the default interval: the build stages are each checked once per query.
  QueryError err = QueryError_Default();
  fakeRatio = 0.5f;
  EXPECT_EQ(QueryMemory_CheckBeforeExecution(&timeout_, OomPolicy_Fail, &err), REDISMODULE_OK);
  EXPECT_EQ(QueryMemory_CheckBeforeExecution(&timeout_, OomPolicy_Fail, &err), REDISMODULE_OK);
  EXPECT_EQ(fakeRatioReads.load(), 2u);
  EXPECT_TRUE(QueryError_IsOk(&err));

  fakeRatio = 1.0f;
  EXPECT_EQ(QueryMemory_CheckBeforeExecution(&timeout_, OomPolicy_Fail, &err), REDISMODULE_ERR);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_OUT_OF_MEMORY);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));
  QueryError_ClearError(&err);
}

TEST_F(QueryMemoryTest, BuildCheckUnderReturnOnlyRequestsTheStop) {
  QueryError err = QueryError_Default();
  fakeRatio = 1.0f;
  EXPECT_EQ(QueryMemory_CheckBeforeExecution(&timeout_, OomPolicy_Return, &err), REDISMODULE_OK);
  EXPECT_TRUE(QueryError_IsOk(&err));
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&timeout_));
}
