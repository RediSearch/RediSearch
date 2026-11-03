/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "aggregate/aggregate.h"
#include "hybrid/hybrid_request.h"
#include "hybrid/hybrid_exec.h"
#include "redismock/util.h"

class HybridRequestBasicTest : public ::testing::Test {};

// Tests that don't require full Redis Module integration

// Test basic HybridRequest creation and initialization with multiple AREQ requests
TEST_F(HybridRequestBasicTest, testHybridRequestCreationBasic) {
  // Test basic HybridRequest creation without Redis dependencies
  AREQ **requests = array_new(AREQ*, 2);
  // Initialize the AREQ structures
  AREQ *req1 = AREQ_New(NULL, 0);
  AREQ *req2 = AREQ_New(NULL, 0);

  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  // Construction requires an argv; the command + index tokens are stepped
  // over, so the wrappers hold nothing here.
  RMCK::ArgvList args(NULL, "FT.HYBRID", "idx");
  RedisSearchCtx *sctx = (RedisSearchCtx *)rm_new(RedisSearchCtx);
  *sctx = SEARCH_CTX_STATIC(NULL, NULL);
  HybridRequest *hybridReq = HybridRequest_New(sctx, requests, 2, args, args.size());
  ASSERT_TRUE(hybridReq != nullptr);
  ASSERT_EQ(hybridReq->nrequests, 2);
  ASSERT_TRUE(hybridReq->requests != nullptr);

  // Verify the merge pipeline is initialized
  ASSERT_TRUE(hybridReq->tailPipeline->ap.steps.next != nullptr);
  // Only the container owns result rows; subqueries retain their own error slots.
  HREQ_StoreResults(hybridReq, nullptr, RS_RESULT_EOF, cachedVars{});
  EXPECT_TRUE(hybridReq->base.reply.hasStoredResults);
  EXPECT_EQ(hybridReq->base.reply.rc, RS_RESULT_EOF);
  EXPECT_FALSE(req1->base.reply.hasStoredResults);
  EXPECT_FALSE(req2->base.reply.hasStoredResults);
  QueryRequest_ResetReply(&hybridReq->base);
  EXPECT_FALSE(hybridReq->base.reply.hasStoredResults);
  EXPECT_EQ(hybridReq->base.reply.results, nullptr);
  HybridRequest_Free(hybridReq);
}

class HybridApplyOomStopTest : public ::testing::Test {
 protected:
  RMCK::ArgvList args{NULL, "FT.HYBRID", "idx"};
  HybridRequest *hreq = nullptr;

  void SetUp() override {
    AREQ **requests = array_new(AREQ *, 2);
    for (int i = 0; i < 2; ++i) {
      AREQ *subquery = AREQ_New(NULL, 0);
      requests = array_ensure_append_1(requests, subquery);
    }
    RedisSearchCtx *sctx = (RedisSearchCtx *)rm_new(RedisSearchCtx);
    *sctx = SEARCH_CTX_STATIC(NULL, NULL);
    hreq = HybridRequest_New(sctx, requests, 2, args, args.size());
    hreq->subqueriesReturnCodes[0] = RS_RESULT_EOF;
    hreq->subqueriesReturnCodes[1] = RS_RESULT_EOF;
  }
  void TearDown() override { HybridRequest_Free(hreq); }

  QueryError *tailErr() { return hreq->tailPipeline->qctx.err; }
  // A genuine timeout of the subquery: its blocked-client deadline has fired.
  void timeOutSubquery(size_t i) {
    QueryRequestLimits *limits = &hreq->requests[i]->base.limits;
    QueryRequestLimits_BeginCycle(limits, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
    QueryRequestLimits_MarkTimedOut(limits);
  }
  void stopSubqueryForOom(size_t i) {
    QueryRequestLimits_RequestMemoryStop(&hreq->requests[i]->base.limits);
    hreq->subqueriesReturnCodes[i] = RS_RESULT_TIMEDOUT;
  }
};

// Subquery checkpoints request their stop on the subquery's own timeout; the request still ends
// per ON_OOM, without stopping the tail, which still holds the merged rows.
TEST_F(HybridApplyOomStopTest, subqueryOomStopWarnsWithoutStoppingTheTail) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Return;
  stopSubqueryForOom(1);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_OK), RS_RESULT_OK);
  EXPECT_TRUE(QueryError_HasQueryOOMWarning(tailErr()));
  EXPECT_EQ(hreq->subqueriesReturnCodes[1], RS_RESULT_EOF);
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&hreq->base.limits));
}

// The merger under ON_TIMEOUT FAIL turns the subquery's stop into a tail timeout.
TEST_F(HybridApplyOomStopTest, mergerTimeoutFromASubqueryOomStopIsNotATimeout) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Return;
  stopSubqueryForOom(0);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_EOF);
  EXPECT_TRUE(QueryError_HasQueryOOMWarning(tailErr()));
}

// Another subquery's genuine timeout is what the merger reports: it must not be downgraded to a
// partial reply because a sibling stopped for OOM.
TEST_F(HybridApplyOomStopTest, concurrentSubqueryTimeoutStaysATimeout) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Return;
  stopSubqueryForOom(0);
  timeOutSubquery(1);
  hreq->subqueriesReturnCodes[1] = RS_RESULT_TIMEDOUT;
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_TIMEDOUT);
  EXPECT_EQ(hreq->subqueriesReturnCodes[0], RS_RESULT_EOF);
  EXPECT_EQ(hreq->subqueriesReturnCodes[1], RS_RESULT_TIMEDOUT);
}

// Both stops on one subquery: the elapsed deadline outranks the OOM stop under ON_OOM RETURN, which
// still adds its warning; the merger's timeout stays the tail's outcome.
TEST_F(HybridApplyOomStopTest, subqueryTimeoutBesideItsOomStopStaysATimeoutUnderOnOomReturn) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Return;
  stopSubqueryForOom(0);
  *QueryRequestLimits_GetClockDeadlineForUpdate(&hreq->requests[0]->base.limits) = {0, 0};
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_TIMEDOUT);
  EXPECT_EQ(hreq->subqueriesReturnCodes[0], RS_RESULT_TIMEDOUT);
  EXPECT_TRUE(QueryError_HasQueryOOMWarning(tailErr()));
}

// ON_OOM FAIL beats the timeout of the same subquery.
TEST_F(HybridApplyOomStopTest, subqueryOomStopBeatsItsTimeoutUnderOnOomFail) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  stopSubqueryForOom(0);
  *QueryRequestLimits_GetClockDeadlineForUpdate(&hreq->requests[0]->base.limits) = {0, 0};
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_ERROR);
  EXPECT_EQ(QueryError_GetCode(tailErr()), QUERY_ERROR_CODE_OUT_OF_MEMORY);
}

// The tail's own stop follows ApplyOomStop: a deadline that also elapsed keeps the timeout outcome
// under ON_OOM RETURN and loses to the OOM error under FAIL.
TEST_F(HybridApplyOomStopTest, tailTimeoutBesideItsOomStopFollowsTheStopPrecedence) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Return;
  QueryRequestLimits_RequestMemoryStop(&hreq->base.limits);
  *QueryRequestLimits_GetClockDeadlineForUpdate(&hreq->base.limits) = {0, 0};
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_TIMEDOUT);
  EXPECT_TRUE(QueryError_HasQueryOOMWarning(tailErr()));

  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_ERROR);
  EXPECT_EQ(QueryError_GetCode(tailErr()), QUERY_ERROR_CODE_OUT_OF_MEMORY);
}

TEST_F(HybridApplyOomStopTest, subqueryOomStopFailsUnderOnOomFail) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  stopSubqueryForOom(0);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_EOF), RS_RESULT_ERROR);
  EXPECT_EQ(QueryError_GetCode(tailErr()), QUERY_ERROR_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(HybridRequest_GetFatalError(hreq), tailErr());
}

// A soft tail error (not a fatal one) must not mask the OOM failure.
TEST_F(HybridApplyOomStopTest, softTailErrorIsReplacedByOomUnderOnOomFail) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  QueryError_SetCode(tailErr(), QUERY_ERROR_CODE_NO_PROP_VAL);
  stopSubqueryForOom(0);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_EOF), RS_RESULT_ERROR);
  EXPECT_EQ(QueryError_GetCode(tailErr()), QUERY_ERROR_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(HybridRequest_GetFatalError(hreq), tailErr());
}

// A tail-owned OOM stop replaces a soft tail error the same way.
TEST_F(HybridApplyOomStopTest, softTailErrorIsReplacedByATailOomStopUnderOnOomFail) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  QueryError_SetCode(tailErr(), QUERY_ERROR_CODE_NO_PROP_VAL);
  QueryRequestLimits_RequestMemoryStop(&hreq->base.limits);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_ERROR);
  EXPECT_EQ(QueryError_GetCode(tailErr()), QUERY_ERROR_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(HybridRequest_GetFatalError(hreq), tailErr());
}

TEST_F(HybridApplyOomStopTest, fatalTailErrorIsKeptByATailOomStopUnderOnOomFail) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  QueryError_SetCode(tailErr(), QUERY_ERROR_CODE_GENERIC);
  QueryRequestLimits_RequestMemoryStop(&hreq->base.limits);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_ERROR);
  EXPECT_EQ(QueryError_GetCode(tailErr()), QUERY_ERROR_CODE_GENERIC);
}

TEST_F(HybridApplyOomStopTest, fatalTailErrorIsKeptUnderOnOomFail) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  QueryError_SetCode(tailErr(), QUERY_ERROR_CODE_GENERIC);
  stopSubqueryForOom(0);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_EOF), RS_RESULT_ERROR);
  EXPECT_EQ(QueryError_GetCode(tailErr()), QUERY_ERROR_CODE_GENERIC);
}

// A sibling subquery's fatal error must not be hidden by the OOM installed on the tail slot.
TEST_F(HybridApplyOomStopTest, fatalSubqueryErrorIsKeptUnderOnOomFail) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  QueryError *subErr = &hreq->requests[1]->base.reply.err;
  QueryError_SetCode(subErr, QUERY_ERROR_CODE_GENERIC);
  stopSubqueryForOom(0);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_EOF), RS_RESULT_ERROR);
  EXPECT_EQ(HybridRequest_GetFatalError(hreq), subErr);
  EXPECT_EQ(QueryError_GetCode(HybridRequest_GetFatalError(hreq)), QUERY_ERROR_CODE_GENERIC);
}

// The same holds when the tail's own checkpoint stops.
TEST_F(HybridApplyOomStopTest, fatalSubqueryErrorIsKeptByATailOomStopUnderOnOomFail) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  QueryError *subErr = &hreq->requests[1]->base.reply.err;
  QueryError_SetCode(subErr, QUERY_ERROR_CODE_GENERIC);
  QueryRequestLimits_RequestMemoryStop(&hreq->base.limits);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_ERROR);
  EXPECT_EQ(HybridRequest_GetFatalError(hreq), subErr);
}

TEST_F(HybridApplyOomStopTest, subqueryTimeoutIsNotAnOomStop) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Fail;
  timeOutSubquery(0);
  hreq->subqueriesReturnCodes[0] = RS_RESULT_TIMEDOUT;
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_TIMEDOUT);
  EXPECT_EQ(hreq->subqueriesReturnCodes[0], RS_RESULT_TIMEDOUT);
  EXPECT_TRUE(QueryError_IsOk(tailErr()));
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&hreq->base.limits));
}

// The timeout callback replies a blocked-client timeout; a subquery's OOM stop must not turn it
// into an OOM reply.
TEST_F(HybridApplyOomStopTest, blockedClientTimeoutIsNotRelabelled) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Return;
  HybridRequest_BeginTimeoutCycle(hreq, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestLimits_MarkTimedOut(&hreq->base.limits);
  stopSubqueryForOom(0);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_TIMEDOUT);
  EXPECT_EQ(hreq->subqueriesReturnCodes[0], RS_RESULT_TIMEDOUT);
  EXPECT_FALSE(QueryError_HasQueryOOMWarning(tailErr()));
}

// Nor does an OOM stop of the tail's own, requested before the deadline fired.
TEST_F(HybridApplyOomStopTest, blockedClientTimeoutWinsOverATailOomStop) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Return;
  HybridRequest_BeginTimeoutCycle(hreq, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  QueryRequestLimits_RequestMemoryStop(&hreq->base.limits);
  QueryRequestLimits_MarkTimedOut(&hreq->base.limits);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_TIMEDOUT);
  EXPECT_FALSE(QueryError_HasQueryOOMWarning(tailErr()));
}

// A tail timeout of the tail's own, after a subquery's OOM stop was mapped, stays a timeout.
TEST_F(HybridApplyOomStopTest, tailTimeoutAfterASubqueryOomStopStaysATimeout) {
  hreq->base.reqConfig.oomPolicy = OomPolicy_Return;
  stopSubqueryForOom(0);
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_OK), RS_RESULT_OK);
  // The tail's clock deadline has elapsed.
  hreq->base.limits.timeout.kind = QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE;
  hreq->base.limits.timeout.source.clock.deadline = {1, 0};
  EXPECT_EQ(HybridRequest_ApplyOomStop(hreq, RS_RESULT_TIMEDOUT), RS_RESULT_TIMEDOUT);
}
