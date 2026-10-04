/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
extern "C" {
#include "reply.h"
}
#include "aggregate/aggregate.h"
#include "hybrid/hybrid_request.h"
#include "hybrid/hybrid_exec.h"
#include "module.h"
#include "redismock/util.h"
#include <atomic>
#include <thread>

class HybridRequestBasicTest : public ::testing::Test {};

TEST_F(HybridRequestBasicTest, RecoveryReadsOnlyPublishedInputErrors) {
  HybridRequest request = {};
  request.base.reply.err = QueryError_Default();
  AREQ *visible = AREQ_New(nullptr, 0);
  // An inaccessible input makes any accidental metadata read fail immediately.
  AREQ *inputs[] = {nullptr, visible};
  request.requests = inputs;
  request.nrequests = 2;
  bool published[] = {false, true};
  QueryError_SetError(&visible->base.reply.err, QUERY_ERROR_CODE_GENERIC, "published failure");
  EXPECT_EQ(&visible->base.reply.err, HybridRequest_GetPublishedFatalError(&request, published));
  published[1] = false;
  EXPECT_EQ(nullptr, HybridRequest_GetPublishedFatalError(&request, published));
  QueryError_SetError(&request.base.reply.err, QUERY_ERROR_CODE_GENERIC, "tail failure");
  EXPECT_EQ(&request.base.reply.err, HybridRequest_GetPublishedFatalError(&request, published));
  QueryError_ClearError(&request.base.reply.err);
  AREQ_Free(visible);
}

TEST_F(HybridRequestBasicTest, RecoverySerializationKeepsUnpublishedDiagnosticsUntouched) {
  for (bool automatic : {false, true}) {
    SCOPED_TRACE(automatic);
    AREQ **requests = array_new(AREQ *, 2);
    auto *hidden = AREQ_New(nullptr, 0);
    auto *visible = AREQ_New(nullptr, 0);
    array_append(requests, hidden);
    array_append(requests, visible);
    RMCK::ArgvList args(nullptr, "FT.HYBRID", "idx");
    auto *sctx = static_cast<RedisSearchCtx *>(rm_new(RedisSearchCtx));
    *sctx = SEARCH_CTX_STATIC(nullptr, nullptr);
    auto *request = HybridRequest_New(sctx, requests, 2, args, args.size());
    auto *producer = RPSafeDepleter_New(DepleterSync_New(1, false), sctx, depleterPool);
    QITR_PushRP(&hidden->pipeline.qctx, producer);
    request->reqflags |= QEXEC_F_PROFILE;
    request->base.timeout.config.timeoutPolicy = TimeoutPolicy_ReturnStrict;
    static bool profileCalled;
    profileCalled = false;
    request->base.timeoutWasCapped = true;
    request->profile = [](RedisModule_Reply *reply, HybridRequest *request, const bool *published) {
      EXPECT_NE(nullptr, published);
      EXPECT_FALSE(published[0]);
      EXPECT_TRUE(published[1]);
      profileCalled = true;
      RedisModule_Reply_EmptyMap(reply);
    };
    QueryError_SetError(&hidden->base.reply.err, QUERY_ERROR_CODE_GENERIC, "unpublished failure");
    request->subqueriesReturnCodes[0] = RS_RESULT_ERROR;
    HREQ_StoreResults(request, array_new(SearchResult *, 1), RS_RESULT_TIMEDOUT, cachedVars{});
    bool published[] = {false, true};
    auto *ctx = RedisModule_GetThreadSafeContext(nullptr);
    auto reply = RedisModule_NewReply(ctx);
    std::atomic<bool> stop{false};
    std::atomic<bool> started{false};
    std::thread producerStateWriter([&] {
      hidden->stateflags |= QEXEC_S_SHARD_TIMED_OUT_WARNING;
      started.store(true);
      while (!stop.load()) {
        hidden->stateflags ^= QEXEC_S_SHARD_TIMED_OUT_WARNING;
      }
    });
    while (!started.load()) {
      std::this_thread::yield();
    }

    if (automatic) {
      serializeStoredResults_hybrid(request, &reply);
    } else {
      serializePublishedResults_hybrid(request, &reply, published);
    }
    RedisModule_EndReply(&reply);
    stop.store(true);
    producerStateWriter.join();

    EXPECT_TRUE(profileCalled);
    EXPECT_TRUE(request->base.timeoutWasCapped);
    EXPECT_EQ(QUERY_ERROR_CODE_GENERIC, QueryError_GetCode(&hidden->base.reply.err));
    EXPECT_FALSE(request->base.reply.hasStoredResults);
    EXPECT_EQ(nullptr, request->base.reply.results);
    HybridRequest_Free(request);
    RedisModule_FreeThreadSafeContext(ctx);
  }
}

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
  EXPECT_EQ(hybridReq->replyflags, 0);
  EXPECT_EQ(req1->replyflags, 0);
  EXPECT_EQ(req2->replyflags, 0);
  hybridReq->replyflags |= QUERY_REPLY_F_TIMEOUT_CAPPED;
  req1->replyflags |= QUERY_REPLY_F_TIMEOUT_CAPPED;

  // Verify the merge pipeline is initialized
  ASSERT_TRUE(hybridReq->tailPipeline->ap.steps.next != nullptr);
  // Only the container owns result rows; subqueries retain their own error slots.
  HREQ_StoreResults(hybridReq, nullptr, RS_RESULT_EOF, cachedVars{});
  EXPECT_TRUE(hybridReq->base.reply.hasStoredResults);
  EXPECT_EQ(hybridReq->base.reply.rc, RS_RESULT_EOF);
  EXPECT_FALSE(req1->base.reply.hasStoredResults);
  EXPECT_FALSE(req2->base.reply.hasStoredResults);
  QueryRequest_ResetReply(&hybridReq->base);
  QueryRequest_ResetReply(&req1->base);
  EXPECT_EQ(hybridReq->replyflags, QUERY_REPLY_F_TIMEOUT_CAPPED);
  EXPECT_EQ(req1->replyflags, QUERY_REPLY_F_TIMEOUT_CAPPED);
  EXPECT_FALSE(hybridReq->base.reply.hasStoredResults);
  EXPECT_EQ(hybridReq->base.reply.results, nullptr);
  HybridRequest_Free(hybridReq);
}
