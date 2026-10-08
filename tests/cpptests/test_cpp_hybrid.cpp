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
