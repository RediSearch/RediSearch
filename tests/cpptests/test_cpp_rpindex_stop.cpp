/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */


#include "gtest/gtest.h"
#include <string>
#include "iterator_util.h"
#include "query_request.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "indexes.h"
#include "result_processor.h"
#include "search_result_ffi.h"
#include "search_ctx.h"
#include "spec.h"

class RPIndexStopTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx = nullptr;
  RedisSearchCtx *sctx = nullptr;
  QueryRequestLimits limits = {};
  QueryProcessingCtx qctx = {};
  MockIterator *mock = nullptr;
  ResultProcessor *rp = nullptr;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);
    // Specs outlive flushdb, so every test gets its own index.
    static int indexCount = 0;
    const std::string name = "rpindex_stop" + std::to_string(++indexCount);
    QueryError qerr = QueryError_Default();
    RMCK::ArgvList args(ctx, "FT.CREATE", name.c_str(), "ON", "HASH", "SCHEMA", "t", "TEXT");
    ASSERT_TRUE(Indexes_CreateNewSpec(ctx, args, args.size(), &qerr));
    sctx = NewSearchCtxC(ctx, name.c_str(), true);
    ASSERT_TRUE(sctx);
    // TIMEOUT 0 keeps every cycle UNARMED: only a memory stop can end a read.
    TimeoutConfig timeoutConfig = {0, TimeoutPolicy_Return};
    QueryRequestLimits_Init(&limits, &timeoutConfig);
    sctx->limits = &limits;
    mock = new MockIterator(t_docId{1}, t_docId{2}, t_docId{3});
    rp = RPQueryIterator_New(&mock->base, NULL, 0, sctx, false);
    QITR_PushRP(&qctx, rp);
  }

  void TearDown() override {
    if (rp) rp->Free(rp);
    if (sctx) SearchCtx_Free(sctx);
    RMCK::flushdb(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
  }

  // The mock's doc ids are not in the doc table, so a read skips them all to EOF.
  int next() {
    SearchResult res = SearchResult_New();
    const int rc = rp->Next(rp, &res);
    SearchResult_Destroy(&res);
    EXPECT_FALSE(IndexSpec_IsLocked(sctx->spec));
    return rc;
  }
};

TEST_F(RPIndexStopTest, AStopAloneDoesNotEndTheRead) {
  QueryRequestLimits_RequestMemoryStop(&limits);
  EXPECT_EQ(next(), RS_RESULT_EOF);
  EXPECT_GT(mock->readCount, 0);
}

TEST_F(RPIndexStopTest, EndRowSourceOnStopRepliesTimedOut) {
  QueryRequestLimits_RequestMemoryStop(&limits);
  RP_EndRowSourceOnStop(&qctx);
  RP_EndRowSourceOnStop(&qctx);
  EXPECT_EQ(next(), RS_RESULT_STOPPED);
  EXPECT_EQ(next(), RS_RESULT_STOPPED);
  EXPECT_EQ(mock->readCount, 0);
}
