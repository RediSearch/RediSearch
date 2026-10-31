/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */


#include "gtest/gtest.h"
#include "iterator_util.h"
#include "query_request.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "indexes.h"
#include "result_processor.h"
#include "search_result_ffi.h"
#include "search_ctx.h"
#include "spec.h"

class RPIndexStopTest : public ::testing::Test {};

// The timeout checks ignore a requested stop, so the row source's own read is what ends a query
// whose later stages keep pulling rows under ON_OOM RETURN.
TEST_F(RPIndexStopTest, RequestedStopEndsTheRowSourceBeforeItReads) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RMCK::flushdb(ctx);
  QueryError qerr = QueryError_Default();
  RMCK::ArgvList args(ctx, "FT.CREATE", "rpindex_stop", "ON", "HASH", "SCHEMA", "t", "TEXT");
  ASSERT_TRUE(Indexes_CreateNewSpec(ctx, args, args.size(), &qerr));
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, "rpindex_stop", true);
  ASSERT_TRUE(sctx);

  QueryRequestTimeout timeout = {};
  TimeoutConfig timeoutConfig = {0, TimeoutPolicy_Return};
  QueryRequestTimeout_Init(&timeout, &timeoutConfig);
  sctx->timeout = &timeout;

  for (bool stop : {false, true}) {
    QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    if (stop) {
      QueryRequestTimeout_RequestStop(&timeout, QUERY_STOP_REASON_OOM);
    }
    auto *mock = new MockIterator({1, 2, 3});
    ResultProcessor *rp = RPQueryIterator_New(&mock->base, NULL, 0, sctx);
    QueryProcessingCtx qctx = {};
    rp->parent = &qctx;
    SearchResult res = SearchResult_New();

    // The mock's doc ids are not in the doc table, so an unstopped read skips them to EOF.
    EXPECT_EQ(rp->Next(rp, &res), stop ? RS_RESULT_TIMEDOUT : RS_RESULT_EOF) << stop;
    if (stop) {
      EXPECT_EQ(mock->readCount, 0);
    } else {
      EXPECT_GT(mock->readCount, 0);
    }
    EXPECT_FALSE(IndexSpec_IsLocked(sctx->spec));

    SearchResult_Destroy(&res);
    rp->Free(rp);
  }

  SearchCtx_Free(sctx);
  args.clear();
  RMCK::flushdb(ctx);
  RedisModule_FreeThreadSafeContext(ctx);
}
