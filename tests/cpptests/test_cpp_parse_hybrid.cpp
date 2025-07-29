/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "common.h"
#include "src/hybrid/hybrid_request.h"
#include "src/hybrid/parse_hybrid.h"
#include "src/spec.h"
#include "src/search_ctx.h"
#include "src/query_error.h"
#include "src/rmalloc.h"
#include "src/index.h"

class ParseHybridTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx;
  IndexSpec *spec;
  std::string index_name;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);

    // Initialize pointers to NULL
    spec = NULL;

    // Generate a unique index name for each test to avoid conflicts
    const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();
    index_name = std::string("test_index_") + test_info->test_case_name() + "_" + test_info->name();

    // Create a simple index for testing (without vector field to avoid setup complexity)
    QueryError qerr = {QueryErrorCode(0)};
    RMCK::ArgvList args(ctx, "FT.CREATE", index_name.c_str(), "ON", "HASH",
                        "SCHEMA", "title", "TEXT", "content", "TEXT");
    spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
    if (!spec) {
      printf("Failed to create index '%s': code=%d, detail='%s'\n",
             index_name.c_str(), qerr.code, qerr.detail ? qerr.detail : "NULL");
      QueryError_ClearError(&qerr);
    }
    ASSERT_TRUE(spec);

  }

  void TearDown() override {
    if (ctx) {
      // Drop the index to clean up
      RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.DROPINDEX", "c", index_name.c_str());
      if (reply) {
        RedisModule_FreeCallReply(reply);
      }
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = NULL;
    }
  }

};


#define assertLinearScoringCtx(Weight0, Weight1) { \
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR); \
  ASSERT_EQ(result->hybridParams->scoringCtx->linearCtx.numWeights, HYBRID_REQUEST_NUM_SUBQUERIES); \
  ASSERT_TRUE(result->hybridParams->scoringCtx->linearCtx.linearWeights != NULL); \
  ASSERT_DOUBLE_EQ(result->hybridParams->scoringCtx->linearCtx.linearWeights[0], Weight0); \
  ASSERT_DOUBLE_EQ(result->hybridParams->scoringCtx->linearCtx.linearWeights[1], Weight1); \
}

#define assertRRFScoringCtx(K, Window) { \
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_RRF); \
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.k, K); \
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.window, Window); \
}


TEST_F(ParseHybridTest, testBasicValidInput) {
  QueryError status = {QueryErrorCode(0)};

  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "world");

  // Create a fresh sctx for this test since parseHybridCommand takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify default scoring type is RRF
  assertRRFScoringCtx(1, 20);

  // Verify timeout is set to default
  ASSERT_EQ(result->requests[0]->reqConfig.queryTimeoutMS, 500);
  ASSERT_EQ(result->requests[1]->reqConfig.queryTimeoutMS, 500);

  // Verify dialect is set to default
  ASSERT_EQ(result->requests[0]->reqConfig.dialectVersion, 1);
  ASSERT_EQ(result->requests[1]->reqConfig.dialectVersion, 1);

  // parseHybridCommand calls AREQ_ApplyContext which takes ownership of test_sctx
  // No need to free test_sctx as it's now owned by the result

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testValidInputWithParams) {
  QueryError status = {QueryErrorCode(0)};

  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "@title:($param1)", "VSIM", "world",
                      "PARAMS", "2", "param1", "hello", "DIALECT", "2");

  // Create a fresh sctx for this test since parseHybridCommand takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // // Debug: Print error details if parsing failed
  // if (!result) {
  //   printf("Parsing failed: code=%d, detail='%s'\n", status.code, status.detail ? status.detail : "NULL");
  //   fflush(stdout);
  // }

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify default scoring type is RRF
  assertRRFScoringCtx(1, 20);

  // Verify timeout is set to default
  ASSERT_EQ(result->requests[0]->reqConfig.queryTimeoutMS, 500);
  ASSERT_EQ(result->requests[1]->reqConfig.queryTimeoutMS, 500);

  // Verify dialect is set to default
  ASSERT_EQ(result->requests[0]->reqConfig.dialectVersion, 2);
  ASSERT_EQ(result->requests[1]->reqConfig.dialectVersion, 2);

  // parseHybridCommand calls AREQ_ApplyContext which takes ownership of test_sctx
  // No need to free test_sctx as it's now owned by the result

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testValidInputWithReqConfig) {
  QueryError status = {QueryErrorCode(0)};

  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "world", "TIMEOUT", "240", "DIALECT", "2");

  // Create a fresh sctx for this test since parseHybridCommand takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify default scoring type is RRF
  assertRRFScoringCtx(1, 20);

  // Verify timeout is set correctly
  ASSERT_EQ(result->requests[0]->reqConfig.queryTimeoutMS, 240);
  ASSERT_EQ(result->requests[1]->reqConfig.queryTimeoutMS, 240);

  // Verify dialect is set correctly
  ASSERT_EQ(result->requests[0]->reqConfig.dialectVersion, 2);
  ASSERT_EQ(result->requests[1]->reqConfig.dialectVersion, 2);

  // parseHybridCommand calls AREQ_ApplyContext which takes ownership of test_sctx
  // No need to free test_sctx as it's now owned by the result

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testMissingSearchParameter) {
  QueryError status = {QueryErrorCode(0)};

  // Missing SEARCH parameter: FT.HYBRID <index> VSIM @vector_field
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "VSIM", "@vector_field");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_ESYNTAX);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testMissingQueryStringAfterSearch) {
  QueryError status = {QueryErrorCode(0)};

  // Missing query string after SEARCH: FT.HYBRID <index> SEARCH
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  ASSERT_STREQ(status.detail, "No query string provided for SEARCH");

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testMissingSecondSearchParameter) {
  QueryError status = {QueryErrorCode(0)};

  // Missing second search parameter: FT.HYBRID <index> SEARCH hello
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_ESYNTAX);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testWithCombineLinear) {
  QueryError status = {QueryErrorCode(0)};

  // Test with LINEAR combine method
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "world", "COMBINE", "LINEAR", "0.7", "0.3");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // // Debug: Print error details if parsing failed
  // if (!result) {
  //   printf("Parsing failed: code=%d, detail='%s'\n", status.code, status.detail ? status.detail : "NULL");
  //   fflush(stdout);
  // }

  // Verify the request was parsed successfully
  EXPECT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify LINEAR scoring type was set
  assertLinearScoringCtx(0.7, 0.3);

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testWithCombineRRF) {
  QueryError status = {QueryErrorCode(0)};

  // Test with RRF combine method
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "world", "COMBINE", "RRF");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify RRF scoring type was set
  assertRRFScoringCtx(1, 20);

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testInvalidSearchAfterSearch) {
  QueryError status = {QueryErrorCode(0)};

  // Test invalid syntax: FT.HYBRID <index> SEARCH hello SEARCH world (should fail)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "SEARCH", "world");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testVectorParameterAdvancing) {
  QueryError status = {QueryErrorCode(0)};

  // Test that vector parameters are properly skipped until COMBINE keyword
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector_query", "param1", "value1", "param2", "value2", "COMBINE", "RRF");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify RRF scoring type was set (meaning COMBINE was found and parsed)
  assertRRFScoringCtx(1, 20);

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testVectorParameterAdvancingToLimit) {
  QueryError status = {QueryErrorCode(0)};

  // Test that vector parameters are properly skipped until LIMIT keyword
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector_query", "method", "HNSW", "ef_runtime", "100", "LIMIT", "0", "10");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify default RRF scoring type was set
  assertRRFScoringCtx(1, 20);

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testComplexSingleLineCommand) {
  QueryError status = {QueryErrorCode(0)};

  // Example of a complex command in a single line
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector_field", "method", "HNSW", "k", "10",
                            "COMBINE", "LINEAR", "0.65", "0.35", "SORTBY", "1", "@score", "LIMIT", "0", "20");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Debug: Print error details if parsing failed
  if (!result) {
    printf("Parsing failed: code=%d, detail='%s'\n", status.code, status.detail ? status.detail : "NULL");
    fflush(stdout);
  }

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify LINEAR scoring type was set
  assertLinearScoringCtx(0.65, 0.35);

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testSortBy0DisablesImplicitSort) {
  QueryError status = {QueryErrorCode(0)};

  // Test SORTBY 0 to disable implicit sorting
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "world", "SORTBY", "0");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify that an arrange step was created with noSort flag set
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&result->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep != NULL);
  const PLN_ArrangeStep *arng = (const PLN_ArrangeStep *)arrangeStep;
  ASSERT_TRUE(arng->noSort);
  ASSERT_TRUE(arng->sortKeys == NULL);

  // Verify default RRF scoring type was set
  assertRRFScoringCtx(1, 20);

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testSortByFieldDoesNotDisableImplicitSort) {
  QueryError status = {QueryErrorCode(0)};

  // Test SORTBY with actual field (not 0) - should not disable implicit sorting
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "world", "SORTBY", "1", "@score");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify that an arrange step was created with normal sorting (not noSort)
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&result->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep != NULL);
  const PLN_ArrangeStep *arng = (const PLN_ArrangeStep *)arrangeStep;
  ASSERT_FALSE(arng->noSort);
  ASSERT_TRUE(arng->sortKeys != NULL);

  // Verify default RRF scoring type was set
  assertRRFScoringCtx(1, 20);

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testNoSortByDoesNotDisableImplicitSort) {
  QueryError status = {QueryErrorCode(0)};

  // Test without SORTBY - should not disable implicit sorting (default behavior)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "world");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify that no arrange step exists (so implicit sorting will be applied)
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&result->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep == NULL);

  // Verify default RRF scoring type was set
  assertRRFScoringCtx(1, 20);

  // Clean up
  // The scoring context is freed by the hybrid merger
  HybridScoringContext_Free(result->hybridParams->scoringCtx);
  HybridRequest_Free(result);
}

// Test that SORTBY 0 correctly sets noSort flag and clears sortKeys
TEST_F(ParseHybridTest, ParseSortby0_SetsNoSortFlagAndClearsSortKeys) {
  QueryError qerr = {QueryErrorCode(0)};
  RMCK::ArgvList args(ctx, "hello", "SORTBY", "0");

  AREQ *req = AREQ_New();
  req->reqflags = QEXEC_F_IS_HYBRID;

  int rc = AREQ_Compile(req, args, args.size(), &qerr);
  EXPECT_EQ(REDISMODULE_OK, rc) << QueryError_GetUserError(&qerr);

  RedisSearchCtx *sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(sctx);

  rc = AREQ_ApplyContext(req, sctx, &qerr);
  EXPECT_EQ(REDISMODULE_OK, rc) << QueryError_GetUserError(&qerr);

  // Verify SORTBY 0 sets the correct flags
  PLN_ArrangeStep *arrangeStep = AGPLN_GetArrangeStep(AREQ_AGGPlan(req));
  ASSERT_TRUE(arrangeStep);
  EXPECT_TRUE(arrangeStep->noSort) << "SORTBY 0 should set noSort=true";
  EXPECT_EQ(nullptr, arrangeStep->sortKeys) << "SORTBY 0 should set sortKeys=NULL";

  AREQ_Free(req);
}


