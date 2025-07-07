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
#include "src/hybrid/parse_hybrid.h"
#include "src/spec.h"
#include "src/search_ctx.h"
#include "src/query_error.h"

class ParseHybridTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx;
  IndexSpec *spec;
  RedisSearchCtx *sctx;
  HybridScoringContext scoringCtx;
  HybridPipelineParams hybridParams;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);

    // Create a simple index for testing (without vector field to avoid setup complexity)
    QueryError qerr = {QueryErrorCode(0)};
    RMCK::ArgvList args(ctx, "FT.CREATE", "testidx", "ON", "HASH",
                        "SCHEMA", "title", "TEXT", "content", "TEXT");
    spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
    ASSERT_TRUE(spec);

    sctx = NewSearchCtxC(ctx, "testidx", true);
    ASSERT_TRUE(sctx != NULL);

    // Initialize basic HybridPipelineParams for all tests
    memset(&scoringCtx, 0, sizeof(scoringCtx));
    memset(&hybridParams, 0, sizeof(hybridParams));
    hybridParams.synchronize_read_locks = true;
    hybridParams.scoringCtx = &scoringCtx;
  }

  void TearDown() override {
    if (sctx) {
      SearchCtx_Free(sctx);
      sctx = NULL;
    }
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = NULL;
    }
  }

};

TEST_F(ParseHybridTest, testBasicValidInput) {
  QueryError status = {QueryErrorCode(0)};

  // Create a basic hybrid query: FT.HYBRID testidx SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx SEARCH hello VSIM world");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify default scoring type is RRF
  ASSERT_EQ(scoringCtx.scoringType, HYBRID_SCORING_RRF);
  ASSERT_EQ(scoringCtx.rrfCtx.k, 1);
  ASSERT_EQ(scoringCtx.rrfCtx.window, 20);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testMissingSearchParameter) {
  QueryError status = {QueryErrorCode(0)};

  // Missing SEARCH parameter: FT.HYBRID testidx VSIM @vector_field
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx VSIM @vector_field");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_ESYNTAX);

  // Clean up
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testMissingQueryStringAfterSearch) {
  QueryError status = {QueryErrorCode(0)};

  // Missing query string after SEARCH: FT.HYBRID testidx SEARCH
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx SEARCH");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  ASSERT_STREQ(status.detail, "No query string provided for SEARCH");

  // Clean up
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testMissingSecondSearchParameter) {
  QueryError status = {QueryErrorCode(0)};

  // Missing second search parameter: FT.HYBRID testidx SEARCH hello
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx SEARCH hello");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_ESYNTAX);

  // Clean up
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testWithCombineLinear) {
  QueryError status = {QueryErrorCode(0)};

  // Test with LINEAR combine method
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx SEARCH hello VSIM world COMBINE LINEAR 0.7 0.3");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify LINEAR scoring type was set
  ASSERT_EQ(scoringCtx.scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(scoringCtx.linearCtx.numWeights, 2);
  ASSERT_TRUE(scoringCtx.linearCtx.linearWeights != NULL);
  ASSERT_DOUBLE_EQ(scoringCtx.linearCtx.linearWeights[0], 0.7);
  ASSERT_DOUBLE_EQ(scoringCtx.linearCtx.linearWeights[1], 0.3);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testWithCombineRRF) {
  QueryError status = {QueryErrorCode(0)};

  // Test with RRF combine method
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx SEARCH hello VSIM world COMBINE RRF");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify RRF scoring type was set
  ASSERT_EQ(scoringCtx.scoringType, HYBRID_SCORING_RRF);
  ASSERT_EQ(scoringCtx.rrfCtx.k, 1);
  ASSERT_EQ(scoringCtx.rrfCtx.window, 20);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testInvalidSearchAfterSearch) {
  QueryError status = {QueryErrorCode(0)};

  // Test invalid syntax: FT.HYBRID testidx SEARCH hello SEARCH world (should fail)
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx SEARCH hello SEARCH world");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testVectorParameterAdvancing) {
  QueryError status = {QueryErrorCode(0)};

  // Test that vector parameters are properly skipped until COMBINE keyword
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx SEARCH hello VSIM vector_query param1 value1 param2 value2 COMBINE RRF");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify RRF scoring type was set (meaning COMBINE was found and parsed)
  ASSERT_EQ(scoringCtx.scoringType, HYBRID_SCORING_RRF);
  ASSERT_EQ(scoringCtx.rrfCtx.k, 1);
  ASSERT_EQ(scoringCtx.rrfCtx.window, 20);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testVectorParameterAdvancingToLimit) {
  QueryError status = {QueryErrorCode(0)};

  // Test that vector parameters are properly skipped until LIMIT keyword
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx SEARCH hello VSIM vector_query method HNSW ef_runtime 100 LIMIT 0 10");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify default RRF scoring type was set
  ASSERT_EQ(scoringCtx.scoringType, HYBRID_SCORING_RRF);
  ASSERT_EQ(scoringCtx.rrfCtx.k, 1);
  ASSERT_EQ(scoringCtx.rrfCtx.window, 20);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testComplexSingleLineCommand) {
  QueryError status = {QueryErrorCode(0)};

  // Example of a complex command in a single line
  RMCK::ArgvList args(ctx, "FT.HYBRID testidx SEARCH hello VSIM @vector_field method HNSW k 10 "
                            " COMBINE LINEAR 2 search 0.7 vector 0.3 SORTBY 1 @score DESC LIMIT 0 20");

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify LINEAR scoring type was set
  ASSERT_EQ(scoringCtx.scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(scoringCtx.linearCtx.numWeights, 2);
  ASSERT_TRUE(scoringCtx.linearCtx.linearWeights != NULL);
  ASSERT_DOUBLE_EQ(scoringCtx.linearCtx.linearWeights[0], 0.7);
  ASSERT_DOUBLE_EQ(scoringCtx.linearCtx.linearWeights[1], 0.3);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testMultiLineCommand) {
  QueryError status = {QueryErrorCode(0)};

  // Example of how to write multi-line commands for better readability
  auto command =
    "FT.HYBRID testidx "
    "SEARCH hello "
    "VSIM @vector_field method HNSW k 10 "
    "COMBINE LINEAR 2 search 0.7 vector 0.3 "
    "SORTBY 1 @score DESC "
    "LIMIT 0 20";

  RMCK::ArgvList args(ctx, command);

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), sctx, &hybridParams, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify LINEAR scoring type was set
  ASSERT_EQ(scoringCtx.scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(scoringCtx.linearCtx.numWeights, 2);
  ASSERT_TRUE(scoringCtx.linearCtx.linearWeights != NULL);
  ASSERT_DOUBLE_EQ(scoringCtx.linearCtx.linearWeights[0], 0.7);
  ASSERT_DOUBLE_EQ(scoringCtx.linearCtx.linearWeights[1], 0.3);

  // Clean up
  HybridRequest_Free(result);
}
