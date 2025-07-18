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
#include "src/rmalloc.h"

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

TEST_F(ParseHybridTest, testBasicValidInput) {
  QueryError status = {QueryErrorCode(0)};

  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "world");

  // Create a fresh sctx for this test since parseHybridRequest takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify default scoring type is RRF
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_RRF);
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.k, 1);
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.window, 20);

  // parseHybridRequest calls AREQ_ApplyContext which takes ownership of test_sctx
  // No need to free test_sctx as it's now owned by the result

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testMissingSearchParameter) {
  QueryError status = {QueryErrorCode(0)};

  // Missing SEARCH parameter: FT.HYBRID <index> VSIM @vector_field
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "VSIM", "@vector_field");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

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

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

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

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

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

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // // Debug: Print error details if parsing failed
  // if (!result) {
  //   printf("Parsing failed: code=%d, detail='%s'\n", status.code, status.detail ? status.detail : "NULL");
  //   fflush(stdout);
  // }

  // Verify the request was parsed successfully
  EXPECT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify LINEAR scoring type was set
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(result->hybridParams->scoringCtx->linearCtx.numWeights, 2);
  ASSERT_TRUE(result->hybridParams->scoringCtx->linearCtx.linearWeights != NULL);
  ASSERT_DOUBLE_EQ(result->hybridParams->scoringCtx->linearCtx.linearWeights[0], 0.7);
  ASSERT_DOUBLE_EQ(result->hybridParams->scoringCtx->linearCtx.linearWeights[1], 0.3);

  // Clean up
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

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify RRF scoring type was set
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_RRF);
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.k, 1);
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.window, 20);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testInvalidSearchAfterSearch) {
  QueryError status = {QueryErrorCode(0)};

  // Test invalid syntax: FT.HYBRID <index> SEARCH hello SEARCH world (should fail)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "SEARCH", "world");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

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

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify RRF scoring type was set (meaning COMBINE was found and parsed)
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_RRF);
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.k, 1);
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.window, 20);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testVectorParameterAdvancingToLimit) {
  QueryError status = {QueryErrorCode(0)};

  // Test that vector parameters are properly skipped until LIMIT keyword
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector_query", "method", "HNSW", "ef_runtime", "100", "LIMIT", "0", "10");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify default RRF scoring type was set
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_RRF);
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.k, 1);
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.window, 20);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testComplexSingleLineCommand) {
  QueryError status = {QueryErrorCode(0)};

  // Example of a complex command in a single line
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector_field", "method", "HNSW", "k", "10",
                            "COMBINE", "LINEAR", "0.7", "0.3", "SORTBY", "1", "@score", "LIMIT", "0", "20");

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Debug: Print error details if parsing failed
  if (!result) {
    printf("Parsing failed: code=%d, detail='%s'\n", status.code, status.detail ? status.detail : "NULL");
    fflush(stdout);
  }

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify LINEAR scoring type was set
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(result->hybridParams->scoringCtx->linearCtx.numWeights, 2);
  ASSERT_TRUE(result->hybridParams->scoringCtx->linearCtx.linearWeights != NULL);
  ASSERT_DOUBLE_EQ(result->hybridParams->scoringCtx->linearCtx.linearWeights[0], 0.7);
  ASSERT_DOUBLE_EQ(result->hybridParams->scoringCtx->linearCtx.linearWeights[1], 0.3);

  // Clean up
  HybridRequest_Free(result);
}

TEST_F(ParseHybridTest, testMultiLineCommand) {
  QueryError status = {QueryErrorCode(0)};

  // Example of how to write multi-line commands for better readability
  std::string command =
    "FT.HYBRID " + index_name + " "
    "SEARCH hello "
    "VSIM @vector_field method HNSW k 10 "
    "COMBINE LINEAR 0.7 0.3 "
    "SORTBY 1 @score "
    "LIMIT 0 20";

  RMCK::ArgvList args(ctx, command.c_str());

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  // Debug: Print error details if parsing failed
  if (!result) {
    printf("Parsing failed: code=%d, detail='%s'\n", status.code, status.detail ? status.detail : "NULL");
    fflush(stdout);
  }

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify LINEAR scoring type was set
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(result->hybridParams->scoringCtx->linearCtx.numWeights, 2);
  ASSERT_TRUE(result->hybridParams->scoringCtx->linearCtx.linearWeights != NULL);
  ASSERT_DOUBLE_EQ(result->hybridParams->scoringCtx->linearCtx.linearWeights[0], 0.7);
  ASSERT_DOUBLE_EQ(result->hybridParams->scoringCtx->linearCtx.linearWeights[1], 0.3);

  // Clean up
  HybridRequest_Free(result);
}
