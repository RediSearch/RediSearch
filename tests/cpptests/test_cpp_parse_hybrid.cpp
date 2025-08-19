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
#include "src/hybrid/hybrid_scoring.h"
#include "src/hybrid/vector_query_utils.h"
#include "src/spec.h"
#include "src/search_ctx.h"
#include "src/query_error.h"
#include "src/rmalloc.h"
#include "src/index.h"
#include "src/aggregate/aggregate.h"
#include "src/vector_index.h"
#include "VecSim/query_results.h"
#include "info/global_stats.h"

// Macro for BLOB data that all tests using $BLOB should use
#define TEST_BLOB_DATA "AQIDBAUGBwgJCg=="

class ParseHybridTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx;
  IndexSpec *spec;
  std::string index_name;
  HybridRequest *result;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);

    // Initialize pointers to NULL
    spec = NULL;

    // Generate a unique index name for each test to avoid conflicts
    const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();
    index_name = std::string("test_index_") + test_info->test_case_name() + "_" + test_info->name();

    // Create a simple index for testing
    QueryError qerr = {QueryErrorCode(0)};
    RMCK::ArgvList args(ctx, "FT.CREATE", index_name.c_str(), "ON", "HASH",
                        "SCHEMA", "title", "TEXT", "content", "TEXT", "vector", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "COSINE");
    spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
    if (!spec) {
      printf("Failed to create index '%s': code=%d, detail='%s'\n",
             index_name.c_str(), qerr.code, qerr.detail ? qerr.detail : "NULL");
      QueryError_ClearError(&qerr);
    }
    ASSERT_TRUE(spec);
    result = nullptr;
  }

  void TearDown() override {
    // Free the result if it was set during the test
    if (result) {
      HybridScoringContext_Free(result->hybridParams->scoringCtx);
      HybridRequest_Free(result);
    }
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = NULL;
    }
  }

  /**
   * Helper function to parse and validate hybrid command with common boilerplate.
   * Handles initialization, parsing, validation, and stores result in member variable.
   *
   * @param args The command arguments to parse
   * @return Pointer to the parsed HybridRequest (also stored in member variable)
   */
  HybridRequest* parseCommand(RMCK::ArgvList& args) {
    QueryError status = {QueryErrorCode(0)};

    RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
    EXPECT_TRUE(test_sctx != NULL) << "Failed to create search context";

    result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

    EXPECT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");
    EXPECT_TRUE(result != nullptr) << "parseHybridCommand returned NULL";

    return result;
  }

  // Helper function to test error cases with less boilerplate
  void testErrorCode(RMCK::ArgvList& args, QueryErrorCode expected_code, const char* expected_detail);

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
  ASSERT_DOUBLE_EQ(result->hybridParams->scoringCtx->rrfCtx.k, K); \
  ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.window, Window); \
}


TEST_F(ParseHybridTest, testBasicValidInput) {
  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify default scoring type is RRF
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_K, HYBRID_DEFAULT_WINDOW);

  // Verify timeout is set to default
  ASSERT_EQ(result->requests[0]->reqConfig.queryTimeoutMS, 500);
  ASSERT_EQ(result->requests[1]->reqConfig.queryTimeoutMS, 500);

  // Verify dialect is set to default
  ASSERT_EQ(result->requests[0]->reqConfig.dialectVersion, 1);
  ASSERT_EQ(result->requests[1]->reqConfig.dialectVersion, 1);
}

TEST_F(ParseHybridTest, testValidInputWithParams) {
  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "@title:($param1)", "VSIM", "@vector", TEST_BLOB_DATA,
                      "PARAMS", "2", "param1", "hello", "DIALECT", "2");

  parseCommand(args);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify default scoring type is RRF
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_K, HYBRID_DEFAULT_WINDOW);

  // Verify timeout is set to default
  ASSERT_EQ(result->requests[0]->reqConfig.queryTimeoutMS, 500);
  ASSERT_EQ(result->requests[1]->reqConfig.queryTimeoutMS, 500);

  // Verify dialect is set to default
  ASSERT_EQ(result->requests[0]->reqConfig.dialectVersion, 2);
  ASSERT_EQ(result->requests[1]->reqConfig.dialectVersion, 2);
}

TEST_F(ParseHybridTest, testValidInputWithReqConfig) {
  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "TIMEOUT", "240", "DIALECT", "2");

  parseCommand(args);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify default scoring type is RRF
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_K, HYBRID_DEFAULT_WINDOW);

  // Verify timeout is set correctly
  ASSERT_EQ(result->requests[0]->reqConfig.queryTimeoutMS, 240);
  ASSERT_EQ(result->requests[1]->reqConfig.queryTimeoutMS, 240);

  // Verify dialect is set correctly
  ASSERT_EQ(result->requests[0]->reqConfig.dialectVersion, 2);
  ASSERT_EQ(result->requests[1]->reqConfig.dialectVersion, 2);
}



TEST_F(ParseHybridTest, testWithCombineLinear) {
  // Test with LINEAR combine method
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "LINEAR", "0.7", "0.3", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify LINEAR scoring type was set
  assertLinearScoringCtx(0.7, 0.3);
}

TEST_F(ParseHybridTest, testWithCombineRRF) {
  // Test with RRF combine method
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify BLOB parameter was correctly resolved
  AREQ* vecReq = result->requests[1];
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify the vector data in the AST
  const char* expectedBlob = TEST_BLOB_DATA;
  size_t expectedBlobLen = strlen(expectedBlob);
  ASSERT_TRUE(vecReq->ast.root->vn.vq->knn.vector != NULL);
  ASSERT_EQ(vecReq->ast.root->vn.vq->knn.vecLen, expectedBlobLen);
  ASSERT_EQ(memcmp(vecReq->ast.root->vn.vq->knn.vector, expectedBlob, expectedBlobLen), 0);

  // Verify RRF scoring type was set
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_K, HYBRID_DEFAULT_WINDOW);
}

TEST_F(ParseHybridTest, testWithCombineRRFWithK) {
  // Test with RRF combine method with explicit K parameter
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "2", "K", "1.5", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify RRF scoring type was set with custom K value
  assertRRFScoringCtx(1.5, HYBRID_DEFAULT_WINDOW);

  // Verify hasExplicitWindow flag is false (WINDOW not specified)
  ASSERT_FALSE(result->hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(ParseHybridTest, testWithCombineRRFWithWindow) {
  // Test with RRF combine method with explicit WINDOW parameter
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "2", "WINDOW", "25", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify RRF scoring type was set with custom WINDOW value
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_K, 25);

  // Verify hasExplicitWindow flag is true (WINDOW was specified)
  ASSERT_TRUE(result->hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(ParseHybridTest, testWithCombineRRFWithKAndWindow) {
  // Test with RRF combine method with both K and WINDOW parameters
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "4", "K", "160", "WINDOW", "25", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify RRF scoring type was set with both custom K and WINDOW values
  assertRRFScoringCtx(160, 25);

  // Verify hasExplicitWindow flag is true (WINDOW was specified)
  ASSERT_TRUE(result->hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(ParseHybridTest, testWithCombineRRFWithFloatK) {
  // Test with RRF combine method with floating-point K parameter
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "2", "K", "1.5", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify RRF scoring type was set with custom floating-point K value
  assertRRFScoringCtx(1.5, HYBRID_DEFAULT_WINDOW);

  // Verify hasExplicitWindow flag is false (WINDOW was not specified)
  ASSERT_FALSE(result->hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}


TEST_F(ParseHybridTest, testComplexSingleLineCommand) {
  // Example of a complex command in a single line
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "10",
                            "COMBINE", "LINEAR", "0.65", "0.35", "SORTBY", "1", "@score", "LIMIT", "0", "20", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify LINEAR scoring type was set
  assertLinearScoringCtx(0.65, 0.35);
}

TEST_F(ParseHybridTest, testExplicitWindowAndLimitWithImplicitK) {
  // Test with explicit WINDOW and LIMIT but no explicit K
  // WINDOW should take its explicit value (30), KNN K should follow LIMIT (15)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                            "COMBINE", "RRF", "2", "WINDOW", "30", "LIMIT", "0", "15", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify RRF scoring type was set with explicit WINDOW value (30), not LIMIT fallback
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_K, 30);

  // Verify hasExplicitWindow flag is true (WINDOW was specified)
  ASSERT_TRUE(result->hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);

  // Verify KNN K follows LIMIT value (15) since K was not explicitly set
  AREQ* vecReq = result->requests[1];
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  VectorQuery *vq = vecReq->ast.root->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 15);  // Should follow LIMIT value, not default
}

TEST_F(ParseHybridTest, testSortBy0DisablesImplicitSort) {
  // Test SORTBY 0 to disable implicit sorting
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "SORTBY", "0");

  parseCommand(args);

  // Verify that an arrange step was not created
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&result->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep == NULL);
}

TEST_F(ParseHybridTest, testSortByFieldDoesNotDisableImplicitSort) {
  // Test SORTBY with actual field (not 0) - should not disable implicit sorting
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "SORTBY", "1", "@score");

  parseCommand(args);

  // Verify that an arrange step was created with normal sorting (not noSort)
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&result->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep != NULL);
  const PLN_ArrangeStep *arng = (const PLN_ArrangeStep *)arrangeStep;
  ASSERT_TRUE(arng->sortKeys != NULL);

  // Verify default RRF scoring type was set
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_K, HYBRID_DEFAULT_WINDOW);
}

TEST_F(ParseHybridTest, testNoSortByDoesNotDisableImplicitSort) {
  // Test without SORTBY - should not disable implicit sorting (default behavior)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify that no arrange step exists (so implicit sorting will be applied)
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&result->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep == NULL);

  // Verify default RRF scoring type was set
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_K, HYBRID_DEFAULT_WINDOW);
}

// Tests for parseVectorSubquery functionality (VSIM tests)

TEST_F(ParseHybridTest, testVsimBasicKNNWithFilter) {
  // Parse hybrid request
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "EF_RUNTIME", "4", "FILTER", "@title:hello", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for KNN query
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance); // Vector queries always have this flag
  ASSERT_TRUE(vn->opts.distField == NULL); // No YIELD_DISTANCE_AS specified

  // Verify parameters
  ASSERT_EQ(QueryNode_NumParams(vn), 1);
  ASSERT_STREQ(vn->params[0].name, "BLOB");
  ASSERT_EQ(vn->params[0].len, 4);
  ASSERT_EQ(vn->params[0].type, PARAM_VEC);
  ASSERT_STREQ(*(char**)vn->params[0].target, TEST_BLOB_DATA);
  ASSERT_EQ(*vn->params[0].target_len, strlen(TEST_BLOB_DATA));
  ASSERT_EQ(vn->params[0].sign, 0);

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  // TODO: Remove this once we support YIELD_DISTANCE_AS (not part of phase 1)
  // ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_TRUE(vq->scoreField == NULL);
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 10);
  ASSERT_EQ(vq->knn.order, BY_SCORE);

  // verify the filter child
  ASSERT_TRUE(vn->children != NULL);
  ASSERT_EQ(vn->children[0]->type, QN_UNION);
  ASSERT_EQ(vn->children[0]->children[0]->type, QN_TOKEN); //hello
  ASSERT_STREQ(vn->children[0]->children[0]->tn.str, "hello");
  ASSERT_EQ(vn->children[0]->children[1]->type, QN_TOKEN); //+hello
  ASSERT_STREQ(vn->children[0]->children[1]->tn.str, "+hello");
}

TEST_F(ParseHybridTest, testVsimKNNWithEFRuntime) {
  // Parse hybrid request
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "EF_RUNTIME", "80", "FILTER", "@title:hello", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for KNN query with EF_RUNTIME
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance); // Vector queries always have this flag
  ASSERT_TRUE(vn->opts.distField == NULL); // No YIELD_DISTANCE_AS specified

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  // TODO: Remove this once we support YIELD_DISTANCE_AS (not part of phase 1)
  // ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_TRUE(vq->scoreField == NULL);
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 10);
  ASSERT_EQ(vq->knn.order, BY_SCORE);

  // Verify EF_RUNTIME parameter is stored in VectorQuery params
  ASSERT_TRUE(vq->params.params != NULL);
  bool foundEfRuntime = false;
  for (size_t i = 0; i < array_len(vq->params.params); i++) {
    if (strcmp(vq->params.params[i].name, "EF_RUNTIME") == 0) {
      ASSERT_STREQ(vq->params.params[i].value, "80");
      foundEfRuntime = true;
      break;
    }
  }
  ASSERT_TRUE(foundEfRuntime);
}

TEST_F(ParseHybridTest, testVsimBasicKNNNoFilter) {
  // Parse hybrid request
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "5", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for basic KNN query without filter
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance); // Vector queries always have this flag
  ASSERT_TRUE(vn->opts.distField == NULL); // No YIELD_DISTANCE_AS specified

  // Verify parameters
  ASSERT_EQ(QueryNode_NumParams(vn), 1);
  ASSERT_STREQ(vn->params[0].name, "BLOB");
  ASSERT_EQ(vn->params[0].type, PARAM_VEC);
  ASSERT_EQ(vn->params[0].sign, 0);

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  // TODO: Remove this once we support YIELD_DISTANCE_AS (not part of phase 1)
  // ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_TRUE(vq->scoreField == NULL);
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 5);
  ASSERT_EQ(vq->knn.order, BY_SCORE);

  // Verify wildcard query is the child of the vector querynode
  ASSERT_TRUE(vn->children != NULL);
  ASSERT_EQ(vn->children[0]->type, QN_WILDCARD);
}


// TODO: Enable this once we support YIELD_DISTANCE_AS (not part of phase 1)
TEST_F(ParseHybridTest, testVsimKNNWithYieldDistanceOnly) {
  GTEST_SKIP() << "Skipping YIELD_DISTANCE_AS test";
  // Parse hybrid request
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "8", "YIELD_DISTANCE_AS", "distance_score", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for KNN query with YIELD_DISTANCE_AS only
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
  ASSERT_STREQ(vn->opts.distField, "distance_score");

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 8);
  ASSERT_EQ(vq->knn.order, BY_SCORE);
}

TEST_F(ParseHybridTest, testVsimRangeBasic) {
  // Parse hybrid request
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "2", "RADIUS", "0.5", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for basic RANGE query
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance); // Vector queries always have this flag
  ASSERT_TRUE(vn->opts.distField == NULL); // No YIELD_DISTANCE_AS specified

  // Verify parameters
  ASSERT_EQ(QueryNode_NumParams(vn), 1);
  ASSERT_STREQ(vn->params[0].name, "BLOB");
  ASSERT_EQ(vn->params[0].type, PARAM_VEC);
  ASSERT_EQ(vn->params[0].sign, 0);

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  // TODO: Remove this once we support YIELD_DISTANCE_AS (not part of phase 1)
  // ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_TRUE(vq->scoreField == NULL);
  ASSERT_EQ(vq->type, VECSIM_QT_RANGE);
  ASSERT_EQ(vq->range.radius, 0.5);
  ASSERT_EQ(vq->range.order, BY_SCORE);

  // Verify BLOB parameter was correctly resolved (parameter resolution test)
  const char* expectedBlob = TEST_BLOB_DATA;
  size_t expectedBlobLen = strlen(expectedBlob);
  ASSERT_TRUE(vq->range.vector != NULL);
  ASSERT_EQ(vq->range.vecLen, expectedBlobLen);
  ASSERT_EQ(memcmp(vq->range.vector, expectedBlob, expectedBlobLen), 0);
}

TEST_F(ParseHybridTest, testVsimRangeWithEpsilon) {
  // Parse hybrid request
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.8", "EPSILON", "0.01", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for RANGE query with EPSILON
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  // TODO: Remove this once we support YIELD_DISTANCE_AS (not part of phase 1)
  // ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_TRUE(vq->scoreField == NULL);
  ASSERT_EQ(vq->type, VECSIM_QT_RANGE);
  ASSERT_EQ(vq->range.radius, 0.8);
  ASSERT_EQ(vq->range.order, BY_SCORE);

  // Verify BLOB parameter was correctly resolved (parameter resolution test)
  const char* expectedBlob = TEST_BLOB_DATA;
  size_t expectedBlobLen = strlen(expectedBlob);
  ASSERT_TRUE(vq->range.vector != NULL);
  ASSERT_EQ(vq->range.vecLen, expectedBlobLen);
  ASSERT_EQ(memcmp(vq->range.vector, expectedBlob, expectedBlobLen), 0);

  // Verify EPSILON parameter is stored in VectorQuery params
  ASSERT_TRUE(vq->params.params != NULL);
  bool foundEpsilon = false;
  for (size_t i = 0; i < array_len(vq->params.params); i++) {
    if (strcmp(vq->params.params[i].name, "EPSILON") == 0) {
      ASSERT_STREQ(vq->params.params[i].value, "0.01");
      foundEpsilon = true;
      break;
    }
  }
  ASSERT_TRUE(foundEpsilon);
}

TEST_F(ParseHybridTest, testDirectVectorSyntax) {
  // Test with direct vector data (not parameter)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "KNN", "2", "K", "5");

  parseCommand(args);

  AREQ* vecReq = result->requests[1];

  // Test the AST root
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(QueryNode_NumParams(vn), 0);  // No parameters for direct vector data

  // Verify VectorQuery structure in the AST
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  // TODO: Remove this once we support YIELD_DISTANCE_AS (not part of phase 1)
  // ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_TRUE(vq->scoreField == NULL);
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 5);
  ASSERT_EQ(vq->knn.order, BY_SCORE);

  // Verify vector data is directly assigned (not through parameter resolution)
  ASSERT_TRUE(vq->knn.vector != NULL);
  ASSERT_STREQ((const char*)vq->knn.vector, TEST_BLOB_DATA);
  ASSERT_EQ(vq->knn.vecLen, strlen(TEST_BLOB_DATA));
}

TEST_F(ParseHybridTest, testVsimInvalidFilterWeight) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "FILTER","@title:(foo bar)=> {$weight: 2.0}" );
  testErrorCode(args, QUERY_EHYBRID_VSIM_FILTER_INVALID_WEIGHT, "Weight attributes are not allowed in FT.HYBRID VSIM subquery FILTER");
}

TEST_F(ParseHybridTest, testVsimKNNYieldDistanceAsNotSupported) {
  // Test YIELD_DISTANCE_AS in KNN clause - should fail in phase 1
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "YIELD_DISTANCE_AS", "dist", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EHYBRID_HYBRID_ALIAS, "Alias is not allowed in FT.HYBRID VSIM");
}

TEST_F(ParseHybridTest, testVsimRangeYieldDistanceAsNotSupported) {
  // Test YIELD_DISTANCE_AS in RANGE clause - should fail in phase 1
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "YIELD_DISTANCE_AS", "dist", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EHYBRID_HYBRID_ALIAS, "Alias is not allowed in FT.HYBRID VSIM");
}

// Helper function to test error cases with less boilerplate
void ParseHybridTest::testErrorCode(RMCK::ArgvList& args, QueryErrorCode expected_code, const char* expected_detail) {
  QueryError status = {QueryErrorCode(0)};

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, expected_code);
  ASSERT_STREQ(status.detail, expected_detail);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testVsimInvalidFilterVectorField) {
  // Setup: Dialect 2 is required for vector queries
  unsigned int previousDialectVersion = RSGlobalConfig.requestConfigParams.dialectVersion;
  SET_DIALECT(RSGlobalConfig.requestConfigParams.dialectVersion, 2);

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "FILTER", "@vector:[VECTOR_RANGE 0.01 $BLOB]", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EHYBRID_VSIM_FILTER_INVALID_QUERY, "Vector queries are not allowed in FT.HYBRID VSIM subquery FILTER");

  // Teardown: Restore previous dialect version
  SET_DIALECT(RSGlobalConfig.requestConfigParams.dialectVersion, previousDialectVersion);
}

// ============================================================================
// ERROR HANDLING TESTS - All tests using the testErrorCode helper function
// ============================================================================

// Basic parsing error tests
TEST_F(ParseHybridTest, testMissingSearchParameter) {
  // Missing SEARCH parameter: FT.HYBRID <index> VSIM @vector_field
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "VSIM", "vector_field");
  testErrorCode(args, QUERY_ESYNTAX, "SEARCH parameter is required");
}

TEST_F(ParseHybridTest, testMissingQueryStringAfterSearch) {
  // Missing query string after SEARCH: FT.HYBRID <index> SEARCH
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH");
  testErrorCode(args, QUERY_EPARSEARGS, "No query string provided for SEARCH");
}

TEST_F(ParseHybridTest, testMissingSecondSearchParameter) {
  // Missing second search parameter: FT.HYBRID <index> SEARCH hello
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello");
  testErrorCode(args, QUERY_ESYNTAX, "VSIM parameter is required");
}

TEST_F(ParseHybridTest, testInvalidSearchAfterSearch) {
  // Test invalid syntax: FT.HYBRID <index> SEARCH hello SEARCH world (should fail)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "SEARCH", "world");
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown parameter `SEARCH` in SEARCH");
}

// VSIM parsing error tests
TEST_F(ParseHybridTest, testVsimMissingVectorField) {
  // Test missing vector field name after VSIM
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM");
  testErrorCode(args, QUERY_ESYNTAX, "Missing vector field name");
}

TEST_F(ParseHybridTest, testVsimMissingVectorParameter) {
  // Test missing vector parameter after field name
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector");
  testErrorCode(args, QUERY_ESYNTAX, "Missing vector parameter");
}

TEST_F(ParseHybridTest, testVsimVectorFieldMissingAtPrefix) {
  // Test vector field name without @ prefix - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "2", "K", "10", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ESYNTAX, "Missing @ prefix for vector field name");
}

// Parameter parsing error tests
TEST_F(ParseHybridTest, testBlobWithoutParams) {
  // Test using $BLOB without PARAMS section - should fail
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "10");
  testErrorCode(args, QUERY_ENOPARAM, "No such parameter `BLOB`");
}

// KNN parsing error tests
TEST_F(ParseHybridTest, testKNNMissingParameterCount) {
  // Test KNN without parameter count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN");
  testErrorCode(args, QUERY_ESYNTAX, "Missing parameter count");
}

TEST_F(ParseHybridTest, testVsimKNNOddParamCount) {
  // Test KNN with count=1 (odd count, missing K value)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "1", "K", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Invalid parameter count");
}

TEST_F(ParseHybridTest, testKNNZeroParameterCount) {
  // Test KNN with zero parameter count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "0");
  testErrorCode(args, QUERY_EPARSEARGS, "Invalid parameter count");
}

TEST_F(ParseHybridTest, testVsimSubqueryMissingK) {
  // Test KNN without K parameter
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "EF_RUNTIME", "100", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Missing K parameter");
}

TEST_F(ParseHybridTest, testKNNInvalidKValue) {
  // Test KNN with invalid K value (non-numeric)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "invalid", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ESYNTAX, "Invalid K value");
}

TEST_F(ParseHybridTest, testKNNNegativeKValue) {
  // Test KNN with negative K value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "-1", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ESYNTAX, "Invalid K value");
}

TEST_F(ParseHybridTest, testKNNZeroKValue) {
  // Test KNN with zero K value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "0", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ESYNTAX, "Invalid K value");
}

TEST_F(ParseHybridTest, testVsimKNNDuplicateK) {
  // Test KNN with duplicate K parameters
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "K", "20", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate K parameter");
}

TEST_F(ParseHybridTest, testVsimKNNDuplicateEFRuntime) {
  // Test KNN with duplicate EF_RUNTIME parameters
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "6", "K", "10", "EF_RUNTIME", "100", "EF_RUNTIME", "200", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate EF_RUNTIME parameter");
}


// TODO: Enable this once we support YIELD_DISTANCE_AS (not part of phase 1)
TEST_F(ParseHybridTest, testKNNDuplicateYieldDistanceAs) {
  GTEST_SKIP() << "Skipping YIELD_DISTANCE_AS test";
  // Test KNN with duplicate YIELD_DISTANCE_AS parameters
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "6", "K", "10", "YIELD_DISTANCE_AS", "dist1", "YIELD_DISTANCE_AS", "dist2", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate YIELD_DISTANCE_AS parameter");
}

TEST_F(ParseHybridTest, testVsimKNNWithEpsilon) {
  // Test KNN with EPSILON (should be RANGE-only)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "EPSILON", "0.01", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown parameter `EPSILON` in KNN");
}

TEST_F(ParseHybridTest, testVsimSubqueryWrongParamCount) {
  // Test with wrong parameter count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "\"hello\"", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "FILTER", "@text:hello", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown parameter `FILTER` in KNN");
}

// RANGE parsing error tests
TEST_F(ParseHybridTest, testRangeMissingParameterCount) {
  // Test RANGE without parameter count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE");
  testErrorCode(args, QUERY_ESYNTAX, "Missing parameter count");
}

TEST_F(ParseHybridTest, testVsimRangeOddParamCount) {
  // Test RANGE with count=3 (odd count, missing EPSILON value)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "3", "RADIUS", "0.5", "EPSILON", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Invalid parameter count");
}

TEST_F(ParseHybridTest, testRangeZeroParameterCount) {
  // Test RANGE with zero parameter count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "0");
  testErrorCode(args, QUERY_EPARSEARGS, "Invalid parameter count");
}

TEST_F(ParseHybridTest, testRangeInvalidRadiusValue) {
  // Test RANGE with invalid RADIUS value (non-numeric)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "2", "RADIUS", "invalid", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ESYNTAX, "Invalid RADIUS value");
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateRadius) {
  // Test RANGE with duplicate RADIUS parameters
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "RADIUS", "0.8", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate RADIUS parameter");
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateEpsilon) {
  // Test RANGE with duplicate EPSILON parameters
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "6", "RADIUS", "0.5", "EPSILON", "0.01", "EPSILON", "0.02", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate EPSILON parameter");
}

// TODO: Enable this once we support YIELD_DISTANCE_AS (not part of phase 1)
TEST_F(ParseHybridTest, testRangeDuplicateYieldDistanceAs) {
  GTEST_SKIP() << "Skipping YIELD_DISTANCE_AS test";
  // Test RANGE with duplicate YIELD_DISTANCE_AS parameters
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "6", "RADIUS", "0.5", "YIELD_DISTANCE_AS", "dist1", "YIELD_DISTANCE_AS", "dist2", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate YIELD_DISTANCE_AS parameter");
}

TEST_F(ParseHybridTest, testVsimRangeWithEFRuntime) {
  // Test RANGE with EF_RUNTIME (should be KNN-only)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "EF_RUNTIME", "100", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown parameter `EF_RUNTIME` in RANGE");
}

// NOTE: Invalid parameter values of EF_RUNTIME EPSILON_STRING are NOT validated during parsing.
// The validation happens during query execution in the flow:
// QAST_Iterate() → Query_EvalNode() → NewVectorIterator() → VecSim_ResolveQueryParams()
// These validation tests should be in execution tests, not parsing tests.

TEST_F(ParseHybridTest, testCombineRRFInvalidKValue) {
  // Test RRF with invalid K value (non-numeric)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "2", "K", "invalid", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ESYNTAX, "Invalid K value in RRF");
}
