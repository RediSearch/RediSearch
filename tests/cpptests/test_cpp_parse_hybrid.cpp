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
// #include "src/index.h"
#include "src/aggregate/aggregate.h"
#include "src/vector_index.h"
#include "VecSim/query_results.h"
#include "info/global_stats.h"
#include "src/ext/default.h"

// Macro for BLOB data that all tests using $BLOB should use
#define TEST_BLOB_DATA "AQIDBAUGBwgJCg=="

class ParseHybridTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx;
  IndexSpec *spec;
  std::string index_name;
  HybridRequest *hybridRequest;
  HybridPipelineParams hybridParams;
  ParseHybridCommandCtx result;

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
    QueryError qerr = QueryError_Default();
    RMCK::ArgvList args(ctx, "FT.CREATE", index_name.c_str(), "ON", "HASH",
                        "SCHEMA", "title", "TEXT", "content", "TEXT", "vector", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "COSINE");
    spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
    if (!spec) {
      printf("Failed to create index '%s': code=%d, detail='%s'\n",
             index_name.c_str(), QueryError_GetCode(&qerr), QueryError_GetUserError(&qerr));
      QueryError_ClearError(&qerr);
    }
    ASSERT_TRUE(spec);
    hybridRequest = MakeDefaultHybridRequest(NewSearchCtxC(ctx, index_name.c_str(), true));

    hybridParams = {0};
    result.search = hybridRequest->requests[0];
    result.vector = hybridRequest->requests[1];
    result.tailPlan = &hybridRequest->tailPipeline->ap;
    result.hybridParams = &hybridParams;
    result.reqConfig = &hybridRequest->reqConfig;
    result.cursorConfig = &hybridRequest->cursorConfig;
  }

  void TearDown() override {
    if (hybridRequest) {
      HybridRequest_Free(hybridRequest);
    }
    if (hybridParams.scoringCtx) {
      HybridScoringContext_Free(hybridParams.scoringCtx);
    }
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = NULL;
    }
  }

  // Helper function to find vector node as direct child of PHRASE node (RANGE queries with filters)
  QueryNode* findVectorNodeChild(QueryNode* phraseNode) {
    for (size_t i = 0; i < QueryNode_NumChildren(phraseNode); ++i) {
      QueryNode* child = phraseNode->children[i];
      if (child && child->type == QN_VECTOR) {
        return child;
      }
    }
    return NULL;
  }

  /**
   * Helper function to parse and validate hybrid command with common boilerplate.
   * Handles initialization, parsing, validation, and stores result in member variable.
   *
   * @param args The command arguments to parse
   * @return REDISMODULE_OK if parsing succeeded, REDISMODULE_ERR otherwise
   */
  int parseCommandInternal(RMCK::ArgvList& args) {
    QueryError status = QueryError_Default();
    ArgsCursor ac = {0};
    HybridRequest_InitArgsCursor(hybridRequest, &ac, args, args.size());
    int rc = parseHybridCommand(ctx, &ac, hybridRequest->sctx, &result, &status, true);
    EXPECT_TRUE(QueryError_IsOk(&status)) << "Parse failed: " << QueryError_GetDisplayableError(&status, false);
    return rc;
  }

  // Helper function to test error cases with less boilerplate
  void testErrorCode(RMCK::ArgvList& args, QueryErrorCode expected_code, const char* expected_detail);

};

#define parseCommand(args) ASSERT_EQ(parseCommandInternal(args), REDISMODULE_OK) << "parseCommandInternal failed";


#define assertLinearScoringCtx(Weight0, Weight1) { \
  ASSERT_EQ(result.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR); \
  ASSERT_EQ(result.hybridParams->scoringCtx->linearCtx.numWeights, HYBRID_REQUEST_NUM_SUBQUERIES); \
  ASSERT_TRUE(result.hybridParams->scoringCtx->linearCtx.linearWeights != NULL); \
  ASSERT_DOUBLE_EQ(result.hybridParams->scoringCtx->linearCtx.linearWeights[0], Weight0); \
  ASSERT_DOUBLE_EQ(result.hybridParams->scoringCtx->linearCtx.linearWeights[1], Weight1); \
}

#define assertRRFScoringCtx(Constant, Window) { \
  ASSERT_EQ(result.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_RRF); \
  ASSERT_DOUBLE_EQ(result.hybridParams->scoringCtx->rrfCtx.constant, Constant); \
  ASSERT_EQ(result.hybridParams->scoringCtx->rrfCtx.window, Window); \
}


TEST_F(ParseHybridTest, testBasicValidInput) {
  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify default scoring type is RRF
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_CONSTANT, HYBRID_DEFAULT_WINDOW);

  // Verify timeout is set to default
  ASSERT_EQ(result.search->reqConfig.queryTimeoutMS, 500);
  ASSERT_EQ(result.vector->reqConfig.queryTimeoutMS, 500);

  // Verify dialect is set to default
  ASSERT_EQ(result.search->reqConfig.dialectVersion, 2);
  ASSERT_EQ(result.vector->reqConfig.dialectVersion, 2);
}

TEST_F(ParseHybridTest, testValidInputWithParams) {
  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "@title:($param1)", "VSIM", "@vector", TEST_BLOB_DATA,
                      "PARAMS", "2", "param1", "hello");

  parseCommand(args);

  // Verify default scoring type is RRF
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_CONSTANT, HYBRID_DEFAULT_WINDOW);

  // Verify timeout is set to default
  ASSERT_EQ(result.search->reqConfig.queryTimeoutMS, 500);
  ASSERT_EQ(result.vector->reqConfig.queryTimeoutMS, 500);

  // Verify dialect is set to default
  ASSERT_EQ(result.search->reqConfig.dialectVersion, 2);
  ASSERT_EQ(result.vector->reqConfig.dialectVersion, 2);
}

TEST_F(ParseHybridTest, testValidInputWithReqConfig) {
  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "TIMEOUT", "240");

  parseCommand(args);

  // Verify default scoring type is RRF
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_CONSTANT, HYBRID_DEFAULT_WINDOW);

  // Verify timeout is set correctly
  ASSERT_EQ(result.search->reqConfig.queryTimeoutMS, 240);
  ASSERT_EQ(result.vector->reqConfig.queryTimeoutMS, 240);

  // Verify dialect is set correctly
  ASSERT_EQ(result.search->reqConfig.dialectVersion, 2);
  ASSERT_EQ(result.vector->reqConfig.dialectVersion, 2);
}

TEST_F(ParseHybridTest, testWithCombineLinear) {
  // Test with LINEAR combine method
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify LINEAR scoring type was set
  assertLinearScoringCtx(0.7, 0.3);
}

TEST_F(ParseHybridTest, testWithCombineRRF) {
  // Test with RRF combine method
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify BLOB parameter was correctly resolved
  AREQ* vecReq = result.vector;
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify the vector data in the AST
  const char* expectedBlob = TEST_BLOB_DATA;
  size_t expectedBlobLen = strlen(expectedBlob);
  ASSERT_TRUE(vecReq->ast.root->vn.vq->knn.vector != NULL);
  ASSERT_EQ(vecReq->ast.root->vn.vq->knn.vecLen, expectedBlobLen);
  ASSERT_EQ(memcmp(vecReq->ast.root->vn.vq->knn.vector, expectedBlob, expectedBlobLen), 0);

  // Verify RRF scoring type was set
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_CONSTANT, HYBRID_DEFAULT_WINDOW);
}

TEST_F(ParseHybridTest, testWithCombineRRFWithConstant) {
  // Test with RRF combine method with explicit CONSTANT argument
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
      "COMBINE", "RRF", "2", "CONSTANT", "1.5",
      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify RRF scoring type was set with custom CONSTANT value
  assertRRFScoringCtx(1.5, HYBRID_DEFAULT_WINDOW);

  // Verify hasExplicitWindow flag is false (WINDOW not specified)
  ASSERT_FALSE(result.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(ParseHybridTest, testWithCombineRRFWithWindow) {
  // Test with RRF combine method with explicit WINDOW argument
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "2", "WINDOW", "25", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify RRF scoring type was set with custom WINDOW value
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_CONSTANT, 25);

  // Verify hasExplicitWindow flag is true (WINDOW was specified)
  ASSERT_TRUE(result.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(ParseHybridTest, testWithCombineRRFWithConstantAndWindow) {
  // Test with RRF combine method with both CONSTANT and WINDOW arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
      "COMBINE", "RRF", "4", "CONSTANT", "160", "WINDOW", "25",
      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify RRF scoring type was set with both custom CONSTANT and WINDOW values
  assertRRFScoringCtx(160, 25);

  // Verify hasExplicitWindow flag is true (WINDOW was specified)
  ASSERT_TRUE(result.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(ParseHybridTest, testWithCombineRRFWithFloatConstant) {
  // Test with RRF combine method with floating-point CONSTANT argument
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
      "COMBINE", "RRF", "2", "CONSTANT", "1.5",
      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify RRF scoring type was set with custom floating-point CONSTANT value
  assertRRFScoringCtx(1.5, HYBRID_DEFAULT_WINDOW);

  // Verify hasExplicitWindow flag is false (WINDOW was not specified)
  ASSERT_FALSE(result.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}


TEST_F(ParseHybridTest, testComplexSingleLineCommand) {
  // Example of a complex command in a single line
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "10",
                            "COMBINE", "LINEAR", "4", "ALPHA", "0.65", "BETA", "0.35", "SORTBY", "1", "@score", "LIMIT", "0", "20", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

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

  // Verify RRF scoring type was set with explicit WINDOW value (30), not LIMIT fallback
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_CONSTANT, 30);

  // Verify hasExplicitWindow flag is true (WINDOW was specified)
  ASSERT_TRUE(result.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);

  // Verify KNN K follows LIMIT value (15) since K was not explicitly set
  AREQ* vecReq = result.vector;
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  VectorQuery *vq = vecReq->ast.root->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, HYBRID_DEFAULT_KNN_K);
}

TEST_F(ParseHybridTest, testNOSORTDisablesImplicitSort) {
  // Test SORTBY 0 to disable implicit sorting
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "NOSORT");

  parseCommand(args);

  // Verify that an arrange step was not created
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(result.tailPlan, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep == NULL);
}

TEST_F(ParseHybridTest, testSortByFieldDoesNotDisableImplicitSort) {
  // Test SORTBY with actual field (not 0) - should not disable implicit sorting
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "SORTBY", "1", "@score");

  parseCommand(args);

  // Verify that an arrange step was created with normal sorting (not noSort)
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(result.tailPlan, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep != NULL);
  const PLN_ArrangeStep *arng = (const PLN_ArrangeStep *)arrangeStep;
  ASSERT_TRUE(arng->sortKeys != NULL);

  // Verify default RRF scoring type was set
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_CONSTANT, HYBRID_DEFAULT_WINDOW);
}

TEST_F(ParseHybridTest, testNoSortByWillHaveImplicitSort) {
  // Test without SORTBY - should not disable implicit sorting (default behavior)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA);

  parseCommand(args);

  // Verify that an implicit sort-by-score step was created
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(result.tailPlan, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep != NULL);

  // Verify default RRF scoring type was set
  assertRRFScoringCtx(HYBRID_DEFAULT_RRF_CONSTANT, HYBRID_DEFAULT_WINDOW);
}

// Tests for parseVectorSubquery functionality (VSIM tests)

TEST_F(ParseHybridTest, testVsimBasicKNNWithFilter) {
  // Parse hybrid request
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "EF_RUNTIME", "4", "FILTER", "@title:hello", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result.vector;

  // Verify AST structure for KNN query
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance); // Vector queries always have this flag
  ASSERT_EQ(vn->opts.flags & QueryNode_HybridVectorSubqueryNode, QueryNode_HybridVectorSubqueryNode); // Should be marked as hybrid vector subquery node
  ASSERT_TRUE(vn->opts.distField == NULL); // No YIELD_SCORE_AS specified

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
  ASSERT_TRUE(vq->scoreField != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
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

  AREQ* vecReq = result.vector;

  // Verify AST structure for KNN query with EF_RUNTIME
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance); // Vector queries always have this flag
  ASSERT_TRUE(vn->opts.distField == NULL); // No YIELD_SCORE_AS specified

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  ASSERT_TRUE(vq->scoreField != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
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

  AREQ* vecReq = result.vector;

  // Verify AST structure for basic KNN query without filter
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance); // Vector queries always have this flag
  ASSERT_TRUE(vn->opts.distField == NULL); // No YIELD_SCORE_AS specified

  // Verify parameters
  ASSERT_EQ(QueryNode_NumParams(vn), 1);
  ASSERT_STREQ(vn->params[0].name, "BLOB");
  ASSERT_EQ(vn->params[0].type, PARAM_VEC);
  ASSERT_EQ(vn->params[0].sign, 0);

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  ASSERT_TRUE(vq->scoreField != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 5);
  ASSERT_EQ(vq->knn.order, BY_SCORE);

  // Verify wildcard query is the child of the vector querynode
  ASSERT_TRUE(vn->children != NULL);
  ASSERT_EQ(vn->children[0]->type, QN_WILDCARD);
}

TEST_F(ParseHybridTest, testVsimKNNWithYieldDistanceOnly) {
  // YIELD_SCORE_AS should work
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "8", "YIELD_SCORE_AS", "distance_score", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result.vector;

  // Verify AST structure for KNN query with YIELD_SCORE_AS
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
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 8);
  ASSERT_EQ(vq->knn.order, BY_SCORE);
}

TEST_F(ParseHybridTest, testVsimRangeBasic) {
  // Parse hybrid request
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "2", "RADIUS", "0.5", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result.vector;

  // Verify AST structure for basic RANGE query with filter
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_PHRASE); // Root should be PHRASE for RANGE queries with filters

  QueryNode *vn = findVectorNodeChild(vecReq->ast.root);
  ASSERT_TRUE(vn != NULL) << "Vector node not found as child of PHRASE";

  // Verify QueryNode structure
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance); // Vector queries always have this flag
  ASSERT_EQ(vn->opts.flags & QueryNode_HybridVectorSubqueryNode, QueryNode_HybridVectorSubqueryNode); // Should be marked as hybrid vector subquery node
  ASSERT_TRUE(vn->opts.distField == NULL); // No YIELD_SCORE_AS specified

  // Verify parameters
  ASSERT_EQ(QueryNode_NumParams(vn), 1);
  ASSERT_STREQ(vn->params[0].name, "BLOB");
  ASSERT_EQ(vn->params[0].type, PARAM_VEC);
  ASSERT_EQ(vn->params[0].sign, 0);

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  ASSERT_TRUE(vq->scoreField != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
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

  AREQ* vecReq = result.vector;

  // Verify AST structure for RANGE query with EPSILON
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_PHRASE); // Root should be PHRASE for RANGE queries with filters

  QueryNode *vn = findVectorNodeChild(vecReq->ast.root);
  ASSERT_TRUE(vn != NULL) << "Vector node not found as child of PHRASE";

  // Verify QueryNode structure
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
  ASSERT_EQ(vn->opts.flags & QueryNode_HybridVectorSubqueryNode, QueryNode_HybridVectorSubqueryNode); // Should be marked as hybrid vector subquery node

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  ASSERT_TRUE(vq->scoreField != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
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

TEST_F(ParseHybridTest, testExternalCommandWith_NUM_SSTRING) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
        "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "_NUM_SSTRING");

  QueryError status = QueryError_Default();
  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hybridRequest, &ac, args, args.size());
  parseHybridCommand(ctx, &ac, hybridRequest->sctx, &result, &status, false);
  EXPECT_EQ(QueryError_GetCode(&status), QUERY_EPARSEARGS) << "Should fail as external command";
  QueryError_ClearError(&status);

  // Clean up any partial allocations from the failed parse
  if (result.vector && result.vector->ast.root) {
    QAST_Destroy(&result.vector->ast);
    result.vector->ast.root = NULL;
  }
}

TEST_F(ParseHybridTest, testInternalCommandWith_NUM_SSTRING) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
        "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "_NUM_SSTRING");

  QueryError status = QueryError_Default();

  ASSERT_FALSE(result.hybridParams->aggregationParams.common.reqflags & QEXEC_F_TYPED);
  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hybridRequest, &ac, args, args.size());
  parseHybridCommand(ctx, &ac, hybridRequest->sctx, &result, &status, true);
  EXPECT_EQ(QueryError_GetCode(&status), QUERY_OK) << "Should succeed as internal command";
  QueryError_ClearError(&status);

  // Verify _NUM_SSTRING flag is set after parsing
  ASSERT_TRUE(result.hybridParams->aggregationParams.common.reqflags & QEXEC_F_TYPED);
}

TEST_F(ParseHybridTest, testDirectVectorSyntax) {
  // Test with direct vector data (not argument)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "KNN", "2", "K", "5");

  parseCommand(args);

  AREQ* vecReq = result.vector;

  // Test the AST root
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_HybridVectorSubqueryNode, QueryNode_HybridVectorSubqueryNode); // Should be marked as hybrid vector subquery node
  ASSERT_EQ(QueryNode_NumParams(vn), 0);  // No parameters for direct vector data

  // Verify VectorQuery structure in the AST
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  ASSERT_TRUE(vq->scoreField != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 5);
  ASSERT_EQ(vq->knn.order, BY_SCORE);

  // Verify vector data is directly assigned (not through argument resolution)
  ASSERT_TRUE(vq->knn.vector != NULL);
  ASSERT_STREQ((const char*)vq->knn.vector, TEST_BLOB_DATA);
  ASSERT_EQ(vq->knn.vecLen, strlen(TEST_BLOB_DATA));
}

TEST_F(ParseHybridTest, testVsimInvalidFilterWeight) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "FILTER","@title:(foo bar)=> {$weight: 2.0}" );
  testErrorCode(args, QUERY_EWEIGHT_NOT_ALLOWED, "Weight attributes are not allowed in FT.HYBRID VSIM FILTER");
}

// Helper function to test error cases with less boilerplate
void ParseHybridTest::testErrorCode(RMCK::ArgvList& args, QueryErrorCode expected_code, const char* expected_detail) {
  QueryError status = QueryError_Default();

  // Create a fresh sctx for this test
  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hybridRequest, &ac, args, args.size());
  int rc = parseHybridCommand(ctx, &ac, hybridRequest->sctx, &result, &status, true);
  ASSERT_TRUE(rc == REDISMODULE_ERR) << "parsing error: " << QueryError_GetUserError(&status);
  ASSERT_EQ(QueryError_GetCode(&status), expected_code) << "parsing error: " << QueryError_GetUserError(&status);
  ASSERT_STREQ(QueryError_GetUserError(&status), expected_detail) << "parsing error: " << QueryError_GetUserError(&status);

  // Clean up
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testVsimInvalidFilterVectorField) {
  // Setup: Dialect 2 is required for vector queries
  unsigned int previousDialectVersion = RSGlobalConfig.requestConfigParams.dialectVersion;
  SET_DIALECT(RSGlobalConfig.requestConfigParams.dialectVersion, 2);

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "FILTER", "@vector:[VECTOR_RANGE 0.01 $BLOB]", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EVECTOR_NOT_ALLOWED, "Vector expressions are not allowed in FT.HYBRID VSIM FILTER");

  // Teardown: Restore previous dialect version
  SET_DIALECT(RSGlobalConfig.requestConfigParams.dialectVersion, previousDialectVersion);
}

// ============================================================================
// ERROR HANDLING TESTS - All tests using the testErrorCode helper function
// ============================================================================

// Basic parsing error tests
TEST_F(ParseHybridTest, testMissingSearchArgument) {
  // Missing SEARCH argument: FT.HYBRID <index> VSIM @vector_field
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "VSIM", "vector_field");
  testErrorCode(args, QUERY_ESYNTAX, "SEARCH argument is required");
}

TEST_F(ParseHybridTest, testMissingQueryStringAfterSearch) {
  // Missing query string after SEARCH: FT.HYBRID <index> SEARCH
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH");
  testErrorCode(args, QUERY_EPARSEARGS, "No query string provided for SEARCH");
}

TEST_F(ParseHybridTest, testMissingSecondSearchArgument) {
  // Missing second search argument: FT.HYBRID <index> SEARCH hello
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello");
  testErrorCode(args, QUERY_ESYNTAX, "VSIM argument is required");
}

TEST_F(ParseHybridTest, testInvalidSearchAfterSearch) {
  // Test invalid syntax: FT.HYBRID <index> SEARCH hello SEARCH world (should fail)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "SEARCH", "world");
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown argument `SEARCH` in SEARCH");
}

// VSIM parsing error tests
TEST_F(ParseHybridTest, testVsimMissingVectorField) {
  // Test missing vector field name after VSIM
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM");
  testErrorCode(args, QUERY_ESYNTAX, "Missing vector field name");
}

TEST_F(ParseHybridTest, testVsimMissingVectorArgument) {
  // Test missing vector argument after field name
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector");
  testErrorCode(args, QUERY_ESYNTAX, "Missing vector argument");
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
TEST_F(ParseHybridTest, testKNNMissingArgumentCount) {
  // Test KNN without argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN");
  testErrorCode(args, QUERY_EPARSEARGS, "Missing argument count");
}

TEST_F(ParseHybridTest, testVsimKNNOddParamCount) {
  // Test KNN with count=1 (odd count, missing K value)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "1", "K", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ESYNTAX, "Invalid argument count: 1 (must be a positive even number for key/value pairs)");
}

TEST_F(ParseHybridTest, testKNNZeroArgumentCount) {
  // Test KNN with zero argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "0");
  testErrorCode(args, QUERY_ESYNTAX, "Invalid argument count: 0 (must be a positive even number for key/value pairs)");
}

TEST_F(ParseHybridTest, testVsimSubqueryMissingK) {
  // Test KNN without K argument
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "EF_RUNTIME", "100", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Missing required argument K");
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
  // Test KNN with duplicate K arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "K", "20", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate K argument");
}

TEST_F(ParseHybridTest, testVsimKNNDuplicateEFRuntime) {
  // Test KNN with duplicate EF_RUNTIME arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "6", "K", "10", "EF_RUNTIME", "100", "EF_RUNTIME", "200", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate EF_RUNTIME argument");
}


TEST_F(ParseHybridTest, testKNNDuplicateYieldDistanceAs) {
  // Test KNN with duplicate YIELD_SCORE_AS arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "6", "K", "10", "YIELD_SCORE_AS", "dist1", "YIELD_SCORE_AS", "dist2", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate YIELD_SCORE_AS argument");
}

TEST_F(ParseHybridTest, testVsimKNNWithEpsilon) {
  // Test KNN with EPSILON (should be RANGE-only)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "EPSILON", "0.01", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown argument `EPSILON` in KNN");
}

TEST_F(ParseHybridTest, testVsimSubqueryWrongParamCount) {
  // Test with wrong argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "\"hello\"", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "FILTER", "@text:hello", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown argument `FILTER` in KNN");
}

// RANGE parsing error tests
TEST_F(ParseHybridTest, testRangeMissingArgumentCount) {
  // Test RANGE without argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE");
  testErrorCode(args, QUERY_EPARSEARGS, "Missing argument count");
}

TEST_F(ParseHybridTest, testVsimRangeOddParamCount) {
  // Test RANGE with count=3 (odd count, missing EPSILON value)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "3", "RADIUS", "0.5", "EPSILON", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ESYNTAX, "Invalid argument count: 3 (must be a positive even number for key/value pairs)");
}

TEST_F(ParseHybridTest, testRangeZeroArgumentCount) {
  // Test RANGE with zero argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "0");
  testErrorCode(args, QUERY_ESYNTAX, "Invalid argument count: 0 (must be a positive even number for key/value pairs)");
}

TEST_F(ParseHybridTest, testRangeInvalidRadiusValue) {
  // Test RANGE with invalid RADIUS value (non-numeric)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "2", "RADIUS", "invalid", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ESYNTAX, "Invalid RADIUS value");
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateRadius) {
  // Test RANGE with duplicate RADIUS arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "RADIUS", "0.8", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate RADIUS argument");
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateEpsilon) {
  // Test RANGE with duplicate EPSILON arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "6", "RADIUS", "0.5", "EPSILON", "0.01", "EPSILON", "0.02", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate EPSILON argument");
}

TEST_F(ParseHybridTest, testRangeDuplicateYieldDistanceAs) {
  // Test RANGE with duplicate YIELD_SCORE_AS arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "6", "RADIUS", "0.5", "YIELD_SCORE_AS", "dist1", "YIELD_SCORE_AS", "dist2", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EDUPPARAM, "Duplicate YIELD_SCORE_AS argument");
}

TEST_F(ParseHybridTest, testVsimRangeWithEFRuntime) {
  // Test RANGE with EF_RUNTIME (should be KNN-only)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "EF_RUNTIME", "100", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown argument `EF_RUNTIME` in RANGE");
}

// NOTE: Invalid parameter values of EF_RUNTIME EPSILON_STRING are NOT validated during parsing.
// The validation happens during query execution in the flow:
// QAST_Iterate() → Query_EvalNode() → NewVectorIterator() → VecSim_ResolveQueryParams()
// These validation tests should be in execution tests, not parsing tests.

TEST_F(ParseHybridTest, testCombineRRFInvalidConstantValue) {
  // Test RRF with invalid CONSTANT value (non-numeric)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
      "COMBINE", "RRF", "2", "CONSTANT", "invalid",
      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "CONSTANT: Could not convert argument to expected type");
}

TEST_F(ParseHybridTest, testDefaultTextScorerForLinear) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,\
   "COMBINE", "LINEAR", "4", "ALPHA", "0.6", "BETA", "0.4");

  parseCommand(args);
  // No explicit scorer should be set; the default scorer will be used
  ASSERT_EQ(result.search->searchopts.scorerName, nullptr);
}

TEST_F(ParseHybridTest, testExplicitTextScorerForLinear) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "SCORER", "TFIDF", "VSIM", "@vector", TEST_BLOB_DATA,\
   "COMBINE", "LINEAR", "4", "ALPHA", "0.6", "BETA", "0.4");

  parseCommand(args);

  ASSERT_STREQ(result.search->searchopts.scorerName, TFIDF_SCORER_NAME);
}

TEST_F(ParseHybridTest, testDefaultTextScorerForRRF) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,\
   "COMBINE", "RRF", "2", "CONSTANT", "10");

  parseCommand(args);

  // No explicit scorer should be set; the default scorer will be used
  ASSERT_EQ(result.search->searchopts.scorerName, nullptr);
}

TEST_F(ParseHybridTest, testExplicitTextScorerForRRF) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "SCORER", "TFIDF", "VSIM", "@vector", TEST_BLOB_DATA,\
   "COMBINE", "RRF", "2", "CONSTANT", "10");

  parseCommand(args);

  ASSERT_STREQ(result.search->searchopts.scorerName, TFIDF_SCORER_NAME);
}

TEST_F(ParseHybridTest, testLinearPartialWeightsAlpha) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "LINEAR", "2", "ALPHA", "0.6");
  testErrorCode(args, QUERY_ESYNTAX, "Missing value for BETA");
}

TEST_F(ParseHybridTest, testLinearMissingArgs) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "LINEAR", "4", "ALPHA", "0.6");
  testErrorCode(args, QUERY_ESYNTAX, "Not enough arguments in LINEAR, specified 4 but provided only 2");
}

TEST_F(ParseHybridTest, testLinearPartialWeightsBeta) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "LINEAR", "2", "BETA", "0.6");
  testErrorCode(args, QUERY_ESYNTAX, "Missing value for ALPHA");
}

TEST_F(ParseHybridTest, testLinearNegativeArgumentCount) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "LINEAR", "-2", "ALPHA", "0.6", "BETA", "0.4");
  testErrorCode(args, QUERY_EPARSEARGS, "Invalid LINEAR argument count, error: Value is outside acceptable bounds");
}

TEST_F(ParseHybridTest, testLinearMissingArgumentCount) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "LINEAR");
  testErrorCode(args, QUERY_EPARSEARGS, "Missing LINEAR argument count");
}

// Missing parameter value tests
TEST_F(ParseHybridTest, testKNNMissingKValue) {
  // Test KNN with missing K value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "KNN", "2", "K");
  testErrorCode(args, QUERY_EPARSEARGS, "Missing argument value for K");
}

TEST_F(ParseHybridTest, testKNNMissingEFRuntimeValue) {
  // Test KNN with missing EF_RUNTIME value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "KNN", "4", "K", "10", "EF_RUNTIME");
  testErrorCode(args, QUERY_EPARSEARGS, "Missing argument value for EF_RUNTIME");
}

TEST_F(ParseHybridTest, testRangeMissingRadiusValue) {
  // Test RANGE with missing RADIUS value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "RANGE", "2", "RADIUS");
  testErrorCode(args, QUERY_EPARSEARGS, "Missing argument value for RADIUS");
}

TEST_F(ParseHybridTest, testRangeMissingEpsilonValue) {
  // Test RANGE with missing EPSILON value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "RANGE", "4", "RADIUS", "0.5", "EPSILON");
  testErrorCode(args, QUERY_EPARSEARGS, "Missing argument value for EPSILON");
}

TEST_F(ParseHybridTest, testLinearMissingAlphaValue) {
  // Test LINEAR with missing ALPHA value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "LINEAR", "2", "ALPHA");
  testErrorCode(args, QUERY_ESYNTAX, "Not enough arguments in LINEAR, specified 2 but provided only 1");
}

TEST_F(ParseHybridTest, testLinearMissingBetaValue) {
  // Test LINEAR with missing BETA value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "LINEAR", "2", "BETA");
  testErrorCode(args, QUERY_ESYNTAX,"Not enough arguments in LINEAR, specified 2 but provided only 1");
}

TEST_F(ParseHybridTest, testKNNMissingYieldDistanceAsValue) {
  // Test KNN with missing YIELD_SCORE_AS value (early return before CheckEnd)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "KNN", "4", "K", "10", "YIELD_SCORE_AS");
  testErrorCode(args, QUERY_EPARSEARGS, "Missing argument value for YIELD_SCORE_AS");
}

TEST_F(ParseHybridTest, testRangeMissingYieldDistanceAsValue) {
  // Test RANGE with missing YIELD_SCORE_AS value (early return before CheckEnd)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "RANGE", "4", "RADIUS", "0.5", "YIELD_SCORE_AS");
  testErrorCode(args, QUERY_EPARSEARGS, "Missing argument value for YIELD_SCORE_AS");
}

// ============================================================================
// HYBRID CALLBACK ERROR TESTS - Testing error paths in hybrid_callbacks.c
// ============================================================================

// LIMIT callback error tests - These test the actual callback function error paths
TEST_F(ParseHybridTest, testLimitZeroCountWithNonZeroOffset) {
  // Test LIMIT 0 0 vs LIMIT 5 0 - the callback should catch the second case
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "LIMIT", "5", "0");
  testErrorCode(args, QUERY_ELIMIT, "The `offset` of the LIMIT must be 0 when `num` is 0");
}

TEST_F(ParseHybridTest, testLimitInvalidOffset) {
  // Test LIMIT with invalid offset (negative)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "LIMIT", "-1", "10");
  testErrorCode(args, QUERY_EPARSEARGS, "LIMIT offset must be a non-negative integer");
}

TEST_F(ParseHybridTest, testLimitInvalidCount) {
  // Test LIMIT with invalid count (negative)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "LIMIT", "0", "-5");
  testErrorCode(args, QUERY_EPARSEARGS, "LIMIT count must be a non-negative integer");
}

TEST_F(ParseHybridTest, testLimitExceedsMaxResults) {
  // Test LIMIT that exceeds maxResults (default is 1000000)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "LIMIT", "0", "2000000");
  testErrorCode(args, QUERY_ELIMIT, "LIMIT exceeds maximum of 1000000");
}

// SORTBY callback error tests
TEST_F(ParseHybridTest, testSortByMissingFieldName) {
  // Test SORTBY with missing field name (empty args after SORTBY)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "SORTBY");
  testErrorCode(args, QUERY_EPARSEARGS, "SORTBY: Failed to parse the argument count");
}

// PARAMS callback error tests
TEST_F(ParseHybridTest, testParamsOddArgumentCount) {
  // Test PARAMS with odd number of arguments (not key-value pairs)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "PARAMS", "3", "key1", "value1", "key2");
  testErrorCode(args, QUERY_EADDARGS, "Parameters must be specified in PARAM VALUE pairs");
}

TEST_F(ParseHybridTest, testParamsZeroArguments) {
  // Test PARAMS with zero arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "PARAMS", "0");
  testErrorCode(args, QUERY_EPARSEARGS, "PARAMS: Invalid argument count");
}

// WITHCURSOR callback error tests
TEST_F(ParseHybridTest, testWithCursorInvalidMaxIdle) {
  // Test WITHCURSOR with invalid MAXIDLE value (zero)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "WITHCURSOR", "MAXIDLE", "0");
  testErrorCode(args, QUERY_EPARSEARGS, "Bad arguments for MAXIDLE: Value is outside acceptable bounds");
}

TEST_F(ParseHybridTest, testWithCursorInvalidCount) {
  // Test WITHCURSOR with invalid COUNT value (zero)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "WITHCURSOR", "COUNT", "0");
  testErrorCode(args, QUERY_EPARSEARGS, "Bad arguments for COUNT: Value is outside acceptable bounds");
}

// GROUPBY callback error tests
TEST_F(ParseHybridTest, testGroupByNoProperties) {
  // Test GROUPBY with no properties specified
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "GROUPBY");
  testErrorCode(args, QUERY_EPARSEARGS, "GROUPBY: Failed to parse the argument count");
}

TEST_F(ParseHybridTest, testGroupByPropertyMissingAtPrefix) {
  // Test GROUPBY with property missing @ prefix
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "GROUPBY", "1", "title");
  testErrorCode(args, QUERY_EPARSEARGS, "Bad arguments for GROUPBY: Unknown property `title`. Did you mean `@title`?");
}

// APPLY callback error tests
TEST_F(ParseHybridTest, testApplyMissingAsArgument) {
  // Test APPLY with AS but missing alias argument
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "APPLY", "upper(@title)", "AS");
  testErrorCode(args, QUERY_EPARSEARGS, "AS needs argument");
}

// LOAD callback error tests
TEST_F(ParseHybridTest, testLoadInvalidFieldCount) {
  // Test LOAD with invalid field count (non-numeric)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "LOAD", "invalid");
  testErrorCode(args, QUERY_EPARSEARGS, "Bad arguments for LOAD: Expected number of fields or `*`");
}

TEST_F(ParseHybridTest, testLoadInsufficientFields) {
  // Test LOAD with insufficient fields for specified count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "LOAD", "3", "@title");
  testErrorCode(args, QUERY_EPARSEARGS, "Not enough arguments for LOAD");
}

// ============================================================================
// Test not yet supported arguments
// ============================================================================

TEST_F(ParseHybridTest, testCombineRRFWithoutArgument) {
  // Test RANGE with missing YIELD_DISTANCE_AS value (early return before CheckEnd)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "RRF", "0");
  testErrorCode(args, QUERY_EPARSEARGS, "Explicitly specifying RRF requires at least one argument, argument count must be positive");
}

TEST_F(ParseHybridTest, testCombineRRFWithOddArgumentCount) {
  // Test RANGE with missing YIELD_DISTANCE_AS value (early return before CheckEnd)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "RRF", "1", "WINDOW");
  testErrorCode(args, QUERY_EPARSEARGS, "RRF expects pairs of key value arguments, argument count must be an even number");
}

TEST_F(ParseHybridTest, testExplainScore) {
  // Test EXPLAINSCORE - currently should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "EXPLAINSCORE");
  testErrorCode(args, QUERY_EPARSEARGS, "EXPLAINSCORE is not yet supported by FT.HYBRID");
}

// ============================================================================
// DIALECT ERROR TESTS - Testing DIALECT is not supported
// ============================================================================

TEST_F(ParseHybridTest, testDialectInSearchSubquery) {
  // Test DIALECT in SEARCH subquery - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "DIALECT", "2", "VSIM", "@vector", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "DIALECT is not supported in FT.HYBRID or any of its subqueries. Please check the documentation on search-default-dialect configuration.");
}

TEST_F(ParseHybridTest, testDialectInVectorKNNSubquery) {
  // Test DIALECT in vector KNN subquery - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "KNN", "2", "DIALECT", "2", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown argument `DIALECT` in KNN");
}

TEST_F(ParseHybridTest, testDialectInVectorRangeSubquery) {
  // Test DIALECT in vector RANGE subquery - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "RANGE", "2", "DIALECT", "2", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_EPARSEARGS, "Unknown argument `DIALECT` in RANGE");
}

TEST_F(ParseHybridTest, testDialectInTail) {
  // Test DIALECT in tail (after subqueries) - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "DIALECT", "2");
  testErrorCode(args, QUERY_EPARSEARGS, "DIALECT is not supported in FT.HYBRID or any of its subqueries. Please check the documentation on search-default-dialect configuration.");
}


// ============================================================================
// WINDOW ERROR TESTS
// ============================================================================

TEST_F(ParseHybridTest, testCombineRRFNegativeWindow) {
  // Test RRF with negative WINDOW value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "RRF", "2", "WINDOW", "-5");
  testErrorCode(args, QUERY_EPARSEARGS, "WINDOW: Value below minimum");
}

TEST_F(ParseHybridTest, testCombineRRFZeroWindow) {
  // Test RRF with zero WINDOW value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "RRF", "2", "WINDOW", "0");
  testErrorCode(args, QUERY_EPARSEARGS, "WINDOW: Value below minimum");
}

TEST_F(ParseHybridTest, testCombineLinearNegativeWindow) {
  // Test LINEAR with negative WINDOW value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "LINEAR", "6", "ALPHA", "0.6", "BETA", "0.4", "WINDOW", "-10");
  testErrorCode(args, QUERY_EPARSEARGS, "WINDOW: Value below minimum");
}

TEST_F(ParseHybridTest, testCombineLinearZeroWindow) {
  // Test LINEAR with zero WINDOW value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "COMBINE", "LINEAR", "6", "ALPHA", "0.6", "BETA", "0.4", "WINDOW", "0");
  testErrorCode(args, QUERY_EPARSEARGS, "WINDOW: Value below minimum");
}

TEST_F(ParseHybridTest, testSortby0InvalidArgumentCount) {
  // SORTBY requires at least one argument (param count)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "SORTBY", "0");
  testErrorCode(args, QUERY_EPARSEARGS, "SORTBY: Invalid argument count");
}

TEST_F(ParseHybridTest, testSortbyNotEnoughArguments) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "SORTBY", "2", "title");
  testErrorCode(args, QUERY_EPARSEARGS, "SORTBY: Not enough arguments were provided based on argument count");
}
