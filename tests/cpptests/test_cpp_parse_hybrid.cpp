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
#include "rmalloc.h"
// #include "src/index.h"
#include "src/aggregate/aggregate.h"
#include "src/vector_index.h"
#include "VecSim/query_results.h"
#include "info/global_stats.h"
#include "src/ext/default.h"
#include "src/redisearch_rs/headers/query_error.h"
#include "asm_state_machine.h"

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
  RedisModuleSlotRangeArray* local_slots;

  // Helper function to create a RedisModuleSlotRangeArray for testing
  RedisModuleSlotRangeArray* createSlotRangeArray(uint16_t start, uint16_t end) {
    size_t array_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange);
    RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)rm_malloc(array_size);
    array->num_ranges = 1;
    array->ranges[0].start = start;
    array->ranges[0].end = end;
    return array;
  }

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    ASM_StateMachine_Init();
    local_slots = createSlotRangeArray(0, 16383);
    ASM_StateMachine_SetLocalSlots(local_slots);
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
    result.coordDispatchTime = &hybridRequest->profileClocks.coordDispatchTime;
  }

  void TearDown() override {
    if (hybridRequest) {
      HybridRequest_DecrRef(hybridRequest);
    }
    if (hybridParams.scoringCtx) {
      HybridScoringContext_Free(hybridParams.scoringCtx);
    }
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = NULL;
    }
    ASM_StateMachine_End();
    if (local_slots) {
      rm_free(local_slots);
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
    int rc = parseHybridCommand(ctx, &ac, hybridRequest->sctx, &result, &status, false, EXEC_NO_FLAGS);
    EXPECT_TRUE(QueryError_IsOk(&status)) << "Parse failed: " << QueryError_GetDisplayableError(&status, false);
    return rc;
  }

  // Helper function to test error cases with less boilerplate
  void testErrorCode(RMCK::ArgvList& args, QueryErrorCode expected_code, const char* expected_detail);

};

#define parseCommand(args) do { \
  ASSERT_EQ(parseCommandInternal(args), REDISMODULE_OK) << "parseCommandInternal failed"; \
} while(0)


#define assertLinearScoringCtx(Weight0, Weight1) do { \
  ASSERT_EQ(result.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR); \
  ASSERT_EQ(result.hybridParams->scoringCtx->linearCtx.numWeights, HYBRID_REQUEST_NUM_SUBQUERIES); \
  ASSERT_TRUE(result.hybridParams->scoringCtx->linearCtx.linearWeights != NULL); \
  ASSERT_DOUBLE_EQ(result.hybridParams->scoringCtx->linearCtx.linearWeights[0], Weight0); \
  ASSERT_DOUBLE_EQ(result.hybridParams->scoringCtx->linearCtx.linearWeights[1], Weight1); \
} while(0)

#define assertRRFScoringCtx(Constant, Window) do { \
  ASSERT_EQ(result.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_RRF); \
  ASSERT_DOUBLE_EQ(result.hybridParams->scoringCtx->rrfCtx.constant, Constant); \
  ASSERT_EQ(result.hybridParams->scoringCtx->rrfCtx.window, Window); \
} while(0)


TEST_F(ParseHybridTest, testBasicValidInput) {
  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

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
                      "SEARCH", "@title:($param1)", "VSIM", "@vector", "$BLOB",
                      "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA);

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
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "TIMEOUT", "240");

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

TEST_F(ParseHybridTest, testConfigOOMFailPolicyPropagation) {
  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Fail;
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  ASSERT_EQ(result.reqConfig->oomPolicy, OomPolicy_Fail);
  ASSERT_EQ(result.vector->reqConfig.oomPolicy, OomPolicy_Fail);
  ASSERT_EQ(result.search->reqConfig.oomPolicy, OomPolicy_Fail);
}

TEST_F(ParseHybridTest, testConfigOOMReturnPolicyPropagation) {
  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Return;
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  ASSERT_EQ(result.reqConfig->oomPolicy, OomPolicy_Return);
  ASSERT_EQ(result.vector->reqConfig.oomPolicy, OomPolicy_Return);
  ASSERT_EQ(result.search->reqConfig.oomPolicy, OomPolicy_Return);
}

TEST_F(ParseHybridTest, testConfigOOMIgnorePolicyPropagation) {

  // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
  RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Ignore;
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  parseCommand(args);
  ASSERT_EQ(result.reqConfig->oomPolicy, OomPolicy_Ignore);
  ASSERT_EQ(result.vector->reqConfig.oomPolicy, OomPolicy_Ignore);
  ASSERT_EQ(result.search->reqConfig.oomPolicy, OomPolicy_Ignore);
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
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "NOSORT");

  parseCommand(args);

  // Verify that an arrange step was not created
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(result.tailPlan, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_TRUE(arrangeStep == NULL);
}

TEST_F(ParseHybridTest, testSortByFieldDoesNotDisableImplicitSort) {
  // Test SORTBY with actual field (not 0) - should not disable implicit sorting
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "SORTBY", "1", "@score");

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
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

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
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "2", "K", "8",
      "YIELD_SCORE_AS", "distance_score",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

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
  // Parse hybrid request - no explicit VSIM FILTER clause
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "2", "RADIUS", "0.5", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result.vector;

  // Verify AST structure for RANGE query without explicit VSIM FILTER
  // The vector node is the root directly (no PHRASE/intersection needed)
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  QueryNode *vn = vecReq->ast.root;

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
  // RANGE queries in FT.HYBRID without explicit VSIM FILTER use BY_SCORE,
  // so the iterator returns results sorted by distance.
  ASSERT_EQ(vq->range.order, BY_SCORE);

  // Verify BLOB parameter was correctly resolved (parameter resolution test)
  const char* expectedBlob = TEST_BLOB_DATA;
  size_t expectedBlobLen = strlen(expectedBlob);
  ASSERT_TRUE(vq->range.vector != NULL);
  ASSERT_EQ(vq->range.vecLen, expectedBlobLen);
  ASSERT_EQ(memcmp(vq->range.vector, expectedBlob, expectedBlobLen), 0);
}

TEST_F(ParseHybridTest, testVsimRangeWithEpsilon) {
  // Parse hybrid request - no explicit VSIM FILTER clause
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.8", "EPSILON", "0.01", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result.vector;

  // Verify AST structure for RANGE query without explicit VSIM FILTER
  // The vector node is the root directly (no PHRASE/intersection needed)
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  QueryNode *vn = vecReq->ast.root;

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
  // RANGE queries in FT.HYBRID without explicit VSIM FILTER use BY_SCORE,
  // so the iterator returns results sorted by distance.
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

TEST_F(ParseHybridTest, testVsimRangeWithFilter) {
  // Parse hybrid request with RANGE and FILTER clause
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "2", "RADIUS", "0.5",
    "FILTER", "@title:hello", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  AREQ* vecReq = result.vector;

  // Verify AST structure for RANGE query with FILTER
  // Unlike KNN (where vector is root), RANGE with FILTER creates a PHRASE node
  // as root with the filter and vector node as children
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_PHRASE);

  // Use findVectorNodeChild to locate the vector node within the PHRASE
  QueryNode *vn = findVectorNodeChild(vecReq->ast.root);
  ASSERT_TRUE(vn != NULL);
  ASSERT_EQ(vn->type, QN_VECTOR);

  // Verify QueryNode structure
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
  ASSERT_EQ(vn->opts.flags & QueryNode_HybridVectorSubqueryNode, QueryNode_HybridVectorSubqueryNode);
  ASSERT_TRUE(vn->opts.distField == NULL); // No YIELD_SCORE_AS specified

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  ASSERT_TRUE(vq->scoreField != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_EQ(vq->type, VECSIM_QT_RANGE);
  ASSERT_EQ(vq->range.radius, 0.5);
  // RANGE queries with explicit FILTER use BY_ID ordering because the filter
  // creates a PHRASE node which uses an intersection iterator with SkipTo.
  // SkipTo requires child iterators to be sorted by document ID.
  ASSERT_EQ(vq->range.order, BY_ID);

  // Verify BLOB parameter was correctly resolved
  const char* expectedBlob = TEST_BLOB_DATA;
  size_t expectedBlobLen = strlen(expectedBlob);
  ASSERT_TRUE(vq->range.vector != NULL);
  ASSERT_EQ(vq->range.vecLen, expectedBlobLen);
  ASSERT_EQ(memcmp(vq->range.vector, expectedBlob, expectedBlobLen), 0);

  // Verify the filter is also present in the PHRASE node
  // The PHRASE should have at least 2 children: filter node and vector node
  ASSERT_GE(QueryNode_NumChildren(vecReq->ast.root), 2);

  // Find and verify the filter node (should be a UNION containing TOKEN nodes
  // for "hello")
  bool foundFilter = false;
  for (size_t i = 0; i < QueryNode_NumChildren(vecReq->ast.root); ++i) {
    QueryNode* child = vecReq->ast.root->children[i];
    if (child && child->type == QN_UNION) {
      // This is the filter node - verify it contains the expected tokens
      ASSERT_GE(QueryNode_NumChildren(child), 1);
      ASSERT_EQ(child->children[0]->type, QN_TOKEN);
      ASSERT_STREQ(child->children[0]->tn.str, "hello");
      foundFilter = true;
      break;
    }
  }
  ASSERT_TRUE(foundFilter);
}

TEST_F(ParseHybridTest, testExternalCommandWith_NUM_SSTRING) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
        "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "_NUM_SSTRING");

  QueryError status = QueryError_Default();
  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hybridRequest, &ac, args, args.size());
  parseHybridCommand(ctx, &ac, hybridRequest->sctx, &result, &status, false, EXEC_NO_FLAGS);
  EXPECT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_PARSE_ARGS) << "Should fail as external command";
  QueryError_ClearError(&status);

  // Clean up any partial allocations from the failed parse
  if (result.vector && result.vector->ast.root) {
    QAST_Destroy(&result.vector->ast);
    result.vector->ast.root = NULL;
  }
}

TEST_F(ParseHybridTest, testInternalCommandWith_NUM_SSTRING) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
        "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "_NUM_SSTRING", SLOTS_STR);
  static RedisModuleSlotRangeArray slots = {
    .num_ranges = 1,
    .ranges = {
      { .start = 0, .end = 16383 }
    }
  };
  char * serializedSlots = SlotRangesArray_Serialize(&slots);
  args.add(serializedSlots, SlotRangeArray_SizeOf(1));
  rm_free(serializedSlots);

  // Add _COORD_DISPATCH_TIME argument (required for internal commands)
  args.add("_COORD_DISPATCH_TIME", strlen("_COORD_DISPATCH_TIME"));
  args.add("1000000", strlen("1000000"));  // 1ms in nanoseconds

  QueryError status = QueryError_Default();

  ASSERT_FALSE(result.hybridParams->aggregationParams.common.reqflags & QEXEC_F_TYPED);
  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hybridRequest, &ac, args, args.size());
  parseHybridCommand(ctx, &ac, hybridRequest->sctx, &result, &status, true, EXEC_NO_FLAGS);
  EXPECT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_OK) << "Should succeed as internal command";
  QueryError_ClearError(&status);

  // Verify _NUM_SSTRING flag is set after parsing
  ASSERT_TRUE(result.hybridParams->aggregationParams.common.reqflags & QEXEC_F_TYPED);

  // Verify _COORD_DISPATCH_TIME was parsed and stored
  EXPECT_EQ(hybridRequest->profileClocks.coordDispatchTime, 1000000) << "Coordinator dispatch time should be set";
}

TEST_F(ParseHybridTest, testVsimInvalidFilterWeight) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "FILTER","@title:(foo bar)=> {$weight: 2.0}", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_WEIGHT_NOT_ALLOWED, "Weight attributes are not allowed in FT.HYBRID VSIM FILTER");
}

// Helper function to test error cases with less boilerplate
void ParseHybridTest::testErrorCode(RMCK::ArgvList& args, QueryErrorCode expected_code, const char* expected_detail) {
  QueryError status = QueryError_Default();

  // Create a fresh sctx for this test
  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hybridRequest, &ac, args, args.size());
  int rc = parseHybridCommand(ctx, &ac, hybridRequest->sctx, &result, &status, false, EXEC_NO_FLAGS);
  ASSERT_TRUE(rc == REDISMODULE_ERR) << "parsing error: " << QueryError_GetUserError(&status);
  ASSERT_EQ(QueryError_GetCode(&status), expected_code) << "parsing error: " << QueryError_GetUserError(&status);

  // Errors now include a stable prefix (e.g. "SEARCH_FOO ...") for uniqueness.
  // To keep tests stable, allow either exact match or "contains" match when the
  // test asserts only the detail portion.
  const char *user_error = QueryError_GetUserError(&status);
  ASSERT_TRUE(user_error != nullptr);
  if (strcmp(user_error, expected_detail) != 0) {
    ASSERT_NE(std::string(user_error).find(expected_detail), std::string::npos)
        << "parsing error: " << user_error;
  }

  // Clean up
  QueryError_ClearError(&status);
}

TEST_F(ParseHybridTest, testVsimInvalidFilterVectorField) {
  // Setup: Dialect 2 is required for vector queries
  unsigned int previousDialectVersion = RSGlobalConfig.requestConfigParams.dialectVersion;
  SET_DIALECT(RSGlobalConfig.requestConfigParams.dialectVersion, 2);

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "FILTER", "@vector:[VECTOR_RANGE 0.01 $BLOB]", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_VECTOR_NOT_ALLOWED, "Vector expressions are not allowed in FT.HYBRID VSIM FILTER");

  // Teardown: Restore previous dialect version
  SET_DIALECT(RSGlobalConfig.requestConfigParams.dialectVersion, previousDialectVersion);
}

// ============================================================================
// ERROR HANDLING TESTS - All tests using the testErrorCode helper function
// ============================================================================

// Basic parsing error tests
TEST_F(ParseHybridTest, testMissingSearchArgument) {
  // Missing SEARCH argument: FT.HYBRID <index> VSIM @vector_field
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid subqueries count: expected an unsigned integer");
}

TEST_F(ParseHybridTest, testMissingQueryStringAfterSearch) {
  // Missing query string after SEARCH: FT.HYBRID <index> SEARCH
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "No query string provided for SEARCH");
}

TEST_F(ParseHybridTest, testMissingSecondSearchArgument) {
  // Missing second search argument: FT.HYBRID <index> SEARCH hello
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "VSIM argument is required");
}

TEST_F(ParseHybridTest, testInvalidSearchAfterSearch) {
  // Test invalid syntax: FT.HYBRID <index> SEARCH hello SEARCH world (should fail)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "SEARCH", "world");
  testErrorCode(args, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument `SEARCH` in SEARCH");
}

// VSIM parsing error tests
TEST_F(ParseHybridTest, testVsimMissingVectorField) {
  // Test missing vector field name after VSIM
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Missing vector field name");
}

TEST_F(ParseHybridTest, testVsimMissingVectorArgument) {
  // Test missing vector argument after field name
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Missing vector argument");
}

TEST_F(ParseHybridTest, testVsimVectorFieldMissingAtPrefix) {
  // Test vector field name without @ prefix - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "2", "K", "10", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Missing @ prefix for vector field name");
}

// Parameter parsing error tests
TEST_F(ParseHybridTest, testBlobWithoutParams) {
  // Test using $BLOB without PARAMS section - should fail
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "10");
  testErrorCode(args, QUERY_ERROR_CODE_NO_PARAM, "Parameter not found `BLOB`");
}

TEST_F(ParseHybridTest, testDirectVector) {
  // Test using direct vector - should fail
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid vector argument, expected a parameter name starting with $");
}

// KNN parsing error tests
TEST_F(ParseHybridTest, testKNNMissingArgumentCount) {
  // Test KNN without argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument count");
}

TEST_F(ParseHybridTest, testVsimKNNOddParamCount) {
  // Test KNN with count=1 (odd count, missing K value)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "1", "K", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid argument count: 1 (must be a positive even number for key/value pairs)");
}

TEST_F(ParseHybridTest, testKNNZeroArgumentCount) {
  // Test KNN with zero argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "0");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid argument count: 0 (must be a positive even number for key/value pairs)");
}

TEST_F(ParseHybridTest, testVsimSubqueryMissingK) {
  // Test KNN without K argument
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "EF_RUNTIME", "100", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing required argument K");
}

TEST_F(ParseHybridTest, testKNNInvalidKValue) {
  // Test KNN with invalid K value (non-numeric)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "invalid", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid K value");
}

TEST_F(ParseHybridTest, testKNNNegativeKValue) {
  // Test KNN with negative K value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "-1", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid K value");
}

TEST_F(ParseHybridTest, testKNNZeroKValue) {
  // Test KNN with zero K value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "0", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid K value");
}

TEST_F(ParseHybridTest, testVsimKNNDuplicateK) {
  // Test KNN with duplicate K arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "K", "20", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate K argument");
}

TEST_F(ParseHybridTest, testVsimKNNDuplicateEFRuntime) {
  // Test KNN with duplicate EF_RUNTIME arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "6", "K", "10", "EF_RUNTIME", "100", "EF_RUNTIME", "200", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate EF_RUNTIME argument");
}

TEST_F(ParseHybridTest, testKNNDuplicateYieldDistanceAs) {
  // Test KNN with duplicate YIELD_SCORE_AS arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "2", "K", "10",
      "YIELD_SCORE_AS", "dist1", "YIELD_SCORE_AS", "dist2",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_DUP_PARAM,
                "Duplicate YIELD_SCORE_AS argument");
}

TEST_F(ParseHybridTest, testKNNCountingYieldDistanceAs) {
  // Test KNN with YIELD_SCORE_AS as counting argument
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "YIELD_SCORE_AS", "v_score",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument `YIELD_SCORE_AS` in KNN");
}

TEST_F(ParseHybridTest, testVsimKNNWithEpsilon) {
  // Test KNN with EPSILON (should be RANGE-only)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "EPSILON", "0.01", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument `EPSILON` in KNN");
}

TEST_F(ParseHybridTest, testVsimSubqueryWrongParamCount) {
  // Test with wrong argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "\"hello\"", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "FILTER", "@text:hello", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument `FILTER` in KNN");
}

// RANGE parsing error tests
TEST_F(ParseHybridTest, testRangeMissingArgumentCount) {
  // Test RANGE without argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument count");
}

TEST_F(ParseHybridTest, testVsimRangeOddParamCount) {
  // Test RANGE with count=3 (odd count, missing EPSILON value)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "3", "RADIUS", "0.5", "EPSILON", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid argument count: 3 (must be a positive even number for key/value pairs)");
}

TEST_F(ParseHybridTest, testRangeZeroArgumentCount) {
  // Test RANGE with zero argument count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "0");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid argument count: 0 (must be a positive even number for key/value pairs)");
}

TEST_F(ParseHybridTest, testRangeInvalidRadiusValue) {
  // Test RANGE with invalid RADIUS value (non-numeric)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "2", "RADIUS", "invalid", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid RADIUS value");
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateRadius) {
  // Test RANGE with duplicate RADIUS arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "RADIUS", "0.8", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate RADIUS argument");
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateEpsilon) {
  // Test RANGE with duplicate EPSILON arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "6", "RADIUS", "0.5", "EPSILON", "0.01", "EPSILON", "0.02", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_DUP_PARAM, "Duplicate EPSILON argument");
}

TEST_F(ParseHybridTest, testRangeDuplicateYieldDistanceAs) {
  // Test RANGE with duplicate YIELD_SCORE_AS arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "RANGE", "2", "RADIUS", "0.5",
      "YIELD_SCORE_AS", "dist1", "YIELD_SCORE_AS", "dist2",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_DUP_PARAM,
                "Duplicate YIELD_SCORE_AS argument");
}

TEST_F(ParseHybridTest, testRangeCountingYieldDistanceAs) {
  // Test RANGE with YIELD_SCORE_AS as counting argument
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "RANGE", "4", "RADIUS", "0.5", "YIELD_SCORE_AS", "v_score",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument `YIELD_SCORE_AS` in RANGE");
}

TEST_F(ParseHybridTest, testVsimRangeWithEFRuntime) {
  // Test RANGE with EF_RUNTIME (should be KNN-only)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "EF_RUNTIME", "100", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument `EF_RUNTIME` in RANGE");
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
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "CONSTANT: Could not convert argument to expected type");
}

TEST_F(ParseHybridTest, testDefaultTextScorerForLinear) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", \
   "COMBINE", "LINEAR", "4", "ALPHA", "0.6", "BETA", "0.4",
   "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  // No explicit scorer should be set; the default scorer will be used
  ASSERT_STREQ(result.search->searchopts.scorerName, DEFAULT_SCORER_NAME);
}

TEST_F(ParseHybridTest, testExplicitTextScorerForLinear) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "SCORER", "TFIDF", "VSIM", "@vector", "$BLOB", \
   "COMBINE", "LINEAR", "4", "ALPHA", "0.6", "BETA", "0.4",
   "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  ASSERT_STREQ(result.search->searchopts.scorerName, TFIDF_SCORER_NAME);
}

TEST_F(ParseHybridTest, testDefaultTextScorerForRRF) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", \
   "COMBINE", "RRF", "2", "CONSTANT", "10",
   "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  // No explicit scorer should be set; the default scorer will be used
  ASSERT_STREQ(result.search->searchopts.scorerName, DEFAULT_SCORER_NAME);
}

TEST_F(ParseHybridTest, testExplicitTextScorerForRRF) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "SCORER", "TFIDF", "VSIM", "@vector", "$BLOB",\
   "COMBINE", "RRF", "2", "CONSTANT", "10",
   "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  ASSERT_STREQ(result.search->searchopts.scorerName, TFIDF_SCORER_NAME);
}

TEST_F(ParseHybridTest, testLinearPartialWeightsAlpha) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "LINEAR", "2", "ALPHA", "0.6", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Missing value for BETA");
}

TEST_F(ParseHybridTest, testLinearMissingArgs) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "LINEAR", "4", "ALPHA", "0.6");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Not enough arguments in LINEAR, specified 4 but provided only 2");
}

TEST_F(ParseHybridTest, testLinearPartialWeightsBeta) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "LINEAR", "2", "BETA", "0.6", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Missing value for ALPHA");
}

TEST_F(ParseHybridTest, testLinearNegativeArgumentCount) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "LINEAR", "-2", "ALPHA", "0.6", "BETA", "0.4", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid LINEAR argument count, error: Value is outside acceptable bounds");
}

TEST_F(ParseHybridTest, testLinearMissingArgumentCount) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "LINEAR");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing LINEAR argument count");
}

// Missing parameter value tests
TEST_F(ParseHybridTest, testKNNMissingKValue) {
  // Test KNN with missing K value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument value for K");
}

TEST_F(ParseHybridTest, testKNNMissingEFRuntimeValue) {
  // Test KNN with missing EF_RUNTIME value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "EF_RUNTIME");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument value for EF_RUNTIME");
}

TEST_F(ParseHybridTest, testRangeMissingRadiusValue) {
  // Test RANGE with missing RADIUS value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "2", "RADIUS");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument value for RADIUS");
}

TEST_F(ParseHybridTest, testRangeMissingEpsilonValue) {
  // Test RANGE with missing EPSILON value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "EPSILON");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument value for EPSILON");
}

TEST_F(ParseHybridTest, testLinearMissingAlphaValue) {
  // Test LINEAR with missing ALPHA value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "LINEAR", "2", "ALPHA");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Not enough arguments in LINEAR, specified 2 but provided only 1");
}

TEST_F(ParseHybridTest, testLinearMissingBetaValue) {
  // Test LINEAR with missing BETA value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "LINEAR", "2", "BETA");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX,"Not enough arguments in LINEAR, specified 2 but provided only 1");
}

TEST_F(ParseHybridTest, testKNNMissingYieldDistanceAsValue) {
  // Test KNN with missing YIELD_SCORE_AS value (early return before CheckEnd)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "2", "K", "10",
      "YIELD_SCORE_AS");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument value for YIELD_SCORE_AS");
}

TEST_F(ParseHybridTest, testRangeMissingYieldDistanceAsValue) {
  // Test RANGE with missing YIELD_SCORE_AS value (early return before CheckEnd)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "RANGE", "2", "RADIUS", "0.5",
      "YIELD_SCORE_AS");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument value for YIELD_SCORE_AS");
}

// ============================================================================
// HYBRID CALLBACK ERROR TESTS - Testing error paths in hybrid_callbacks.c
// ============================================================================

// LIMIT callback error tests - These test the actual callback function error paths
TEST_F(ParseHybridTest, testLimitZeroCountWithNonZeroOffset) {
  // Test LIMIT 0 0 vs LIMIT 5 0 - the callback should catch the second case
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "LIMIT", "5", "0", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_LIMIT, "The `offset` of the LIMIT must be 0 when `num` is 0");
}

TEST_F(ParseHybridTest, testLimitInvalidOffset) {
  // Test LIMIT with invalid offset (negative)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "LIMIT", "-1", "10", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "LIMIT offset must be a non-negative integer");
}

TEST_F(ParseHybridTest, testLimitInvalidCount) {
  // Test LIMIT with invalid count (negative)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "LIMIT", "0", "-5", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "LIMIT count must be a non-negative integer");
}

TEST_F(ParseHybridTest, testLimitExceedsMaxResults) {
  // Test LIMIT that exceeds maxResults (default is 1000000)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "LIMIT", "0", "2000000", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_LIMIT, "LIMIT exceeds maximum of 1000000");
}

TEST_F(ParseHybridTest, testLimitOnlyOffset) {
  // Test LIMIT with only offset (should fail)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "LIMIT", "1");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "LIMIT: Not enough arguments were provided");
}

// SORTBY callback error tests
TEST_F(ParseHybridTest, testSortByMissingFieldName) {
  // Test SORTBY with missing field name (empty args after SORTBY)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "SORTBY");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "SORTBY: Failed to parse the argument count");
}

// PARAMS callback error tests
TEST_F(ParseHybridTest, testParamsOddArgumentCount) {
  // Test PARAMS with odd number of arguments (not key-value pairs)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "3", "BLOB", TEST_BLOB_DATA, "key2");
  testErrorCode(args, QUERY_ERROR_CODE_ADD_ARGS, "Parameters must be specified in PARAM VALUE pairs");
}

TEST_F(ParseHybridTest, testParamsZeroArguments) {
  // Test PARAMS with zero arguments
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "0");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "PARAMS: Invalid argument count");
}

TEST_F(ParseHybridTest, testParamsSpecifiedMultipleTimes) {
  // Test PARAMS with multiple PARAMS clauses
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "PARAMS", "2", "p2", "val2");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "PARAMS: Argument specified multiple times");
}

// WITHCURSOR callback error tests
TEST_F(ParseHybridTest, testWithCursorInvalidMaxIdle) {
  // Test WITHCURSOR with invalid MAXIDLE value (zero)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "WITHCURSOR", "MAXIDLE", "0");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for MAXIDLE: Value is outside acceptable bounds");
}

TEST_F(ParseHybridTest, testWithCursorInvalidCount) {
  // Test WITHCURSOR with invalid COUNT value (zero)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "WITHCURSOR", "COUNT", "0");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for COUNT: Value is outside acceptable bounds");
}

// GROUPBY callback error tests
TEST_F(ParseHybridTest, testGroupByNoProperties) {
  // Test GROUPBY with no properties specified
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "GROUPBY");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "GROUPBY: Failed to parse the argument count");
}

TEST_F(ParseHybridTest, testGroupByPropertyMissingAtPrefix) {
  // Test GROUPBY with property missing @ prefix
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "GROUPBY", "1", "title");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for GROUPBY: Unknown property `title`. Did you mean `@title`?");
}

// APPLY callback error tests
TEST_F(ParseHybridTest, testApplyMissingAsArgument) {
  // Test APPLY with AS but missing alias argument
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "APPLY", "upper(@title)", "AS");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "AS needs argument");
}

// LOAD callback error tests
TEST_F(ParseHybridTest, testLoadInvalidFieldCount) {
  // Test LOAD with invalid field count (non-numeric)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "LOAD", "invalid");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for LOAD: Expected number of fields or `*`");
}

TEST_F(ParseHybridTest, testLoadInsufficientFields) {
  // Test LOAD with insufficient fields for specified count
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "LOAD", "3", "@title");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Not enough arguments for LOAD");
}

// ============================================================================
// Test not yet supported arguments
// ============================================================================

TEST_F(ParseHybridTest, testCombineRRFWithoutArgument) {
  // Test RANGE with missing YIELD_DISTANCE_AS value (early return before CheckEnd)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "0", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Explicitly specifying RRF requires at least one argument, argument count must be positive");
}

TEST_F(ParseHybridTest, testCombineRRFWithOddArgumentCount) {
  // Test RANGE with missing YIELD_DISTANCE_AS value (early return before CheckEnd)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "1", "WINDOW", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "RRF expects pairs of key value arguments, argument count must be an even number");
}

TEST_F(ParseHybridTest, testExplainScore) {
  // Test EXPLAINSCORE - currently should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "EXPLAINSCORE", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "EXPLAINSCORE is not yet supported by FT.HYBRID");
}

// ============================================================================
// DIALECT ERROR TESTS - Testing DIALECT is not supported
// ============================================================================

TEST_F(ParseHybridTest, testDialectInSearchSubquery) {
  // Test DIALECT in SEARCH subquery - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "DIALECT", "2", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "DIALECT is not supported in FT.HYBRID or any of its subqueries. Please check the documentation on search-default-dialect configuration.");
}

TEST_F(ParseHybridTest, testDialectInVectorKNNSubquery) {
  // Test DIALECT in vector KNN subquery - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "DIALECT", "2", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument `DIALECT` in KNN");
}

TEST_F(ParseHybridTest, testDialectInVectorRangeSubquery) {
  // Test DIALECT in vector RANGE subquery - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "RANGE", "2", "DIALECT", "2", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_ARG_UNRECOGNIZED, "Unknown argument `DIALECT` in RANGE");
}

TEST_F(ParseHybridTest, testDialectInTail) {
  // Test DIALECT in tail (after subqueries) - should fail with specific error
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "DIALECT", "2", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "DIALECT is not supported in FT.HYBRID or any of its subqueries. Please check the documentation on search-default-dialect configuration.");
}


// ============================================================================
// WINDOW ERROR TESTS
// ============================================================================

TEST_F(ParseHybridTest, testCombineRRFNegativeWindow) {
  // Test RRF with negative WINDOW value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "2", "WINDOW", "-5", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "WINDOW: Value below minimum");
}

TEST_F(ParseHybridTest, testCombineRRFZeroWindow) {
  // Test RRF with zero WINDOW value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "COMBINE", "RRF", "2", "WINDOW", "0", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "WINDOW: Value below minimum");
}

TEST_F(ParseHybridTest, testCombineLinearNegativeWindow) {
  // Test LINEAR with negative WINDOW value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "LINEAR", "6", "ALPHA", "0.6", "BETA", "0.4", "WINDOW", "-10",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "WINDOW: Value below minimum");
}

TEST_F(ParseHybridTest, testCombineLinearZeroWindow) {
  // Test LINEAR with zero WINDOW value
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "LINEAR", "6", "ALPHA", "0.6", "BETA", "0.4", "WINDOW", "0",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "WINDOW: Value below minimum");
}

TEST_F(ParseHybridTest, testSortby0InvalidArgumentCount) {
  // SORTBY requires at least one argument (param count)
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "SORTBY", "0", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "SORTBY: Invalid argument count");
}

TEST_F(ParseHybridTest, testSortbyNotEnoughArguments) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "SORTBY", "2", "title");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "SORTBY: Not enough arguments were provided based on argument count");
}


// ============================================================================
// HYBRID SUBQUERIES COUNT ERROR TESTS
// ============================================================================

TEST_F(ParseHybridTest, testHybridSubqueriesCountMissing) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str());
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "Missing subqueries count for FT.HYBRID");
}

TEST_F(ParseHybridTest, testHybridSubqueriesCountInvalid) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "INVALID_COUNT", "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "2");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid subqueries count: expected an unsigned integer");
}

TEST_F(ParseHybridTest, testHybridSubqueriesCountInvalidThree) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "3" ,"SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "2");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "FT.HYBRID currently supports only two subqueries");
}

TEST_F(ParseHybridTest, testHybridSubqueriesCountInvalidOne) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "1" ,"SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "2");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS, "FT.HYBRID currently supports only two subqueries");
}

TEST_F(ParseHybridTest, testHybridSubqueriesCountInvalidRange) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "0" ,"SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "2");
  testErrorCode(args, QUERY_ERROR_CODE_SYNTAX, "Invalid subqueries count: expected an unsigned integer");
}

TEST_F(ParseHybridTest, testHybridSubqueriesCountInvalidKeyword) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "2", "INVALID_KEYWORD", "hello", "VSIM", "@vector", TEST_BLOB_DATA, "2");
  testErrorCode(args,  QUERY_ERROR_CODE_SYNTAX, "SEARCH keyword is required");
}

// ============================================================================
// SHARD_K_RATIO TESTS
// ============================================================================

TEST_F(ParseHybridTest, testShardKRatioValidMinValue) {
  // Test valid minimum SHARD_K_RATIO value (0.1)
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "0.1",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  const AREQ* vecReq = result.vector;
  ASSERT_TRUE(vecReq->ast.root != nullptr);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  const VectorQuery *vq = vecReq->ast.root->vn.vq;
  ASSERT_DOUBLE_EQ(vq->knn.shardWindowRatio, 0.1);
}

TEST_F(ParseHybridTest, testShardKRatioValidMidValue) {
  // Test valid mid-range SHARD_K_RATIO value (0.5)
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "0.5",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  const AREQ* vecReq = result.vector;
  ASSERT_TRUE(vecReq->ast.root != nullptr);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  const VectorQuery *vq = vecReq->ast.root->vn.vq;
  ASSERT_DOUBLE_EQ(vq->knn.shardWindowRatio, 0.5);
}

TEST_F(ParseHybridTest, testShardKRatioValidMaxValue) {
  // Test valid maximum SHARD_K_RATIO value (1.0)
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "1.0",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  const AREQ* vecReq = result.vector;
  ASSERT_TRUE(vecReq->ast.root != nullptr);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  const VectorQuery *vq = vecReq->ast.root->vn.vq;
  ASSERT_DOUBLE_EQ(vq->knn.shardWindowRatio, 1.0);
}

// Passing SHARD_K_RATIO as a parameter is not yet supported.
// This test should be updated once it is supported. MOD-12915
TEST_F(ParseHybridTest, testShardKRatioValidFromParams) {
  // Test valid maximum SHARD_K_RATIO value (1.0)
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "$RATIO",
    "PARAMS", "4", "BLOB", TEST_BLOB_DATA, "$RATIO", "0.75");
  testErrorCode(args, QUERY_ERROR_CODE_INVAL,
    "Invalid shard k ratio value '$RATIO'");
}

// Passing SHARD_K_RATIO as a parameter is not yet supported.
// This test should be updated once it is supported. MOD-12915
TEST_F(ParseHybridTest, testShardKRatioInvalidFromParams) {
  // Test valid maximum SHARD_K_RATIO value (1.0)
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "$RATIO",
    "PARAMS", "4", "BLOB", TEST_BLOB_DATA, "RATIO", "8.1");
  testErrorCode(args, QUERY_ERROR_CODE_INVAL,
    "Invalid shard k ratio value '$RATIO'");
}

TEST_F(ParseHybridTest, testShardKRatioInvalidBelowMin) {
  // Test invalid SHARD_K_RATIO value at exclusive minimum (must be > 0.0)
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "0.0",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_INVAL,
    "Invalid shard k ratio value: Shard k ratio must be greater than 0 and at most 1 (got 0)");
}

TEST_F(ParseHybridTest, testShardKRatioInvalidAboveMax) {
  // Test invalid SHARD_K_RATIO value above maximum (> 1.0)
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "1.5",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_INVAL,
    "Invalid shard k ratio value: Shard k ratio must be greater than 0 and at most 1 (got 1.5)");
}

TEST_F(ParseHybridTest, testShardKRatioInvalidNegative) {
  // Test invalid negative SHARD_K_RATIO value
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "-0.5",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_INVAL,
    "Invalid shard k ratio value: Shard k ratio must be greater than 0 and at most 1 (got -0.5)");
}

TEST_F(ParseHybridTest, testShardKRatioInvalidNonNumeric) {
  // Test invalid non-numeric SHARD_K_RATIO value
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "invalid",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_INVAL,
    "Invalid shard k ratio value 'invalid'");
}

TEST_F(ParseHybridTest, testShardKRatioMissingValue) {
  // Test missing SHARD_K_RATIO value
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_INVAL,
    "Invalid shard k ratio value 'PARAMS'");
}

TEST_F(ParseHybridTest, testShardKRatioMissingValueAtEnd) {
  // Test missing SHARD_K_RATIO value
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS,
    "Missing argument value for SHARD_K_RATIO");
}

TEST_F(ParseHybridTest, testShardKRatioWrongPosition) {
  // Test missing SHARD_K_RATIO value at end of command (no PARAMS after it)
  // NOTE: Current implementation doesn't loop to check for SHARD_K_RATIO after PARAMS,
  // so it's reported as "Unknown argument" instead of "Missing argument value"
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "2", "K", "10",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA,
    "SHARD_K_RATIO");
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS,
    "SHARD_K_RATIO: Unknown argument");
}

TEST_F(ParseHybridTest, testShardKRatioDuplicate) {
  // Test duplicate SHARD_K_RATIO argument - proper duplicate detection via
  // looping through optional args.
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
      "VSIM", "@vector", "$BLOB", "KNN", "6", "K", "10", "SHARD_K_RATIO", "0.5",
        "SHARD_K_RATIO", "0.7",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_DUP_PARAM,
    "Duplicate SHARD_K_RATIO argument");
}

TEST_F(ParseHybridTest, testShardKRatioWithFilter) {
  // Test SHARD_K_RATIO with KNN query and FILTER
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "0.8",
      "FILTER", "@title:world",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  const AREQ* vecReq = result.vector;
  ASSERT_TRUE(vecReq->ast.root != nullptr);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  const VectorQuery *vq = vecReq->ast.root->vn.vq;
  ASSERT_DOUBLE_EQ(vq->knn.shardWindowRatio, 0.8);
}

TEST_F(ParseHybridTest, testShardKRatiowithFilterAndPostFilter) {
  // Test SHARD_K_RATIO with KNN query, FILTER, and POST-FILTER
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "4", "K", "10", "SHARD_K_RATIO", "0.8",
      "FILTER", "@title:(world)",
    "FILTER", "@__key:doc:1",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  const AREQ* vecReq = result.vector;
  ASSERT_TRUE(vecReq->ast.root != nullptr);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  const VectorQuery *vq = vecReq->ast.root->vn.vq;
  ASSERT_DOUBLE_EQ(vq->knn.shardWindowRatio, 0.8);

  // Verify that the vector node is the child of the filter node
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(QueryNode_NumChildren(vn), 1);
  ASSERT_EQ(vn->children[0]->type, QN_UNION);
  ASSERT_EQ(vn->children[0]->children[0]->type, QN_TOKEN);
  ASSERT_STREQ(vn->children[0]->children[0]->tn.str, "world");
  ASSERT_STREQ(vn->children[0]->children[1]->tn.str, "+world");

  // Verify the post-filter
  // Post-filters are stored in the tail pipeline, not in the vector AST
  const AGGPlan *tailPlan = result.tailPlan;
  ASSERT_NE(tailPlan, nullptr) << "Tail plan should not be NULL";

  // Check that a FILTER step exists in the tail plan
  ASSERT_TRUE(AGPLN_HasStep(tailPlan, PLN_T_FILTER))
    << "Post-filter should be present in tail plan";

  // Find the FILTER step
  const PLN_BaseStep *filterStep = AGPLN_FindStep(tailPlan, nullptr, nullptr, PLN_T_FILTER);
  ASSERT_NE(filterStep, nullptr)
    << "Post-filter step should be found in tail plan";
  ASSERT_EQ(filterStep->type, PLN_T_FILTER)
    << "Step should be of type PLN_T_FILTER";

  // Cast to PLN_MapFilterStep to access the filter expression
  const auto *mapFilterStep = (const PLN_MapFilterStep *)filterStep;
  ASSERT_NE(mapFilterStep->expr, nullptr) << "Filter expression should not be NULL";

  // Verify the expression content
  size_t exprLen = 0;
  const char *exprStr = HiddenString_GetUnsafe(mapFilterStep->expr, &exprLen);
  ASSERT_NE(exprStr, nullptr) << "Expression string should not be NULL";
  ASSERT_GT(exprLen, 0) << "Expression length should be greater than 0";
  ASSERT_STREQ(exprStr, "@__key:doc:1") << "Post-filter expression should match expected value";
}

TEST_F(ParseHybridTest, testShardKRatioAfterYieldScoreAs) {
  // Test SHARD_K_RATIO combined with YIELD_SCORE_AS
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB",
      "KNN", "2", "K", "10",
      "YIELD_SCORE_AS", "my_score",
      "SHARD_K_RATIO", "0.75",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);
  testErrorCode(args, QUERY_ERROR_CODE_PARSE_ARGS,
    "SHARD_K_RATIO: Unknown argument");
}

TEST_F(ParseHybridTest, testShardKRatioBeforeYieldScoreAs) {
  // Test SHARD_K_RATIO combined with YIELD_SCORE_AS
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello",
    "VSIM", "@vector", "$BLOB", "KNN", "4", "K", "10", "SHARD_K_RATIO", "0.75",
    "YIELD_SCORE_AS", "my_score",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  const AREQ* vecReq = result.vector;
  ASSERT_TRUE(vecReq->ast.root != nullptr);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  const VectorQuery *vq = vecReq->ast.root->vn.vq;
  ASSERT_DOUBLE_EQ(vq->knn.shardWindowRatio, 0.75);

  // YIELD_SCORE_AS is stored in QueryNode opts.distField (not in parsedVectorData)
  const QueryNode *vn = vecReq->ast.root;
  ASSERT_STREQ(vn->opts.distField, "my_score");
}

TEST_F(ParseHybridTest, testShardKRatioDefaultValue) {
  // Test default SHARD_K_RATIO when not specified (should be 1.0)
  RMCK::ArgvList args(
    ctx, "FT.HYBRID", index_name.c_str(),
    "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "10",
    "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);

  const AREQ* vecReq = result.vector;
  ASSERT_TRUE(vecReq->ast.root != nullptr);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  const VectorQuery *vq = vecReq->ast.root->vn.vq;
  // Default should be 1.0 (DEFAULT_SHARD_WINDOW_RATIO - no optimization)
  ASSERT_DOUBLE_EQ(vq->knn.shardWindowRatio, 1.0);
}
