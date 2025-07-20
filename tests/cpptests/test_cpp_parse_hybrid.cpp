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
#include "src/aggregate/aggregate.h"
#include "src/vector_index.h"
#include "VecSim/query_results.h"

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

    // Add a test document for vector tests
    RSDoc* d = RediSearch_CreateDocument("doc:1", strlen("doc:1"), 1.0, NULL);
    RediSearch_DocumentAddFieldCString(d, "title", "another indexing testing",
                                     RSFLDTYPE_FULLTEXT);
    RediSearch_DocumentAddFieldCString(d, "vector", "ABCDEFG==", RSFLDTYPE_VECTOR);
    RediSearch_SpecAddDocument(spec->own_ref.rm, d);
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

  // Helper function to create RedisModuleString array from C strings
  RedisModuleString** createStringArray(const std::vector<const char*>& strings) {
    RedisModuleString** argv = (RedisModuleString**)malloc(strings.size() * sizeof(RedisModuleString*));
    for (size_t i = 0; i < strings.size(); i++) {
      argv[i] = RedisModule_CreateString(ctx, strings[i], strlen(strings[i]));
    }
    return argv;
  }

  void freeStringArray(RedisModuleString** argv, size_t count) {
    for (size_t i = 0; i < count; i++) {
      RedisModule_FreeString(ctx, argv[i]);
    }
    free(argv);
  }
};

// TEST_F(ParseHybridTest, testBasicValidInput) {
//   QueryError status = {QueryErrorCode(0)};

//   // Create a basic hybrid query: FT.HYBRID <index> SEARCH hello VSIM world
//   RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "world");

//   // Create a fresh sctx for this test since parseHybridRequest takes ownership
//   RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
//   ASSERT_TRUE(test_sctx != NULL);

//   HybridRequest* result = parseHybridRequest(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

//   // Verify the request was parsed successfully
//   ASSERT_TRUE(result != NULL);
//   ASSERT_EQ(status.code, QUERY_OK);

//   // Verify the structure contains expected number of requests
//   ASSERT_EQ(result->nrequests, 2);

//   // Verify default scoring type is RRF
//   ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_RRF);
//   ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.k, 1);
//   ASSERT_EQ(result->hybridParams->scoringCtx->rrfCtx.window, 20);

//   // parseHybridRequest calls AREQ_ApplyContext which takes ownership of test_sctx
//   // No need to free test_sctx as it's now owned by the result

//   // Clean up
//   HybridRequest_Free(result);
// }

TEST_F(ParseHybridTest, testMissingSearchParameter) {
  QueryError status = {QueryErrorCode(0)};

  // Missing SEARCH parameter: FT.HYBRID <index> VSIM @vector_field
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "VSIM", "vector_field");
  // RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "VSIM", "vector", "$BLOB", "PARAMS", "2", "$BLOB", "ABCDEFG==");
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
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "COMBINE", "LINEAR", "0.7", "0.3");

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
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "COMBINE", "RRF");

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



TEST_F(ParseHybridTest, testComplexSingleLineCommand) {
  QueryError status = {QueryErrorCode(0)};

  // Example of a complex command in a single line
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "2", "K", "10",
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
    "VSIM vector $BLOB KNN 2 K 10 "
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

// Tests for parseVectorSubquery functionality (VSIM tests)

TEST_F(ParseHybridTest, testVsimBasicKNNWithFilter) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "@title:hello=>[KNN 10 @vector $BLOB]=>{$YIELD_DISTANCE_AS: testdist; $EF_RUNTIME: 4;}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "6", "K", "10", "YIELD_DISTANCE_AS", "testdist","EF_RUNTIME", "4", "FILTER", "@title:hello"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  // Create a fresh sctx for this test since parseHybridRequest takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for KNN query
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
  ASSERT_STREQ(vn->opts.distField, "testdist");

  // Verify parameters
  ASSERT_EQ(QueryNode_NumParams(vn), 1);
  ASSERT_STREQ(vn->params[0].name, "BLOB");
  ASSERT_EQ(vn->params[0].len, 4);
  ASSERT_EQ(vn->params[0].type, PARAM_VEC);
  ASSERT_TRUE(vn->params[0].target != NULL);
  ASSERT_TRUE(vn->params[0].target_len != NULL);
  ASSERT_EQ(vn->params[0].sign, 0);

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 10);
  ASSERT_EQ(vq->knn.order, BY_SCORE);

  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNWithEFRuntime) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "@title:hello=>[KNN 10 @vector $BLOB EF_RUNTIME 80]=>{$YIELD_DISTANCE_AS: testdist;}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "6", "K", "10", "EF_RUNTIME", "80", "YIELD_DISTANCE_AS", "testdist", "FILTER", "@title:hello"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  // Create a fresh sctx for this test since parseHybridRequest takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for KNN query with EF_RUNTIME
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
  ASSERT_STREQ(vn->opts.distField, "testdist");

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
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

  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimBasicKNNNoFilter) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "*=>[KNN 5 @vector $BLOB]" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "2", "K", "5"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  // Create a fresh sctx for this test since parseHybridRequest takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for basic KNN query without filter
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
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
  ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 5);
  ASSERT_EQ(vq->knn.order, BY_SCORE);

  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNWithYieldDistanceOnly) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "*=>[KNN 8 @vector $BLOB]=>{$YIELD_DISTANCE_AS: distance_score;}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "4", "K", "8", "YIELD_DISTANCE_AS", "distance_score"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  // Create a fresh sctx for this test since parseHybridRequest takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

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
  ASSERT_EQ(vq->type, VECSIM_QT_KNN);
  ASSERT_EQ(vq->knn.k, 8);
  ASSERT_EQ(vq->knn.order, BY_SCORE);

  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeBasic) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "@vector:[VECTOR_RANGE 0.5 $BLOB]" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "2", "RADIUS", "0.5"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  // Create a fresh sctx for this test since parseHybridRequest takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for basic RANGE query
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
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
  ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_EQ(vq->type, VECSIM_QT_RANGE);
  ASSERT_EQ(vq->range.radius, 0.5);
  ASSERT_EQ(vq->range.order, BY_SCORE);

  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeWithEpsilon) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "@vector:[VECTOR_RANGE 0.8 $BLOB]=>{$EPSILON: 0.01; $YIELD_DISTANCE_AS: dist}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "6", "RADIUS", "0.8", "EPSILON", "0.01", "YIELD_DISTANCE_AS", "dist"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  // Create a fresh sctx for this test since parseHybridRequest takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = result->requests[1];

  // Verify AST structure for RANGE query with EPSILON
  ASSERT_TRUE(vecReq->ast.root != NULL);
  ASSERT_EQ(vecReq->ast.root->type, QN_VECTOR);

  // Verify QueryNode structure
  QueryNode *vn = vecReq->ast.root;
  ASSERT_EQ(vn->opts.flags & QueryNode_YieldsDistance, QueryNode_YieldsDistance);
  ASSERT_STREQ(vn->opts.distField, "dist");

  // Verify VectorQuery structure
  VectorQuery *vq = vn->vn.vq;
  ASSERT_TRUE(vq != NULL);
  ASSERT_TRUE(vq->field != NULL);
  ASSERT_STREQ(vq->scoreField, "__vector_score");
  ASSERT_EQ(vq->type, VECSIM_QT_RANGE);
  ASSERT_EQ(vq->range.radius, 0.8);
  ASSERT_EQ(vq->range.order, BY_SCORE);

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

  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

// Error handling tests for parseVectorSubquery

TEST_F(ParseHybridTest, testVsimSubqueryWrongParamCount) {
  QueryError status = {QueryErrorCode(0)};

  // Test with wrong parameter count
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "\"hello\"", "VSIM", "vector", "$BLOB", "KNN", "4", "K", "10", "FILTER", "@text:hello",
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  // Verify the request was parsed unsuccessfully
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNOddParamCount) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN with count=1 (odd count, missing K value)
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "1", "K"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeOddParamCount) {
  QueryError status = {QueryErrorCode(0)};

  // Test RANGE with count=3 (odd count, missing EPSILON value)
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "3", "RADIUS", "0.5", "EPSILON"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimSubqueryMissingK) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN without K parameter
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "2", "EF_RUNTIME", "100"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNDuplicateK) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN with duplicate K parameters
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "4", "K", "10", "K", "20"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateRadius) {
  QueryError status = {QueryErrorCode(0)};

  // Test RANGE with duplicate RADIUS parameters
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "RADIUS", "0.8"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNWithEpsilon) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN with EPSILON (should be RANGE-only)
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "4", "K", "10", "EPSILON", "0.01"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeWithEFRuntime) {
  QueryError status = {QueryErrorCode(0)};

  // Test RANGE with EF_RUNTIME (should be KNN-only)
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "EF_RUNTIME", "100"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNDuplicateEFRuntime) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN with duplicate EF_RUNTIME parameters
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "6", "K", "10", "EF_RUNTIME", "100", "EF_RUNTIME", "200"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateEpsilon) {
  QueryError status = {QueryErrorCode(0)};

  // Test RANGE with duplicate EPSILON parameters
  std::vector<const char*> args = {
    "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "6", "RADIUS", "0.5", "EPSILON", "0.01", "EPSILON", "0.02"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  // Create a fresh sctx for this test
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, test_sctx, index_name.c_str(), &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);

  // Clean up
  SearchCtx_Free(test_sctx);
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}
