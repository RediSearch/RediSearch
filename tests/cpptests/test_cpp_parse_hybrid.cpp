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
#include "src/aggregate/aggregate.h"
#include "src/vector_index.h"
#include "VecSim/query_results.h"


class ParseHybridTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx;
  IndexSpec *spec;
  RedisSearchCtx *sctx;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);

    // Create a simple index for testing (without vector field to avoid setup complexity)
    QueryError qerr = {QueryErrorCode(0)};
    RMCK::ArgvList args(ctx, "FT.CREATE", "testidx", "ON", "HASH",
                        "SCHEMA", "title", "TEXT", "vector", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "COSINE");
    spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
    ASSERT_TRUE(spec);
    RSDoc* d = RediSearch_CreateDocument("doc:1", strlen("doc:1"), 1.0, NULL);
    RediSearch_DocumentAddFieldCString(d, "title", "another indexing testing",
                                     RSFLDTYPE_FULLTEXT);
    RediSearch_DocumentAddFieldCString(d, "vector", "ABCDEFG==", RSFLDTYPE_VECTOR);
    RediSearch_SpecAddDocument(spec->own_ref.rm, d);


    sctx = NewSearchCtxC(ctx, "testidx", true);
    ASSERT_TRUE(sctx != NULL);
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


TEST_F(ParseHybridTest, testVsimBasicKNNWithFilter) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "@title:hello=>[KNN 10 @vector $BLOB]=>{$YIELD_DISTANCE_AS: testdist; $EF_RUNTIME: 4;}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "6", "K", "10", "YIELD_DISTANCE_AS", "testdist","EF_RUNTIME", "4", "FILTER", "@title:hello"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = &result->requests[1];

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

  // HybridRequest_Free(result);
  // freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNWithEFRuntime) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "@title:hello=>[KNN 10 @vector $BLOB EF_RUNTIME 80]=>{$YIELD_DISTANCE_AS: testdist;}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "6", "K", "10", "EF_RUNTIME", "80", "YIELD_DISTANCE_AS", "testdist", "FILTER", "@title:hello"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = &result->requests[1];

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
}

TEST_F(ParseHybridTest, testVsimBasicKNNNoFilter) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "*=>[KNN 5 @vector $BLOB]" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "2", "K", "5"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = &result->requests[1];

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
}

TEST_F(ParseHybridTest, testVsimKNNWithYieldDistanceOnly) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "*=>[KNN 8 @vector $BLOB]=>{$YIELD_DISTANCE_AS: distance_score;}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "4", "K", "8", "YIELD_DISTANCE_AS", "distance_score"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = &result->requests[1];

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
}

TEST_F(ParseHybridTest, testVsimRangeBasic) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "@vector:[VECTOR_RANGE 0.5 $BLOB]" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "2", "RADIUS", "0.5"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = &result->requests[1];

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
}

TEST_F(ParseHybridTest, testVsimRangeWithEpsilon) {
  // For equivalence check and easy to read representation, can run this equivalent query:
  // FT.SEARCH testidx "@vector:[VECTOR_RANGE 0.8 $BLOB]=>{$EPSILON: 0.01; $YIELD_DISTANCE_AS: dist}" PARAMS 2 BLOB "\x12\xa9\xf5\x6c" DIALECT 2

  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "6", "RADIUS", "0.8", "EPSILON", "0.01", "YIELD_DISTANCE_AS", "dist"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = &result->requests[1];

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
}


TEST_F(ParseHybridTest, testVsimSubqueryWrongParamCount) {
  QueryError status = {QueryErrorCode(0)};

  // Test with RRF combine method: FT.HYBRID testidx SEARCH "hello" VSIM "world" COMBINE RRF
  std::vector<const char*> args = {
    "FT.HYBRID" ,"idx" ,"SEARCH" ,"\"hello\"" ,"VSIM" ,"vector" ,"$BLOB" ,"KNN" ,"4" ,"K" ,"10" ,"FILTER" ,"@text:hello",
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  // Verify the request was parsed unsuccessfully
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNOddParamCount) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN with count=1 (odd count, missing K value)
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "1", "K"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeOddParamCount) {
  QueryError status = {QueryErrorCode(0)};

  // Test RANGE with count=3 (odd count, missing EPSILON value)
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "3", "RADIUS", "0.5", "EPSILON"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimSubqueryMissingK) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN without K parameter
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "2", "EF_RUNTIME", "100"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNDuplicateK) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN with duplicate K parameters
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "4", "K", "10", "K", "20"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateRadius) {
  QueryError status = {QueryErrorCode(0)};

  // Test RANGE with duplicate RADIUS parameters
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "RADIUS", "0.8"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNWithEpsilon) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN with EPSILON (should be RANGE-only)
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "4", "K", "10", "EPSILON", "0.01"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeWithEFRuntime) {
  QueryError status = {QueryErrorCode(0)};

  // Test RANGE with EF_RUNTIME (should be KNN-only)
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "4", "RADIUS", "0.5", "EF_RUNTIME", "100"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimKNNDuplicateEFRuntime) {
  QueryError status = {QueryErrorCode(0)};

  // Test KNN with duplicate EF_RUNTIME parameters
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "6", "K", "10", "EF_RUNTIME", "100", "EF_RUNTIME", "200"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testVsimRangeDuplicateEpsilon) {
  QueryError status = {QueryErrorCode(0)};

  // Test RANGE with duplicate EPSILON parameters
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "RANGE", "6", "RADIUS", "0.5", "EPSILON", "0.01", "EPSILON", "0.02"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_EPARSEARGS);
  freeStringArray(argv, argc);
}
