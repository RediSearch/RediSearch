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
  QueryError status = {QueryErrorCode(0)};

  // Parse hybrid request
  std::vector<const char*> hybridArgs = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "4", "K", "10", "YIELD_DISTANCE_AS", "testdist", "FILTER", "@title:hello"
  };
  RedisModuleString** argv = createStringArray(hybridArgs);
  int argc = hybridArgs.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  AREQ* vecReq = &result->requests[1];
  char *vecReqExplain = QAST_DumpExplain(&vecReq->ast, sctx->spec);

  // Parse equivalent FT.SEARCH request
  std::vector<const char*> equivalentArgs = {
    "FT.SEARCH", "testidx", "@title:hello=>[KNN 10 @vector $BLOB]=>{$YIELD_DISTANCE_AS: testdist;}", "PARAMS", "2", "BLOB", "\x12\xa9\xf5\x6c", "DIALECT", "2"
  };
  RedisModuleString** argv_equiv = createStringArray(equivalentArgs);
  int argc_equiv = equivalentArgs.size();

  QueryError s = {QueryErrorCode(0)};
  AREQ *equivalentREQ = AREQ_New();
  prepareRequest(&equivalentREQ, ctx, argv_equiv, argc_equiv, COMMAND_SEARCH, 0, &s);
  ASSERT_EQ(s.code, QUERY_OK);

  char* equivalentREQExplain = QAST_DumpExplain(&equivalentREQ->ast, sctx->spec);

  // Compare the AST explanations
  ASSERT_STREQ(vecReqExplain, equivalentREQExplain);
}

TEST_F(ParseHybridTest, testVsimKNNWithEFRuntime) {
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
  char *vecReqExplain = QAST_DumpExplain(&vecReq->ast, sctx->spec);

  // Parse equivalent FT.SEARCH request
  std::vector<const char*> equivalentArgs = {
    "FT.SEARCH", "testidx", "@title:hello=>[KNN 10 @vector $BLOB EF_RUNTIME 80]=>{$YIELD_DISTANCE_AS: testdist;}", "PARAMS", "2", "BLOB", "\x12\xa9\xf5\x6c", "DIALECT", "2"
  };
  RedisModuleString** argv_equiv = createStringArray(equivalentArgs);
  int argc_equiv = equivalentArgs.size();

  QueryError s = {QueryErrorCode(0)};
  AREQ *equivalentREQ = AREQ_New();
  prepareRequest(&equivalentREQ, ctx, argv_equiv, argc_equiv, COMMAND_SEARCH, 0, &s);
  ASSERT_EQ(s.code, QUERY_OK);

  char* equivalentREQExplain = QAST_DumpExplain(&equivalentREQ->ast, sctx->spec);

  // Compare the AST explanations
  ASSERT_STREQ(vecReqExplain, equivalentREQExplain);
}

TEST_F(ParseHybridTest, testVsimBasicKNNNoFilter) {
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
  char *vecReqExplain = QAST_DumpExplain(&vecReq->ast, sctx->spec);

  // Parse equivalent FT.SEARCH request
  std::vector<const char*> equivalentArgs = {
    "FT.SEARCH", "testidx", "*=>[KNN 5 @vector $BLOB]", "PARAMS", "2", "BLOB", "\x12\xa9\xf5\x6c", "DIALECT", "2"
  };
  RedisModuleString** argv_equiv = createStringArray(equivalentArgs);
  int argc_equiv = equivalentArgs.size();

  QueryError s = {QueryErrorCode(0)};
  AREQ *equivalentREQ = AREQ_New();
  prepareRequest(&equivalentREQ, ctx, argv_equiv, argc_equiv, COMMAND_SEARCH, 0, &s);
  ASSERT_EQ(s.code, QUERY_OK);

  char* equivalentREQExplain = QAST_DumpExplain(&equivalentREQ->ast, sctx->spec);

  // Compare the AST explanations
  ASSERT_STREQ(vecReqExplain, equivalentREQExplain);
}

TEST_F(ParseHybridTest, testVsimKNNWithYieldDistanceOnly) {
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
  char *vecReqExplain = QAST_DumpExplain(&vecReq->ast, sctx->spec);

  // Parse equivalent FT.SEARCH request
  std::vector<const char*> equivalentArgs = {
    "FT.SEARCH", "testidx", "*=>[KNN 8 @vector $BLOB]=>{$YIELD_DISTANCE_AS: distance_score;}", "PARAMS", "2", "BLOB", "\x12\xa9\xf5\x6c", "DIALECT", "2"
  };
  RedisModuleString** argv_equiv = createStringArray(equivalentArgs);
  int argc_equiv = equivalentArgs.size();

  QueryError s = {QueryErrorCode(0)};
  AREQ *equivalentREQ = AREQ_New();
  prepareRequest(&equivalentREQ, ctx, argv_equiv, argc_equiv, COMMAND_SEARCH, 0, &s);
  ASSERT_EQ(s.code, QUERY_OK);

  char* equivalentREQExplain = QAST_DumpExplain(&equivalentREQ->ast, sctx->spec);

  // Compare the AST explanations
  ASSERT_STREQ(vecReqExplain, equivalentREQExplain);
}

TEST_F(ParseHybridTest, testVsimRangeBasic) {
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
  char *vecReqExplain = QAST_DumpExplain(&vecReq->ast, sctx->spec);

  // Parse equivalent FT.SEARCH request
  std::vector<const char*> equivalentArgs = {
    "FT.SEARCH", "testidx", "@vector:[VECTOR_RANGE 0.5 $BLOB]", "PARAMS", "2", "BLOB", "\x12\xa9\xf5\x6c", "DIALECT", "2"
  };
  RedisModuleString** argv_equiv = createStringArray(equivalentArgs);
  int argc_equiv = equivalentArgs.size();

  QueryError s = {QueryErrorCode(0)};
  AREQ *equivalentREQ = AREQ_New();
  prepareRequest(&equivalentREQ, ctx, argv_equiv, argc_equiv, COMMAND_SEARCH, 0, &s);
  ASSERT_EQ(s.code, QUERY_OK);

  char* equivalentREQExplain = QAST_DumpExplain(&equivalentREQ->ast, sctx->spec);

  // Compare the AST explanations
  ASSERT_STREQ(vecReqExplain, equivalentREQExplain);
}

TEST_F(ParseHybridTest, testVsimRangeWithEpsilon) {
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
  char *vecReqExplain = QAST_DumpExplain(&vecReq->ast, sctx->spec);

  // Parse equivalent FT.SEARCH request
  std::vector<const char*> equivalentArgs = {
    "FT.SEARCH", "testidx", "@vector:[VECTOR_RANGE 0.8 $BLOB]=>{$EPSILON: 00.1; $YIELD_DISTANCE_AS: dist}", "PARAMS", "2", "BLOB", "\x12\xa9\xf5\x6c", "DIALECT", "2"
  };
  RedisModuleString** argv_equiv = createStringArray(equivalentArgs);
  int argc_equiv = equivalentArgs.size();

  QueryError s = {QueryErrorCode(0)};
  AREQ *equivalentREQ = AREQ_New();
  prepareRequest(&equivalentREQ, ctx, argv_equiv, argc_equiv, COMMAND_SEARCH, 0, &s);
  ASSERT_EQ(s.code, QUERY_OK);

  char* equivalentREQExplain = QAST_DumpExplain(&equivalentREQ->ast, sctx->spec);

  // Compare the AST explanations
  ASSERT_STREQ(vecReqExplain, equivalentREQExplain);
}

// TEST_F(ParseHybridTest, testVsimSubqueryWrongParamCount) {
//   QueryError status = {QueryErrorCode(0)};

//   // Test with RRF combine method: FT.HYBRID testidx SEARCH "hello" VSIM "world" COMBINE RRF
//   std::vector<const char*> args = {
//     "FT.HYBRID" ,"idx" ,"SEARCH" ,"\"hello\"" ,"VSIM" ,"vector" ,"ABCDEFG==" ,"KNN" ,"4" ,"K" ,"10" ,"FILTER" ,"@text:hello",
//   };

//   RedisModuleString** argv = createStringArray(args);
//   int argc = args.size();

//   HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

//   // Verify the request was parsed unsuccessfully
//   ASSERT_TRUE(result == NULL);
//   ASSERT_EQ(status.code, QUERY_EPARSEARGS);
//   // Clean up
//   HybridRequest_Free(result);
//   freeStringArray(argv, argc);
// }


// TEST_F(ParseHybridTest, testBasicValidInput) {
//   QueryError status = {QueryErrorCode(0)};

//   // Create a basic hybrid query: FT.HYBRID testidx SEARCH "hello" VSIM vector $BLOB KNN 2 K 5
//   std::vector<const char*> args = {
//     "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "vector", "$BLOB", "KNN", "2", "K", "5"
//   };

//   RedisModuleString** argv = createStringArray(args);
//   int argc = args.size();

//   HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

//   // Debug: Print error if parsing failed
//   if (result == NULL) {
//     printf("Parsing failed with error code: %d, message: %s\n", status.code, status.detail ? status.detail : "No detail");
//   }

//   // Verify the request was parsed successfully
//   ASSERT_TRUE(result != NULL);
//   ASSERT_EQ(status.code, QUERY_OK);

//   // Verify the structure contains expected number of requests
//   ASSERT_EQ(result->nrequests, 2);

//   // Verify default scoring type is RRF
//   ASSERT_EQ(result->combineCtx.scoringType, HYBRID_SCORING_RRF);

//   // Clean up handled later
// }

// TEST_F(ParseHybridTest, testMissingSearchParameter) {
//   QueryError status = {QueryErrorCode(0)};

//   // Missing SEARCH parameter: FT.HYBRID testidx VSIM @vector_field
//   std::vector<const char*> args = {
//     "FT.HYBRID", "testidx", "VSIM", "@vector_field"
//   };

//   RedisModuleString** argv = createStringArray(args);
//   int argc = args.size();

//   HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

//   // Verify parsing failed with appropriate error
//   ASSERT_TRUE(result == NULL);
//   ASSERT_EQ(status.code, QUERY_ESYNTAX);

//   // Clean up
//   QueryError_ClearError(&status);
//   freeStringArray(argv, argc);
// }

// TEST_F(ParseHybridTest, testMissingSecondSearchParameter) {
//   QueryError status = {QueryErrorCode(0)};

//   // Missing second search parameter: FT.HYBRID testidx SEARCH "hello"
//   std::vector<const char*> args = {
//     "FT.HYBRID", "testidx", "SEARCH", "hello"
//   };

//   RedisModuleString** argv = createStringArray(args);
//   int argc = args.size();

//   HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

//   // Verify parsing failed with appropriate error
//   ASSERT_TRUE(result == NULL);
//   ASSERT_EQ(status.code, QUERY_ESYNTAX);

//   // Clean up
//   QueryError_ClearError(&status);
//   freeStringArray(argv, argc);
// }

// TEST_F(ParseHybridTest, testWithCombineLinear) {
//   QueryError status = {QueryErrorCode(0)};

//   // Test with LINEAR combine method: FT.HYBRID testidx SEARCH "hello" VSIM "world" COMBINE LINEAR 0.7 0.3
//   std::vector<const char*> args = {
//     "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "world",
//     "COMBINE", "LINEAR", "0.7", "0.3"
//   };

//   RedisModuleString** argv = createStringArray(args);
//   int argc = args.size();

//   HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

//   // Verify the request was parsed successfully
//   ASSERT_TRUE(result != NULL);
//   ASSERT_EQ(status.code, QUERY_OK);

//   // Verify LINEAR scoring type was set
//   ASSERT_EQ(result->combineCtx.scoringType, HYBRID_SCORING_LINEAR);

//   // Clean up
//   HybridRequest_Free(result);
//   freeStringArray(argv, argc);
// }

// TEST_F(ParseHybridTest, testWithCombineRRF) {
//   QueryError status = {QueryErrorCode(0)};

//   // Test with RRF combine method: FT.HYBRID testidx SEARCH "hello" VSIM "world" COMBINE RRF
//   std::vector<const char*> args = {
//     "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "world",
//     "COMBINE", "RRF"
//   };

//   RedisModuleString** argv = createStringArray(args);
//   int argc = args.size();

//   HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

//   // Verify the request was parsed successfully
//   ASSERT_TRUE(result != NULL);
//   ASSERT_EQ(status.code, QUERY_OK);

//   // Verify RRF scoring type was set
//   ASSERT_EQ(result->combineCtx.scoringType, HYBRID_SCORING_RRF);

//   // Clean up
//   HybridRequest_Free(result);
//   freeStringArray(argv, argc);
// }



