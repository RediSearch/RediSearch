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

TEST_F(ParseHybridTest, testBasicValidInput) {
  QueryError status = {QueryErrorCode(0)};

  // Create a basic hybrid query: FT.HYBRID testidx SEARCH "hello" VSIM "world"
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "world"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify the structure contains expected number of requests
  ASSERT_EQ(result->nrequests, 2);

  // Verify default scoring type is RRF
  ASSERT_EQ(result->combineCtx.scoringType, HYBRID_SCORING_RRF);

  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testMissingSearchParameter) {
  QueryError status = {QueryErrorCode(0)};

  // Missing SEARCH parameter: FT.HYBRID testidx VSIM @vector_field
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "VSIM", "@vector_field"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_ESYNTAX);

  // Clean up
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testMissingSecondSearchParameter) {
  QueryError status = {QueryErrorCode(0)};

  // Missing second search parameter: FT.HYBRID testidx SEARCH "hello"
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  // Verify parsing failed with appropriate error
  ASSERT_TRUE(result == NULL);
  ASSERT_EQ(status.code, QUERY_ESYNTAX);

  // Clean up
  QueryError_ClearError(&status);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testWithCombineLinear) {
  QueryError status = {QueryErrorCode(0)};

  // Test with LINEAR combine method: FT.HYBRID testidx SEARCH "hello" VSIM "world" COMBINE LINEAR 0.7 0.3
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "world",
    "COMBINE", "LINEAR", "0.7", "0.3"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify LINEAR scoring type was set
  ASSERT_EQ(result->combineCtx.scoringType, HYBRID_SCORING_LINEAR);

  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

TEST_F(ParseHybridTest, testWithCombineRRF) {
  QueryError status = {QueryErrorCode(0)};

  // Test with RRF combine method: FT.HYBRID testidx SEARCH "hello" VSIM "world" COMBINE RRF
  std::vector<const char*> args = {
    "FT.HYBRID", "testidx", "SEARCH", "hello", "VSIM", "world",
    "COMBINE", "RRF"
  };

  RedisModuleString** argv = createStringArray(args);
  int argc = args.size();

  HybridRequest* result = parseHybridRequest(ctx, argv, argc, sctx, &status);

  // Verify the request was parsed successfully
  ASSERT_TRUE(result != NULL);
  ASSERT_EQ(status.code, QUERY_OK);

  // Verify RRF scoring type was set
  ASSERT_EQ(result->combineCtx.scoringType, HYBRID_SCORING_RRF);

  // Clean up
  HybridRequest_Free(result);
  freeStringArray(argv, argc);
}

// TEST_F(ParseHybridTest, TestParseVectorSubquery) {
//   // Test basic KNN query transformation
//   const char* args[] = {"FT.HYBRID", "idx", "SEARCH", "@title:hello", "VSIM", "@v \"[1,2,3]\"", "KNN", "10"};
//   int argc = sizeof(args) / sizeof(args[0]);
//   char** argv = createStringArray(args, argc);

//   QueryError status = {QueryErrorCode(0)};
//   HybridRequest* result = HybridRequest_Parse(argv, argc, &status);

//   // Verify the request was parsed successfully
//   ASSERT_TRUE(result != NULL);
//   ASSERT_EQ(status.code, QUERY_OK);

//   // Verify the vector query was transformed correctly
//   ASSERT_TRUE(result->requests != NULL);
//   ASSERT_GT(result->nrequests, 0);

//   // Check that the vector request has the transformed query
//   AREQ* vectorReq = &result->requests[1]; // Second request should be vector
//   ASSERT_TRUE(vectorReq->query != NULL);

//   // The query should be transformed to old syntax: @v:[KNN 10 $vec]
//   std::string queryStr(vectorReq->query);
//   ASSERT_TRUE(queryStr.find("@v:[KNN 10 $vec]") != std::string::npos);

//   // Clean up
//   HybridRequest_Free(result);
//   freeStringArray(argv, argc);
// }

// TEST_F(ParseHybridTest, TestParseVectorSubqueryWithFilter) {
//   // Test KNN query with filter transformation
//   const char* args[] = {"FT.HYBRID", "idx", "SEARCH", "@title:hello", "VSIM", "@v \"[1,2,3]\"", "KNN", "5", "FILTER", "text:(world)"};
//   int argc = sizeof(args) / sizeof(args[0]);
//   char** argv = createStringArray(args, argc);

//   QueryError status = {QueryErrorCode(0)};
//   HybridRequest* result = HybridRequest_Parse(argv, argc, &status);

//   // Verify the request was parsed successfully
//   ASSERT_TRUE(result != NULL);
//   ASSERT_EQ(status.code, QUERY_OK);

//   // Verify the vector query was transformed correctly
//   ASSERT_TRUE(result->requests != NULL);
//   ASSERT_GT(result->nrequests, 0);

//   // Check that the vector request has the transformed query and filter
//   AREQ* vectorReq = &result->requests[1]; // Second request should be vector
//   ASSERT_TRUE(vectorReq->query != NULL);
//   ASSERT_TRUE(vectorReq->filterClause != NULL);

//   // The query should be transformed to: (text:(world))=>{@v:[KNN 5 $vec]}
//   std::string queryStr(vectorReq->query);
//   ASSERT_TRUE(queryStr.find("(text:(world))=>{@v:[KNN 5 $vec]}") != std::string::npos);

//   // The filter clause should be stored separately
//   std::string filterStr(vectorReq->filterClause);
//   ASSERT_EQ(filterStr, "text:(world)");

//   // Clean up
//   HybridRequest_Free(result);
//   freeStringArray(argv, argc);
// }
