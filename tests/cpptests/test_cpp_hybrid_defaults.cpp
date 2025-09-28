#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "spec.h"
#include "hybrid/parse_hybrid.h"
#include "hybrid/hybrid_scoring.h"
#include "common.h"

class HybridDefaultsTest : public ::testing::Test {
protected:
  RedisModuleCtx *ctx;
  std::string index_name;
  RedisSearchCtx *sctx;
  HybridRequest *result;  // Member to hold current test result
  HybridPipelineParams hybridParams;
  ParseHybridCommandCtx parseCtx;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);
    // Generate unique index name for each test
    static int test_counter = 0;
    index_name = "testidx" + std::to_string(++test_counter);

    // Create index with vector field using IndexSpec_CreateNew like other tests
    QueryError qerr = {QueryErrorCode(0)};
    RMCK::ArgvList createArgs(ctx, "FT.CREATE", index_name.c_str(), "ON", "HASH",
                              "SCHEMA", "title", "TEXT", "content", "TEXT",
                              "vector", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "COSINE");
    IndexSpec *spec = IndexSpec_CreateNew(ctx, createArgs, createArgs.size(), &qerr);
    if (!spec) {
      printf("Failed to create index '%s': code=%d, detail='%s'\n",
             index_name.c_str(), qerr.code, qerr.detail ? qerr.detail : "NULL");
      QueryError_ClearError(&qerr);
    }
    ASSERT_TRUE(spec);

    sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
    ASSERT_TRUE(sctx != NULL);
    result = MakeDefaultHybridRequest(sctx);
    hybridParams = {0};
    parseCtx.search = result->requests[0];
    parseCtx.vector = result->requests[1];
    parseCtx.tailPlan = &result->tailPipeline->ap;
    parseCtx.hybridParams = &hybridParams;
  }

  void TearDown() override {
    // Free the result if it was set during the test
    if (result) {
      HybridRequest_Free(result);
    }
    if (hybridParams.scoringCtx) {
      HybridScoringContext_Free(hybridParams.scoringCtx);
    }
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }

  /**
   * Helper function to parse and validate hybrid command with common boilerplate.
   * Handles initialization, parsing, validation, and stores result in member variable.
   *
   * @param args The command arguments to parse
   * @return Pointer to the parsed HybridRequest (also stored in member variable)
   */
  HybridRequest *parseCommand(RMCK::ArgvList& args) {
    QueryError status = {QueryErrorCode(0)};

    EXPECT_TRUE(result->sctx != NULL) << "Failed to create search context";

    ParseHybridCommandCtx cmd = {0};
    cmd.search = result->requests[0];
    cmd.vector = result->requests[1];
    cmd.tailPlan = &result->tailPipeline->ap;
    cmd.hybridParams = &hybridParams;
    cmd.reqConfig = &result->reqConfig;
    cmd.cursorConfig = &result->cursorConfig;

    int rc =  parseHybridCommand(ctx, args, args.size(), result->sctx, index_name.c_str(), &cmd, &status, true);
    if (rc != REDISMODULE_OK) {
      HybridRequest_Free(result);
      result = nullptr;
    }

    EXPECT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");
    EXPECT_TRUE(result != nullptr) << "parseHybridCommand returned NULL";

    return result;
  }

static void validateDefaultParams(HybridRequest* result, ParseHybridCommandCtx& parseCtx,
    size_t expectedWindow, size_t expectedKnnK) {
    ASSERT_TRUE(result != NULL);
    ASSERT_TRUE(parseCtx.hybridParams->scoringCtx != NULL);

    // Verify RRF-specific parameters (only for RRF)
    if (parseCtx.hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF) {
        ASSERT_EQ(expectedWindow, parseCtx.hybridParams->scoringCtx->rrfCtx.window)
            << "Expected window=" << expectedWindow << ", got " << parseCtx.hybridParams->scoringCtx->rrfCtx.window;

        // Verify RRF k default
        ASSERT_DOUBLE_EQ(HYBRID_DEFAULT_RRF_CONSTANT, parseCtx.hybridParams->scoringCtx->rrfCtx.constant)
            << "Expected RRF constant=" << HYBRID_DEFAULT_RRF_CONSTANT;
    }

    // Verify KNN K value
    ASSERT_TRUE(result->requests != NULL);
    ASSERT_TRUE(result->nrequests >= 2) << "Expected at least 2 requests, got " << result->nrequests;
    AREQ* vectorRequest = result->requests[1];
    ASSERT_TRUE(vectorRequest != NULL);
    ASSERT_TRUE(vectorRequest->ast.root != NULL);
    ASSERT_EQ(vectorRequest->ast.root->type, QN_VECTOR);
    VectorQuery *vq = vectorRequest->ast.root->vn.vq;
    ASSERT_TRUE(vq != NULL);
    ASSERT_EQ(vq->type, VECSIM_QT_KNN);
    ASSERT_EQ(expectedKnnK, vq->knn.k)
        << "Expected KNN K=" << expectedKnnK << ", got " << vq->knn.k;
  }
};

const char* TEST_BLOB_DATA = "\x12\xa9\xf5\x6c";

// All defaults applied
TEST_F(HybridDefaultsTest, testDefaultValues) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA);

  parseCommand(args);
  validateDefaultParams(result, parseCtx, HYBRID_DEFAULT_WINDOW, HYBRID_DEFAULT_KNN_K);
}

// LIMIT affects both implicit parameters
TEST_F(HybridDefaultsTest, testLimitFallbackBoth) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "LIMIT", "0", "25");

  parseCommand(args);
  validateDefaultParams(result, parseCtx, 25, 25);
}

// LIMIT affects only implicit K, but K gets capped at explicit WINDOW
TEST_F(HybridDefaultsTest, testLimitFallbackKOnly) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "RRF", "2", "WINDOW", "15", "LIMIT", "0", "25");

  parseCommand(args);
  // K should be capped at WINDOW=15 even though LIMIT fallback would set it to 25
  validateDefaultParams(result, parseCtx, 15, 15);
}

// LIMIT affects only implicit WINDOW
TEST_F(HybridDefaultsTest, testLimitFallbackWindowOnly) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8", "LIMIT", "0", "25");

  parseCommand(args);
  validateDefaultParams(result, parseCtx, 25, 8);
}

// Explicit parameters override LIMIT
TEST_F(HybridDefaultsTest, testExplicitOverridesLimit) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8", "COMBINE", "RRF", "2", "WINDOW", "15", "LIMIT", "0", "25");

  parseCommand(args);
  validateDefaultParams(result, parseCtx, 15, 8);
}

// Large LIMIT values work
TEST_F(HybridDefaultsTest, testLargeLimitFallback) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "LIMIT", "0", "10000");

  parseCommand(args);
  validateDefaultParams(result, parseCtx, 10000, 10000);
}

// Flag verification tests
TEST_F(HybridDefaultsTest, testFlagTrackingImplicitBoth) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA);

  parseCommand(args);
  // Both flags should be false
  ASSERT_FALSE(parseCtx.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
  ASSERT_FALSE(result->requests[1]->parsedVectorData->hasExplicitK);
}

TEST_F(HybridDefaultsTest, testFlagTrackingExplicitK) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8");

  parseCommand(args);
  // K explicit, WINDOW implicit
  ASSERT_TRUE(result->requests[1]->parsedVectorData->hasExplicitK);
  ASSERT_FALSE(parseCtx.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(HybridDefaultsTest, testFlagTrackingExplicitWindow) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "RRF", "2", "WINDOW", "15");

  parseCommand(args);
  // WINDOW explicit, K implicit
  ASSERT_TRUE(parseCtx.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
  ASSERT_FALSE(result->requests[1]->parsedVectorData->hasExplicitK);
}

TEST_F(HybridDefaultsTest, testFlagTrackingExplicitBoth) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8", "COMBINE", "RRF", "2", "WINDOW", "15");

  parseCommand(args);
  // Both flags should be true
  ASSERT_TRUE(result->requests[1]->parsedVectorData->hasExplicitK);
  ASSERT_TRUE(parseCtx.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(HybridDefaultsTest, testLinearDefaults) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "LINEAR", "4", "ALPHA", "0.6", "BETA", "0.4");

  parseCommand(args);
  // LINEAR should not have window parameter (uses regular limit instead)
  ASSERT_EQ(parseCtx.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);

  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(HYBRID_DEFAULT_KNN_K, vq->knn.k)
      << "Expected KNN k=" << HYBRID_DEFAULT_KNN_K << ", got " << vq->knn.k;
}

// Test K ≤ WINDOW constraint: explicit K > explicit WINDOW should cap K to WINDOW
TEST_F(HybridDefaultsTest, testKCappedAtExplicitWindow) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "50", "COMBINE", "RRF", "2", "WINDOW", "15");

  parseCommand(args);
  // Verify K was capped to WINDOW value
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(15, vq->knn.k) << "Expected K to be capped at WINDOW=15, got " << vq->knn.k;
  ASSERT_EQ(15, parseCtx.hybridParams->scoringCtx->rrfCtx.window);
}

// Test K ≤ WINDOW constraint: K from LIMIT fallback > explicit WINDOW should cap K to WINDOW
TEST_F(HybridDefaultsTest, testKFromLimitCappedAtExplicitWindow) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "RRF", "2", "WINDOW", "12", "LIMIT", "0", "30");

  parseCommand(args);
  // K should be capped to WINDOW (12) even though LIMIT fallback would set it to 30
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(12, vq->knn.k) << "Expected K to be capped at WINDOW=12, got " << vq->knn.k;
  ASSERT_EQ(12, parseCtx.hybridParams->scoringCtx->rrfCtx.window);
}

// Test K ≤ WINDOW constraint: explicit K > WINDOW from LIMIT fallback should cap K to WINDOW
TEST_F(HybridDefaultsTest, testExplicitKCappedAtWindowFromLimit) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "25", "COMBINE", "RRF", "LIMIT", "0", "18");

  parseCommand(args);
  // K should be capped to WINDOW (18 from LIMIT fallback) even though K was explicitly set to 25
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(18, vq->knn.k) << "Expected K to be capped at WINDOW=18, got " << vq->knn.k;
  ASSERT_EQ(18, parseCtx.hybridParams->scoringCtx->rrfCtx.window);
}

// Test that Linear scoring is unaffected by K ≤ WINDOW constraint
TEST_F(HybridDefaultsTest, testLinearScoringUnaffectedByKWindowConstraint) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "50", "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3");

  parseCommand(args);
  // Linear scoring should not apply K ≤ WINDOW constraint, K should remain 50
  ASSERT_EQ(parseCtx.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(50, vq->knn.k) << "Expected K to remain 50 for Linear scoring, got " << vq->knn.k;
}

// Test that K ≤ WINDOW constraint doesn't affect cases where K is already ≤ WINDOW
TEST_F(HybridDefaultsTest, testKAlreadyWithinWindow) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8", "COMBINE", "RRF", "2", "WINDOW", "20");

  parseCommand(args);
  // K should remain unchanged since 8 ≤ 20
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(8, vq->knn.k) << "Expected K to remain 8, got " << vq->knn.k;
  ASSERT_EQ(20, parseCtx.hybridParams->scoringCtx->rrfCtx.window);
}
