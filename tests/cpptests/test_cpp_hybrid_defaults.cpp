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
    QueryError qerr = QueryError_Default();
    RMCK::ArgvList createArgs(ctx, "FT.CREATE", index_name.c_str(), "ON", "HASH",
                              "SCHEMA", "title", "TEXT", "content", "TEXT",
                              "vector", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "COSINE");
    IndexSpec *spec = IndexSpec_CreateNew(ctx, createArgs, createArgs.size(), &qerr);
    if (!spec) {
      printf("Failed to create index '%s': code=%d, detail='%s'\n",
             index_name.c_str(), QueryError_GetCode(&qerr), QueryError_GetUserError(&qerr));
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
      HybridRequest_DecrRef(result);
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
    QueryError status = QueryError_Default();

    EXPECT_TRUE(result->sctx != NULL) << "Failed to create search context";

    ParseHybridCommandCtx cmd = {0};
    cmd.search = result->requests[0];
    cmd.vector = result->requests[1];
    cmd.tailPlan = &result->tailPipeline->ap;
    cmd.hybridParams = &hybridParams;
    cmd.reqConfig = &result->reqConfig;
    cmd.cursorConfig = &result->cursorConfig;

    ArgsCursor ac = {0};
    HybridRequest_InitArgsCursor(result, &ac, args, args.size());
    int rc =  parseHybridCommand(ctx, &ac, result->sctx, &cmd, &status, false, EXEC_NO_FLAGS);
    if (rc != REDISMODULE_OK) {
      HybridRequest_DecrRef(result);
      result = nullptr;
    }

    EXPECT_TRUE(QueryError_IsOk(&status)) << "Parse failed: " << QueryError_GetDisplayableError(&status, false);
    EXPECT_NE(result, nullptr) << "parseHybridCommand returned NULL";

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
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  validateDefaultParams(result, parseCtx, HYBRID_DEFAULT_WINDOW, HYBRID_DEFAULT_KNN_K);
}

// LIMIT affects both implicit parameters
TEST_F(HybridDefaultsTest, testLimitFallbackBoth) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector","$BLOB", "PARAMS", "2", "BLOB", TEST_BLOB_DATA,
                      "LIMIT", "0", "25");

  parseCommand(args);
  validateDefaultParams(result, parseCtx, HYBRID_DEFAULT_WINDOW, HYBRID_DEFAULT_KNN_K);
}

// LIMIT affects only implicit K, but K gets capped at explicit WINDOW
TEST_F(HybridDefaultsTest, testLimitFallbackKOnly) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "RRF", "2", "WINDOW", "15", "LIMIT", "0", "25",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  // K should be capped at WINDOW=15 even though LIMIT fallback would set it to 25
  validateDefaultParams(result, parseCtx, 15, HYBRID_DEFAULT_KNN_K);
}

// LIMIT affects only implicit WINDOW
TEST_F(HybridDefaultsTest, testLimitFallbackWindowOnly) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "KNN", "2", "K", "8", "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "LIMIT", "0", "25");

  parseCommand(args);
  validateDefaultParams(result, parseCtx, HYBRID_DEFAULT_WINDOW, 8);
}

// Explicit parameters override LIMIT
TEST_F(HybridDefaultsTest, testExplicitOverridesLimit) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "8",
                      "COMBINE", "RRF", "2", "WINDOW", "15", "LIMIT", "0", "25",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  validateDefaultParams(result, parseCtx, 15, 8);
}

// Large LIMIT values work
TEST_F(HybridDefaultsTest, testLargeLimitFallback) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "LIMIT", "0", "10000");

  parseCommand(args);
  validateDefaultParams(result, parseCtx, HYBRID_DEFAULT_WINDOW, HYBRID_DEFAULT_KNN_K); // K capped at WINDOW (DEFAULT_WINDOW)
}

// Flag verification tests
TEST_F(HybridDefaultsTest, testFlagTrackingImplicitBoth) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  // Both flags should be false
  ASSERT_FALSE(parseCtx.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
  ASSERT_FALSE(result->requests[1]->parsedVectorData->hasExplicitK);
}

TEST_F(HybridDefaultsTest, testFlagTrackingExplicitK) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB", "KNN", "2", "K", "8",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  // K explicit, WINDOW implicit
  ASSERT_TRUE(result->requests[1]->parsedVectorData->hasExplicitK);
  ASSERT_FALSE(parseCtx.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(HybridDefaultsTest, testFlagTrackingExplicitWindow) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "RRF", "2", "WINDOW", "15",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  // WINDOW explicit, K implicit
  ASSERT_TRUE(parseCtx.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
  ASSERT_FALSE(result->requests[1]->parsedVectorData->hasExplicitK);
}

TEST_F(HybridDefaultsTest, testFlagTrackingExplicitBoth) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "KNN", "2", "K", "8", "COMBINE", "RRF", "2", "WINDOW", "15",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  // Both flags should be true
  ASSERT_TRUE(result->requests[1]->parsedVectorData->hasExplicitK);
  ASSERT_TRUE(parseCtx.hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
}

TEST_F(HybridDefaultsTest, testLinearDefaults) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "LINEAR", "4", "ALPHA", "0.6", "BETA", "0.4",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

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
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "KNN", "2", "K", "50", "COMBINE", "RRF", "2", "WINDOW", "15",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  // Verify K was capped to WINDOW value
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(15, vq->knn.k) << "Expected K to be capped at WINDOW=15, because K is explicitly set to 50, got " << vq->knn.k;
  ASSERT_EQ(15, parseCtx.hybridParams->scoringCtx->rrfCtx.window);
}

// Test K ≤ WINDOW constraint: K from LIMIT fallback > explicit WINDOW should cap K to WINDOW
TEST_F(HybridDefaultsTest, testKFromLimitCappedAtExplicitWindow) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "RRF", "2", "WINDOW", "12", "LIMIT", "0", "30",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  // K should be capped to WINDOW (12) even though LIMIT fallback would set it to 30
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(HYBRID_DEFAULT_KNN_K, vq->knn.k) << "Expected K to be capped at WINDOW=12, got " << vq->knn.k;
  ASSERT_EQ(12, parseCtx.hybridParams->scoringCtx->rrfCtx.window);
}

// Test K = min{ K, WINDOW} optimization is used in LINEAR
TEST_F(HybridDefaultsTest, testLinearScoringKWindowConstraint) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "KNN", "2", "K", "50", "COMBINE", "LINEAR", "6", "ALPHA", "0.7", "BETA", "0.3", "WINDOW", "12",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  ASSERT_EQ(parseCtx.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(12, vq->knn.k) << "Expected K to be capped by WINDOW=12, got " << vq->knn.k;
}

// Test that K ≤ WINDOW constraint doesn't affect cases where K is already ≤ WINDOW
TEST_F(HybridDefaultsTest, testKAlreadyWithinWindow) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "KNN", "2", "K", "8", "COMBINE", "RRF", "2", "WINDOW", "20",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  // K should remain unchanged since 8 ≤ 20
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(8, vq->knn.k) << "Expected K to remain 8, got " << vq->knn.k;
  ASSERT_EQ(20, parseCtx.hybridParams->scoringCtx->rrfCtx.window);
}

// Test LINEAR with explicit WINDOW parameter
TEST_F(HybridDefaultsTest, testLinearExplicitWindow) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "LINEAR", "6", "ALPHA", "0.6", "BETA", "0.4", "WINDOW", "30",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  ASSERT_EQ(parseCtx.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(30, parseCtx.hybridParams->scoringCtx->linearCtx.window)
      << "Expected LINEAR window=30, got " << parseCtx.hybridParams->scoringCtx->linearCtx.window;

  // K should remain at default since LINEAR doesn't apply K≤WINDOW constraint
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(HYBRID_DEFAULT_KNN_K, vq->knn.k)
      << "Expected KNN k=" << HYBRID_DEFAULT_KNN_K << ", got " << vq->knn.k;
}

// Test LINEAR WINDOW defaults to HYBRID_DEFAULT_WINDOW
TEST_F(HybridDefaultsTest, testLinearWindowDefaults) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "LINEAR", "4", "ALPHA", "0.6", "BETA", "0.4",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  ASSERT_EQ(parseCtx.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(HYBRID_DEFAULT_WINDOW, parseCtx.hybridParams->scoringCtx->linearCtx.window)
      << "Expected LINEAR window=" << HYBRID_DEFAULT_WINDOW << ", got " << parseCtx.hybridParams->scoringCtx->linearCtx.window;
}

// Test LINEAR WINDOW with LIMIT fallback (WINDOW should ignore LIMIT fallback)
TEST_F(HybridDefaultsTest, testLinearWindowLimitFallback) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "LINEAR", "4", "ALPHA", "0.6", "BETA", "0.4",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA, "LIMIT", "0", "50");

  parseCommand(args);
  ASSERT_EQ(parseCtx.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(HYBRID_DEFAULT_WINDOW, parseCtx.hybridParams->scoringCtx->linearCtx.window)
      << "Expected LINEAR window=" << HYBRID_DEFAULT_WINDOW << " (should use default, not LIMIT fallback), got " << parseCtx.hybridParams->scoringCtx->linearCtx.window;
}

// Test LINEAR WINDOW independent of LIMIT
TEST_F(HybridDefaultsTest, testLinearExplicitWindowOverridesLimit) {
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", "$BLOB",
                      "COMBINE", "LINEAR", "6", "ALPHA", "0.6", "BETA", "0.4", "WINDOW", "25",
                      "LIMIT", "0", "100", "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  parseCommand(args);
  ASSERT_EQ(parseCtx.hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);
  ASSERT_EQ(25, parseCtx.hybridParams->scoringCtx->linearCtx.window)
      << "Expected LINEAR window=25 (explicit should override LIMIT), got " << parseCtx.hybridParams->scoringCtx->linearCtx.window;
}
