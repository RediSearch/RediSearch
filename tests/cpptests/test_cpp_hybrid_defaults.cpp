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
  }

  void TearDown() override {
    if (sctx) {
      SearchCtx_Free(sctx);
      sctx = nullptr;
    }
    if (ctx) {
      // Drop the index to clean up
      RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.DROPINDEX", "c", index_name.c_str());
      if (reply) {
        RedisModule_FreeCallReply(reply);
      }
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }

static void validateDefaultParams(HybridRequest* result, size_t expectedWindow, size_t expectedKnnK) {
    ASSERT_TRUE(result != NULL);
    ASSERT_TRUE(result->hybridParams != NULL);
    ASSERT_TRUE(result->hybridParams->scoringCtx != NULL);

    // Verify RRF-specific parameters (only for RRF)
    if (result->hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF) {
        ASSERT_EQ(expectedWindow, result->hybridParams->scoringCtx->rrfCtx.window)
            << "Expected window=" << expectedWindow << ", got " << result->hybridParams->scoringCtx->rrfCtx.window;

        // Verify RRF k default
        ASSERT_DOUBLE_EQ(HYBRID_DEFAULT_RRF_K, result->hybridParams->scoringCtx->rrfCtx.k)
            << "Expected RRF k=" << HYBRID_DEFAULT_RRF_K;
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
  QueryError status = {QueryErrorCode(0)};
  
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(), "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA);
  
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);
  
  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  validateDefaultParams(result, HYBRID_DEFAULT_WINDOW, HYBRID_DEFAULT_KNN_K);
  
  HybridRequest_Free(result);
}

// LIMIT affects both implicit parameters
TEST_F(HybridDefaultsTest, testLimitFallbackBoth) {
  QueryError status = {QueryErrorCode(0)};
  
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "RRF", "LIMIT", "0", "25");
  
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);
  
  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);
  
  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");
  
  validateDefaultParams(result, 25, 25);
  
  HybridRequest_Free(result);
}

// LIMIT affects only implicit K, but K gets capped at explicit WINDOW
TEST_F(HybridDefaultsTest, testLimitFallbackKOnly) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "RRF", "2", "WINDOW", "15", "LIMIT", "0", "25");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // K should be capped at WINDOW=15 even though LIMIT fallback would set it to 25
  validateDefaultParams(result, 15, 15);

  HybridRequest_Free(result);
}

// LIMIT affects only implicit WINDOW
TEST_F(HybridDefaultsTest, testLimitFallbackWindowOnly) {
  QueryError status = {QueryErrorCode(0)};
  
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8", "COMBINE", "RRF", "LIMIT", "0", "25");
  
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);
  
  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);
  
  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");
  
  validateDefaultParams(result, 25, 8);
  
  HybridRequest_Free(result);
}

// Explicit parameters override LIMIT
TEST_F(HybridDefaultsTest, testExplicitOverridesLimit) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8", "COMBINE", "RRF", "2", "WINDOW", "15", "LIMIT", "0", "25");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  validateDefaultParams(result, 15, 8);

  HybridRequest_Free(result);
}

// Large LIMIT values work
TEST_F(HybridDefaultsTest, testLargeLimitFallback) {
  QueryError status = {QueryErrorCode(0)};
  
  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "RRF", "LIMIT", "0", "10000");
  
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);
  
  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);
  
  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");
  
  validateDefaultParams(result, 10000, 10000);
  
  HybridRequest_Free(result);
}

// Flag verification tests
TEST_F(HybridDefaultsTest, testFlagTrackingImplicitBoth) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "RRF");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // Both flags should be false
  ASSERT_FALSE(result->hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
  ASSERT_FALSE(result->requests[1]->parsedVectorData->hasExplicitK);

  HybridRequest_Free(result);
}

TEST_F(HybridDefaultsTest, testFlagTrackingExplicitK) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8", "COMBINE", "RRF");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // K explicit, WINDOW implicit
  ASSERT_TRUE(result->requests[1]->parsedVectorData->hasExplicitK);
  ASSERT_FALSE(result->hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);

  HybridRequest_Free(result);
}

TEST_F(HybridDefaultsTest, testFlagTrackingExplicitWindow) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "RRF", "2", "WINDOW", "15");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // WINDOW explicit, K implicit
  ASSERT_TRUE(result->hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);
  ASSERT_FALSE(result->requests[1]->parsedVectorData->hasExplicitK);

  HybridRequest_Free(result);
}

TEST_F(HybridDefaultsTest, testFlagTrackingExplicitBoth) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8", "COMBINE", "RRF", "2", "WINDOW", "15");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // Both flags should be true
  ASSERT_TRUE(result->requests[1]->parsedVectorData->hasExplicitK);
  ASSERT_TRUE(result->hybridParams->scoringCtx->rrfCtx.hasExplicitWindow);

  HybridRequest_Free(result);
}

TEST_F(HybridDefaultsTest, testLinearDefaults) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "LINEAR", "0.6", "0.4");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // LINEAR should not have window parameter (uses regular limit instead)
  ASSERT_EQ(result->hybridParams->scoringCtx->scoringType, HYBRID_SCORING_LINEAR);

  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(HYBRID_DEFAULT_KNN_K, vq->knn.k)
      << "Expected KNN k=" << HYBRID_DEFAULT_KNN_K << ", got " << vq->knn.k;

  HybridRequest_Free(result);
}

// Test K ≤ WINDOW constraint: explicit K > explicit WINDOW should cap K to WINDOW
TEST_F(HybridDefaultsTest, testKCappedAtExplicitWindow) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "50", "COMBINE", "RRF", "2", "WINDOW", "15");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // Verify K was capped to WINDOW value
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(15, vq->knn.k) << "Expected K to be capped at WINDOW=15, got " << vq->knn.k;
  ASSERT_EQ(15, result->hybridParams->scoringCtx->rrfCtx.window);

  HybridRequest_Free(result);
}

// Test K ≤ WINDOW constraint: K from LIMIT fallback > explicit WINDOW should cap K to WINDOW
TEST_F(HybridDefaultsTest, testKFromLimitCappedAtExplicitWindow) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "COMBINE", "RRF", "2", "WINDOW", "12", "LIMIT", "0", "30");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // K should be capped to WINDOW (12) even though LIMIT fallback would set it to 30
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(12, vq->knn.k) << "Expected K to be capped at WINDOW=12, got " << vq->knn.k;
  ASSERT_EQ(12, result->hybridParams->scoringCtx->rrfCtx.window);

  HybridRequest_Free(result);
}

// Test K ≤ WINDOW constraint: explicit K > WINDOW from LIMIT fallback should cap K to WINDOW
TEST_F(HybridDefaultsTest, testExplicitKCappedAtWindowFromLimit) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "25", "COMBINE", "RRF", "LIMIT", "0", "18");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // K should be capped to WINDOW (18 from LIMIT fallback) even though K was explicitly set to 25
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(18, vq->knn.k) << "Expected K to be capped at WINDOW=18, got " << vq->knn.k;
  ASSERT_EQ(18, result->hybridParams->scoringCtx->rrfCtx.window);

  HybridRequest_Free(result);
}

// Test that K ≤ WINDOW constraint doesn't affect cases where K is already ≤ WINDOW
TEST_F(HybridDefaultsTest, testKAlreadyWithinWindow) {
  QueryError status = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.HYBRID", index_name.c_str(),
                      "SEARCH", "hello", "VSIM", "@vector", TEST_BLOB_DATA,
                      "KNN", "2", "K", "8", "COMBINE", "RRF", "2", "WINDOW", "20");

  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, index_name.c_str(), true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* result = parseHybridCommand(ctx, args, args.size(), test_sctx, index_name.c_str(), &status);

  ASSERT_EQ(status.code, QUERY_OK) << "Parse failed: " << (status.detail ? status.detail : "NULL");

  // K should remain unchanged since 8 ≤ 20
  VectorQuery *vq = result->requests[1]->ast.root->vn.vq;
  ASSERT_EQ(8, vq->knn.k) << "Expected K to remain 8, got " << vq->knn.k;
  ASSERT_EQ(20, result->hybridParams->scoringCtx->rrfCtx.window);

  HybridRequest_Free(result);
}
