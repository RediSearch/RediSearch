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
#include "iterators/hybrid_reader.h"
#include "src/spec.h"
#include "src/search_ctx.h"
#include "aggregate/aggregate.h"

// Macro for BLOB data that all tests using $BLOB should use
#define TEST_BLOB_DATA "AQIDBAUGBwgJCg=="
#define VECTOR_REQUEST_INDEX 1
#define SEARCH_REQUEST_INDEX 0

class HybridRequestParseTest : public ::testing::Test {
protected:
  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);
    QueryError_ClearError(&qerr);
  }

  void TearDown() override {
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }

  RedisModuleCtx *ctx = nullptr;
  QueryError qerr = QueryError_Default();
};

/**
 * Helper function to create a test index spec with standard schema.
 */
IndexSpec* CreateTestIndexSpec(RedisModuleCtx *ctx, const char* indexName, QueryError *status) {
  RMCK::ArgvList createArgs(ctx, "FT.CREATE", indexName, "ON", "HASH", "SKIPINITIALSCAN",
                            "SCHEMA", "title", "TEXT", "score", "NUMERIC",
                            "category", "TEXT", "vector_field", "VECTOR", "FLAT", "6",
                            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE");
  return IndexSpec_CreateNew(ctx, createArgs, createArgs.size(), status);
}

// ============================================================================
// FILTER POLICY AND BATCH SIZE TESTS
// ============================================================================

/**
 * Test context for hybrid iterator property tests.
 * Handles setup/teardown, leaving tests to focus on assertions.
 * Used for tests that need to inspect HybridIterator properties (searchMode, batchSize, etc.)
 * without building the full pipeline.
 */
struct HybridIteratorTestCtx {
    IndexSpec *spec = nullptr;
    HybridRequest *hybridReq = nullptr;
    QueryIterator *rootiter = nullptr;
    HybridIterator *hi = nullptr;
    QueryError status = QueryError_Default();
    QueryError iterError = QueryError_Default();
    HybridPipelineParams hybridParams = {0};

    ~HybridIteratorTestCtx() {
      if (rootiter) rootiter->Free(rootiter);
      if (hybridReq) HybridRequest_DecrRef(hybridReq);
      if (hybridParams.scoringCtx) HybridScoringContext_Free(hybridParams.scoringCtx);
      if (spec) IndexSpec_RemoveFromGlobals(spec->own_ref, false);
    }
  };

/**
 * Setup a hybrid iterator test context.
 * Performs: create index, insert doc, parse command, create iterator.
 * Does NOT build the pipeline - used for testing iterator properties directly.
 *
 * @param ctx Redis module context
 * @param indexName Name for the test index
 * @param args The FT.HYBRID command arguments
 * @param testCtx Output: populated test context
 * @return true if setup succeeded, false otherwise
 */
bool SetupHybridIteratorTest(RedisModuleCtx *ctx,
                             const char *indexName,
                             RMCK::ArgvList &args,
                             HybridIteratorTestCtx *testCtx) {
    // Step 1: Create index spec
    testCtx->spec = CreateTestIndexSpec(ctx, indexName, &testCtx->status);
    if (!testCtx->spec) return false;

    const char *specName = HiddenString_GetUnsafe(testCtx->spec->specName, NULL);

    // Step 2: Insert document (so iterator won't be empty)
    if (!RS::addDocument(ctx, testCtx->spec->own_ref.rm, "doc:1",
                         "title", "hello", "score", "42",
                         "vector_field", TEST_BLOB_DATA)) {
      return false;
    }

    // Step 3: Create search context and hybrid request
    RedisSearchCtx *sctx = NewSearchCtxC(ctx, specName, true);
    if (!sctx) return false;

    testCtx->hybridReq = MakeDefaultHybridRequest(sctx);
    if (!testCtx->hybridReq) return false;

    // Step 4: Parse the hybrid command
    RequestConfig reqConfig = {0};
    CursorConfig cursorConfig = {0};

    ParseHybridCommandCtx cmd = {
      .search = testCtx->hybridReq->requests[SEARCH_REQUEST_INDEX],
      .vector = testCtx->hybridReq->requests[VECTOR_REQUEST_INDEX],
      .tailPlan = &testCtx->hybridReq->tailPipeline->ap,
      .hybridParams = &testCtx->hybridParams,
      .reqConfig = &reqConfig,
      .cursorConfig = &cursorConfig
    };

    ArgsCursor ac = {0};
    HybridRequest_InitArgsCursor(testCtx->hybridReq, &ac, args, args.size());

    int rc = parseHybridCommand(ctx, &ac, sctx, &cmd, &testCtx->status, false, EXEC_NO_FLAGS);
    if (rc != REDISMODULE_OK) return false;

    // Step 5: Create iterator from vector request
    AREQ *vecReq = testCtx->hybridReq->requests[VECTOR_REQUEST_INDEX];
    testCtx->rootiter = QAST_Iterate(&vecReq->ast, &vecReq->searchopts,
                                      AREQ_SearchCtx(vecReq), vecReq->reqflags,
                                      &testCtx->iterError);

    if (!QueryError_IsOk(&testCtx->iterError) || !testCtx->rootiter) return false;
    if (testCtx->rootiter->type != HYBRID_ITERATOR) return false;

    testCtx->hi = (HybridIterator *)testCtx->rootiter;
    return true;
}

TEST_F(HybridRequestParseTest, testFilterBatchSize) {
    // Test FILTER with BATCH_SIZE - verifies batch size is propagated to iterator runtime params
    RMCK::ArgvList args(ctx, "FT.HYBRID", "test_batch_size",
                        "SEARCH", "hello",
                        "VSIM", "@vector_field", "$BLOB",
                        "FILTER", "3", "hello",
                        "BATCH_SIZE", "100",
                        "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

    HybridIteratorTestCtx testCtx;
    ASSERT_TRUE(SetupHybridIteratorTest(ctx, "test_batch_size", args, &testCtx))
      << "Setup failed: " << QueryError_GetUserError(&testCtx.status);

    ASSERT_EQ(testCtx.hi->runtimeParams.batchSize, 100);
}

TEST_F(HybridRequestParseTest, testPolicyBatchesWithBatchSize) {
    // Test POLICY BATCHES with BATCH_SIZE - verifies explicit batches policy with custom batch size
    RMCK::ArgvList args(ctx, "FT.HYBRID", "test_policy_batches",
                        "SEARCH", "hello",
                        "VSIM", "@vector_field", "$BLOB",
                        "FILTER","5", "hello",
                        "POLICY", "BATCHES", "BATCH_SIZE", "50",
                        "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

    HybridIteratorTestCtx testCtx;
    ASSERT_TRUE(SetupHybridIteratorTest(ctx, "test_policy_batches", args, &testCtx))
      << "Setup failed: " << QueryError_GetUserError(&testCtx.status);

    ASSERT_EQ(testCtx.hi->searchMode, VECSIM_HYBRID_BATCHES);
    ASSERT_EQ(testCtx.hi->runtimeParams.batchSize, 50);
}

TEST_F(HybridRequestParseTest, testPolicyAdhoc) {
    // Test POLICY ADHOC - verifies adhoc policy results in ADHOC_BF search mode
    RMCK::ArgvList args(ctx, "FT.HYBRID", "test_policy_adhoc",
                        "SEARCH", "hello",
                        "VSIM", "@vector_field", "$BLOB",
                        "FILTER","3", "hello",
                        "POLICY", "ADHOC",
                        "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

    HybridIteratorTestCtx testCtx;
    ASSERT_TRUE(SetupHybridIteratorTest(ctx, "test_policy_adhoc", args, &testCtx))
      << "Setup failed: " << QueryError_GetUserError(&testCtx.status);

    ASSERT_EQ(testCtx.hi->searchMode, VECSIM_HYBRID_ADHOC_BF);
}
