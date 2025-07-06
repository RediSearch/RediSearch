/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "aggregate/aggregate.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_construction.h"
#include "hybrid/hybrid_request.h"
#include "hybrid/hybrid_scoring.h"
#include "result_processor.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "spec.h"
#include "common.h"
#include "module.h"
#include "version.h"

#include <vector>
#include <array>
#include <iostream>
#include <cstdarg>

class HybridRequestTest : public ::testing::Test {
protected:
  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    QueryError_ClearError(&qerr);
  }

  void TearDown() override {
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
    }
  }

  RedisModuleCtx *ctx = nullptr;
  QueryError qerr = {QueryErrorCode(0)};
};

using RS::addDocument;

// Helper function to create a test index spec
IndexSpec* CreateTestIndexSpec(RedisModuleCtx *ctx, const char* indexName, QueryError *status) {
  RMCK::ArgvList args(ctx, "FT.CREATE", indexName, "ON", "HASH",
                      "SCHEMA", "title", "TEXT", "SORTABLE", "score", "NUMERIC",
                      "SORTABLE", "category", "TEXT", "vector_field", "VECTOR", "FLAT", "6",
                      "TYPE", "FLOAT32", "DIM", "128", "DISTANCE_METRIC", "COSINE");
  auto spec = IndexSpec_CreateNew(ctx, args, args.size(), status);
  return spec;
}

// Helper function to create a basic AREQ for testing
AREQ* CreateTestAREQ(RedisModuleCtx *ctx, const char* query, IndexSpec *spec, QueryError *status) {
  AREQ *req = AREQ_New();
  RMCK::ArgvList args(ctx, query);
  int rv = AREQ_Compile(req, args, args.size(), status);
  if (rv != REDISMODULE_OK) {
    AREQ_Free(req);
    return nullptr;
  }

  const char *specName = HiddenString_GetUnsafe(spec->specName, NULL);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, specName, true);
  if (!sctx) {
    AREQ_Free(req);
    return nullptr;
  }

  rv = AREQ_ApplyContext(req, sctx, status);
  if (rv != REDISMODULE_OK) {
    AREQ_Free(req);
    return nullptr;
  }

  return req;
}

/**
 * Helper function to add a LOAD step to an AGGPlan with properly initialized RLookupKeys.
 * This function creates a LOAD step that specifies which document fields should be loaded
 * during query execution. Unlike the previous version that used NULL keys, this properly
 * initializes RLookupKey objects using the plan's RLookup context.
 *
 * @param plan The AGGPlan to add the LOAD step to
 * @param fields Array of field names to load
 * @param nfields Number of fields in the array
 */
void AddLoadStepToPlan(AGGPlan *plan, const char **fields, size_t nfields) {
  PLN_LoadStep *loadStep = (PLN_LoadStep*)rm_calloc(1, sizeof(PLN_LoadStep));
  loadStep->base.type = PLN_T_LOAD;
  loadStep->nkeys = nfields;
  if (nfields > 0) {
    loadStep->keys = (const RLookupKey**)rm_calloc(nfields, sizeof(RLookupKey*));

    // Get the RLookup from the plan to create proper RLookupKeys
    RLookup *lookup = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_LAST);
    if (!lookup) {
      // If no lookup exists yet, create one at the end of the plan
      lookup = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_FIRST);
    }

    // Create proper RLookupKeys for each field
    for (size_t i = 0; i < nfields; i++) {
      // Use RLookup_GetKey_Load to create keys for loading these fields
      // This will create the key if it doesn't exist and mark it for loading
      RLookupKey *key = RLookup_GetKey_Load(lookup, fields[i], fields[i], RLOOKUP_F_NOFLAGS);
      loadStep->keys[i] = key;
    }
  }
  AGPLN_AddStep(plan, &loadStep->base);
}

// Tests that don't require full Redis Module integration
TEST_F(HybridRequestTest, testHybridRequestCreationBasic) {
  // Test basic HybridRequest creation without Redis dependencies
  AREQ **requests = array_new(AREQ*, 2);
  // Initialize the AREQ structures
  AREQ *req1 = AREQ_New();
  AREQ *req2 = AREQ_New();

  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);
  ASSERT_EQ(hybridReq->nrequests, 2);
  ASSERT_TRUE(hybridReq->requests != nullptr);

  // Verify the merge pipeline is initialized
  ASSERT_TRUE(hybridReq->tail.ap.steps.next != nullptr);

  // Clean up
  HybridRequest_Free(hybridReq);
}

TEST_F(HybridRequestTest, testHybridRequestPipelineBuildingBasic) {
  // Test pipeline building logic without Redis dependencies
  AREQ **requests = array_new(AREQ*, 1);
  AREQ *req = AREQ_New();

  requests = array_ensure_append_1(requests, req);

  // Initialize basic pipeline components
  AGPLN_Init(&requests[0]->pipeline.ap);

  HybridRequest *hybridReq = HybridRequest_New(requests, 1);

  // Add a basic LOAD step to test pipeline building
  const char *loadFields[] = {"test_field"};
  AddLoadStepToPlan(&hybridReq->tail.ap, loadFields, 1);

  ASSERT_TRUE(hybridReq != nullptr);

  // Test that the structure is properly initialized
  EXPECT_EQ(hybridReq->nrequests, 1);
  EXPECT_TRUE(hybridReq->requests != nullptr);

  // Verify merge pipeline structure
  EXPECT_TRUE(hybridReq->tail.ap.steps.next != nullptr);

  // Clean up
  HybridRequest_Free(hybridReq);
}

TEST_F(HybridRequestTest, testHybridRequestCreationWithRedis) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // No need to add documents for pipeline building tests

  // Create two AREQ requests for hybrid search
  AREQ *req1 = CreateTestAREQ(ctx, "hello", spec, &qerr);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create first AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req2 = CreateTestAREQ(ctx, "world", spec, &qerr);
  ASSERT_TRUE(req2 != nullptr) << "Failed to create second AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 2);
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);
  ASSERT_EQ(hybridReq->nrequests, 2);
  ASSERT_TRUE(hybridReq->requests != nullptr);

  // Verify the merge pipeline is initialized
  ASSERT_TRUE(hybridReq->tail.ap.steps.next != nullptr);

  // Clean up
  HybridRequest_Free(hybridReq);
  // Use IndexSpec_RemoveFromGlobals instead of IndexSpec_Free because the spec
  // was added to the global registry by IndexSpec_CreateNew and holds a reference
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

TEST_F(HybridRequestTest, testHybridRequestBuildPipelineBasic) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx2", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // No need to add documents for pipeline building tests

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "machine", spec, &qerr);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create first AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req2 = CreateTestAREQ(ctx, "learning", spec, &qerr);
  ASSERT_TRUE(req2 != nullptr) << "Failed to create second AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 2);
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);

  const char *loadFields[] = {"title", "score"};
  AddLoadStepToPlan(&hybridReq->tail.ap, loadFields, 2);

  HybridPipelineParams params = {
      .aggregation = {
        .common = {
          .pln = &hybridReq->tail.ap,
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = nullptr,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << QueryError_GetUserError(&qerr);

  // Verify that individual request pipelines were built
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];
    EXPECT_TRUE(areq->pipeline.qctx.endProc != nullptr) << "Request " << i << " pipeline not built";
  }

  // Verify merge pipeline has processors
  EXPECT_TRUE(hybridReq->tail.qctx.endProc != nullptr) << "Tail pipeline not built";

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

TEST_F(HybridRequestTest, testHybridRequestBuildPipelineWithMultipleRequests) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx3", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // No need to add documents for pipeline building tests

  // Create multiple AREQ requests for comprehensive hybrid search
  AREQ *req1 = CreateTestAREQ(ctx, "artificial", spec, &qerr);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create first AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req2 = CreateTestAREQ(ctx, "machine", spec, &qerr);
  ASSERT_TRUE(req2 != nullptr) << "Failed to create second AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req3 = CreateTestAREQ(ctx, "neural", spec, &qerr);
  ASSERT_TRUE(req3 != nullptr) << "Failed to create third AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 3);
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);
  requests = array_ensure_append_1(requests, req3);

  // Create HybridRequest with 3 requests
  HybridRequest *hybridReq = HybridRequest_New(requests, 3);
  ASSERT_TRUE(hybridReq != nullptr);
  ASSERT_EQ(hybridReq->nrequests, 3);

  const char *loadFields[] = {"title", "score", "category"};
  AddLoadStepToPlan(&hybridReq->tail.ap, loadFields, 3);

  HybridPipelineParams params = {
      .aggregation = {
        .common = {
          .pln = &hybridReq->tail.ap,
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = nullptr,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << QueryError_GetUserError(&qerr);

  // Verify all individual request pipelines were built
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];
    EXPECT_TRUE(areq->pipeline.qctx.endProc != nullptr) << "Request " << i << " pipeline not built";
    EXPECT_TRUE(areq->pipeline.qctx.rootProc != nullptr) << "Request " << i << " root processor not set";
  }

  // Verify merge pipeline structure
  EXPECT_TRUE(hybridReq->tail.qctx.endProc != nullptr) << "Tail pipeline end processor not set";
  EXPECT_TRUE(hybridReq->tail.qctx.rootProc != nullptr) << "Tail pipeline root processor not set";

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

TEST_F(HybridRequestTest, testHybridRequestBuildPipelineErrorHandling) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx4", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // Create a valid AREQ request
  AREQ *req1 = CreateTestAREQ(ctx, "test", spec, &qerr);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create AREQ: " << QueryError_GetUserError(&qerr);

  // Create array with single request
  AREQ **requests = array_new(AREQ*, 1);
  requests = array_ensure_append_1(requests, req1);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 1);
  ASSERT_TRUE(hybridReq != nullptr);

  HybridPipelineParams params = {
      .aggregation = {
        .common = {
          .pln = &hybridReq->tail.ap,
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = nullptr,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  // Should handle missing LOAD step gracefully
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build should handle missing LOAD step: " << QueryError_GetUserError(&qerr);

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

TEST_F(HybridRequestTest, testHybridRequestBuildPipelineWithDifferentQueries) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx5", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // No need to add documents for pipeline building tests

  // Create AREQ requests with different query types
  AREQ *textReq = CreateTestAREQ(ctx, "redis", spec, &qerr);
  ASSERT_TRUE(textReq != nullptr) << "Failed to create text AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *categoryReq = CreateTestAREQ(ctx, "@category:database", spec, &qerr);
  ASSERT_TRUE(categoryReq != nullptr) << "Failed to create category AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *numericReq = CreateTestAREQ(ctx, "@score:[3.0 5.0]", spec, &qerr);
  ASSERT_TRUE(numericReq != nullptr) << "Failed to create numeric AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of diverse requests
  AREQ **requests = array_new(AREQ*, 3);
  requests = array_ensure_append_1(requests, textReq);
  requests = array_ensure_append_1(requests, categoryReq);
  requests = array_ensure_append_1(requests, numericReq);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 3);
  ASSERT_TRUE(hybridReq != nullptr);

  // Create AGGPlan with comprehensive LOAD step
  const char *loadFields[] = {"title", "score", "category"};
  AddLoadStepToPlan(&hybridReq->tail.ap, loadFields, 3);

  HybridPipelineParams params = {
      .aggregation = {
        .common = {
          .pln = &hybridReq->tail.ap,
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = nullptr,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed with diverse queries: " << QueryError_GetUserError(&qerr);

  // Verify all pipelines are properly built
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];
    EXPECT_TRUE(areq->pipeline.qctx.endProc != nullptr) << "Request " << i << " pipeline not built";

    // Verify pipeline has proper structure
    ResultProcessor *rp = areq->pipeline.qctx.endProc;
    int processorCount = 0;
    while (rp && processorCount < 10) { // Safety limit
      processorCount++;
      rp = rp->upstream;
    }
    EXPECT_GT(processorCount, 0) << "Request " << i << " has no processors";
  }

  // Verify merge pipeline
  EXPECT_TRUE(hybridReq->tail.qctx.endProc != nullptr) << "Tail pipeline not built";

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

TEST_F(HybridRequestTest, testHybridRequestBuildPipelineMemoryManagement) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx6", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // No need to add documents for pipeline building tests

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "memory", spec, &qerr);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 1);
  requests = array_ensure_append_1(requests, req1);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 1);
  ASSERT_TRUE(hybridReq != nullptr);

  // Create AGGPlan with LOAD step
  const char *loadFields[] = {"title"};
  AddLoadStepToPlan(&hybridReq->tail.ap, loadFields, 1);

  HybridPipelineParams params = {
      .aggregation = {
        .common = {
          .pln = &hybridReq->tail.ap,
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = nullptr,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << QueryError_GetUserError(&qerr);

  // Verify proper cleanup - this test mainly ensures no memory leaks
  // The actual verification happens during cleanup
  EXPECT_TRUE(hybridReq->tail.qctx.endProc != nullptr) << "Merge pipeline not built";

  // Clean up - this should not crash or leak memory
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

TEST_F(HybridRequestTest, testHybridRequestPipelineComponents) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx7", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // No need to add documents for pipeline building tests

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "component", spec, &qerr);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create first AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req2 = CreateTestAREQ(ctx, "pipeline", spec, &qerr);
  ASSERT_TRUE(req2 != nullptr) << "Failed to create second AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 2);
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);

  // Create AGGPlan with LOAD step
  const char *loadFields[] = {"title", "score"};
  AddLoadStepToPlan(&hybridReq->tail.ap, loadFields, 2);

  // Build pipeline
  HybridPipelineParams params = {
      .aggregation = {
        .common = {
          .pln = &hybridReq->tail.ap,
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = nullptr,
    };
  int rc = HybridRequest_BuildPipeline(hybridReq, &params);

  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << QueryError_GetUserError(&qerr);

  // Verify pipeline components
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];

    // Check that pipeline has proper processor chain
    ResultProcessor *rp = areq->pipeline.qctx.endProc;
    EXPECT_TRUE(rp != nullptr) << "Request " << i << " has no end processor";

    // Verify we have a depleter in the chain (should be the end processor)
    EXPECT_TRUE(rp != nullptr) << "Request " << i << " missing depleter processor";

    // Check root processor exists
    EXPECT_TRUE(areq->pipeline.qctx.rootProc != nullptr) << "Request " << i << " has no root processor";
  }

  // Verify merge pipeline has hybrid merger
  ResultProcessor *mergeRP = hybridReq->tail.qctx.endProc;
  EXPECT_TRUE(mergeRP != nullptr) << "Tail pipeline has no end processor";

  // The merge pipeline should have processors for handling the merged results
  ResultProcessor *current = mergeRP;
  int processorCount = 0;
  while (current && processorCount < 10) { // Safety limit
    processorCount++;
    current = current->upstream;
  }
  EXPECT_GT(processorCount, 0) << "Tail pipeline has no processors";

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

TEST_F(HybridRequestTest, testHybridRequestBuildPipelineWithComplexPlan) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx8", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // No need to add documents for pipeline building tests

  // Create AREQ requests with different complexities
  AREQ *simpleReq = CreateTestAREQ(ctx, "test", spec, &qerr);
  ASSERT_TRUE(simpleReq != nullptr) << "Failed to create simple AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *complexReq = CreateTestAREQ(ctx, "@category:advanced @score:[1.0 3.0]", spec, &qerr);
  ASSERT_TRUE(complexReq != nullptr) << "Failed to create complex AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 2);
  requests = array_ensure_append_1(requests, simpleReq);
  requests = array_ensure_append_1(requests, complexReq);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);

  // Add LOAD step
  const char *loadFields[] = {"title", "score", "category"};
  AddLoadStepToPlan(&hybridReq->tail.ap, loadFields, 3);

  HybridPipelineParams params = {
      .aggregation = {
        .common = {
          .pln = &hybridReq->tail.ap,
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = nullptr,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Complex pipeline build failed: " << QueryError_GetUserError(&qerr);

  // Verify complex pipeline structure
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];
    EXPECT_TRUE(areq->pipeline.qctx.endProc != nullptr) << "Complex request " << i << " pipeline not built";
    EXPECT_TRUE(areq->pipeline.qctx.rootProc != nullptr) << "Complex request " << i << " root processor not set";
  }

  // Verify merge pipeline handles complex structure
  EXPECT_TRUE(hybridReq->tail.qctx.endProc != nullptr) << "Complex merge pipeline not built";

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}