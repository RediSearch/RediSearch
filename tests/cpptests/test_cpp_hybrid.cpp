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
  RMCK::ArgvList args(ctx, "FT.CREATE", indexName, "ON", "HASH", "SKIPINITIALSCAN",
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

// Helper function to verify pipeline chain structure
void VerifyPipelineChain(ResultProcessor *endProc, const std::vector<ResultProcessorType>& expectedTypes, const std::string& pipelineName) {
  ASSERT_TRUE(endProc != nullptr) << pipelineName << " has no end processor";

  std::vector<ResultProcessorType> actualTypes;
  ResultProcessor *current = endProc;

  // Walk the chain from end to beginning
  while (current != nullptr) {
    actualTypes.push_back(current->type);
    current = current->upstream;
  }

  ASSERT_EQ(expectedTypes.size(), actualTypes.size())
    << pipelineName << " has " << actualTypes.size() << " processors, expected " << expectedTypes.size();

  for (size_t i = 0; i < expectedTypes.size(); i++) {
    EXPECT_EQ(expectedTypes[i], actualTypes[i])
      << pipelineName << " processor " << i << " is " << RPTypeToString(actualTypes[i])
      << ", expected " << RPTypeToString(expectedTypes[i]);
  }
}


/**
 * Helper function to add a LOAD step to an AGGPlan with properly initialized RLookupKeys.
 * This function creates a LOAD step that specifies which document fields should be loaded
 * during query execution.
 *
 * @param plan The AGGPlan to add the LOAD step to
 * @param fields Array of field names to load
 * @param nfields Number of fields in the array
 */
void AddLoadStepToPlan(AGGPlan *plan, const char **fields, size_t nfields) {
  PLN_LoadStep *loadStep = (PLN_LoadStep*)rm_calloc(1, sizeof(PLN_LoadStep));
  loadStep->base.type = PLN_T_LOAD;
  loadStep->base.dtor = loadDtor;
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

/**
 * Helper function to add a SORT step to an AGGPlan.
 * This function gets or creates an arrange step and configures it for sorting.
 *
 * @param plan The AGGPlan to add the SORT step to
 * @param sortFields Array of field names to sort by
 * @param nfields Number of fields in the array
 * @param ascendingMap Bitmap indicating ascending (1) or descending (0) for each field
 */
void AddSortStepToPlan(AGGPlan *plan, const char **sortFields, size_t nfields, uint64_t ascendingMap) {
  PLN_ArrangeStep *arrangeStep = AGPLN_GetOrCreateArrangeStep(plan);

  // Set up sorting (free existing keys if any)
  RS_ASSERT(arrangeStep->sortkeysLK == NULL);

  arrangeStep->sortKeys = array_new(const char*, nfields);
  for (size_t i = 0; i < nfields; i++) {
    array_append(arrangeStep->sortKeys, sortFields[i]);
  }
  arrangeStep->sortAscMap = ascendingMap;
}

/**
 * Helper function to add an APPLY step to an AGGPlan.
 * This function creates an apply step that applies an expression to create a new field.
 *
 * @param plan The AGGPlan to add the APPLY step to
 * @param expression The expression to apply (e.g., "@score * 2")
 * @param alias The alias for the new field (e.g., "boosted_score")
 */
void AddApplyStepToPlan(AGGPlan *plan, const char *expression, const char *alias) {
  HiddenString *expr = NewHiddenString(expression, strlen(expression), false);
  PLN_MapFilterStep *applyStep = PLNMapFilterStep_New(expr, PLN_T_APPLY);
  HiddenString_Free(expr, false);

  // Set the alias for the APPLY step
  if (alias) {
    applyStep->base.alias = rm_strdup(alias);
  }

  AGPLN_AddStep(plan, &applyStep->base);
}

// Tests that don't require full Redis Module integration

// Test basic HybridRequest creation and initialization with multiple AREQ requests
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
  ASSERT_TRUE(hybridReq->pipeline.ap.steps.next != nullptr);
  // Clean up
  HybridRequest_Free(hybridReq);
}

// Test basic pipeline building with two AREQ requests and verify the pipeline structure
TEST_F(HybridRequestTest, testHybridRequestPipelineBuildingBasic) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx2", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

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
  AddLoadStepToPlan(&hybridReq->pipeline.ap, loadFields, 2);

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = (HybridScoringContext*)rm_calloc(1, sizeof(HybridScoringContext));
  scoringCtx->scoringType = HYBRID_SCORING_RRF;
  scoringCtx->rrfCtx.k = 10;
  scoringCtx->rrfCtx.window = 100;

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << QueryError_GetUserError(&qerr);

  // Verify individual request pipeline structures
  std::vector<ResultProcessorType> expectedIndividualPipeline = {RP_DEPLETER, RP_LOADER, RP_INDEX};
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];
    std::string pipelineName = "Request " + std::to_string(i) + " pipeline";
    VerifyPipelineChain(areq->pipeline.qctx.endProc, expectedIndividualPipeline, pipelineName);
  }

  // Verify tail pipeline structure (basic: just hybrid merger)
  // TODO: Add sorter once MOD-10549 is done MOD-10549
  std::vector<ResultProcessorType> expectedTailPipeline = {RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->pipeline.qctx.endProc, expectedTailPipeline, "Tail pipeline");

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

// Test pipeline building with three AREQ requests to verify scalability and proper chain construction
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
  AddLoadStepToPlan(&hybridReq->pipeline.ap, loadFields, 3);

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = (HybridScoringContext*)rm_calloc(1, sizeof(HybridScoringContext));
  scoringCtx->scoringType = HYBRID_SCORING_RRF;
  scoringCtx->rrfCtx.k = 10;
  scoringCtx->rrfCtx.window = 100;

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << QueryError_GetUserError(&qerr);

  // Verify individual request pipeline structures
  std::vector<ResultProcessorType> expectedIndividualPipeline = {RP_DEPLETER, RP_LOADER, RP_INDEX};
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];
    std::string pipelineName = "Request " + std::to_string(i) + " pipeline";
    VerifyPipelineChain(areq->pipeline.qctx.endProc, expectedIndividualPipeline, pipelineName);
  }

  // Verify tail pipeline structure (basic: just hybrid merger)
  // TODO: Add sorter once MOD-10549 is done MOD-10549
  std::vector<ResultProcessorType> expectedTailPipeline = {RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->pipeline.qctx.endProc, expectedTailPipeline, "Tail pipeline");

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

// Test pipeline building error handling and graceful degradation when LOAD step is missing
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

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = (HybridScoringContext*)rm_calloc(1, sizeof(HybridScoringContext));
  scoringCtx->scoringType = HYBRID_SCORING_RRF;
  scoringCtx->rrfCtx.k = 10;
  scoringCtx->rrfCtx.window = 100;

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  // Should handle missing LOAD step gracefully
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build should handle missing LOAD step: " << QueryError_GetUserError(&qerr);

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

// Test complex tail pipeline construction with LOAD, SORT, and APPLY steps in the aggregation plan
TEST_F(HybridRequestTest, testHybridRequestBuildPipelineTail) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx_complex", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "artificial", spec, &qerr);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create first AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req2 = CreateTestAREQ(ctx, "@category:technology", spec, &qerr);
  ASSERT_TRUE(req2 != nullptr) << "Failed to create second AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 2);
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);

  // Add complex AGGPlan with LOAD + SORT + APPLY steps
  const char *loadFields[] = {"title", "score", "category"};
  AddLoadStepToPlan(&hybridReq->pipeline.ap, loadFields, 3);

  // Add SORT step
  const char *sortFields[] = {"score"};
  AddSortStepToPlan(&hybridReq->pipeline.ap, sortFields, 1, SORTASCMAP_INIT);

  // Add APPLY step to create a boosted score field
  AddApplyStepToPlan(&hybridReq->pipeline.ap, "@score * 2", "boosted_score");

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = (HybridScoringContext*)rm_calloc(1, sizeof(HybridScoringContext));
  scoringCtx->scoringType = HYBRID_SCORING_RRF;
  scoringCtx->rrfCtx.k = 10;
  scoringCtx->rrfCtx.window = 100;

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = hybridReq->requests[0]->reqflags,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 5,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Complex pipeline build failed: " << QueryError_GetUserError(&qerr);

  // Verify individual request pipeline structures (should include filter for field queries)
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];
    std::string pipelineName = "Request " + std::to_string(i) + " pipeline";

    // Both requests should have the same basic pipeline structure
    std::vector<ResultProcessorType> expectedPipeline = {RP_DEPLETER, RP_LOADER, RP_INDEX};
    VerifyPipelineChain(areq->pipeline.qctx.endProc, expectedPipeline, pipelineName);
  }

  std::vector<ResultProcessorType> expectedComplexTailPipeline = {RP_PROJECTOR, RP_SORTER, RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->pipeline.qctx.endProc, expectedComplexTailPipeline, "Complex tail pipeline");

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}
