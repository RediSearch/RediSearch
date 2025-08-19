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
#include "hybrid/parse_hybrid.h"
#include "document.h"
#include "result_processor.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "spec.h"
#include "common.h"
#include "module.h"
#include "version.h"
#include "query_optimizer.h"

// Macro for BLOB data that all tests using $BLOB should use
#define TEST_BLOB_DATA "AQIDBAUGBwgJCg=="

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

// Helper function to get error message from HybridRequest for test assertions
std::string HREQ_GetUserError(HybridRequest* req) {
  QueryError error;
  QueryError_Init(&error);
  HREQ_GetError(req, &error);
  return QueryError_GetUserError(&error);
}

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
AREQ* CreateTestAREQ(RedisModuleCtx *ctx, const char* query, IndexSpec *spec, QueryError *status, bool isSearchSubquery = false) {
  AREQ *req = AREQ_New();
  if (isSearchSubquery) {
    AREQ_AddRequestFlags(req, QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY);
  }
  RMCK::ArgvList args(ctx, query);
  int rv = AREQ_Compile(req, args, args.size(), status);
  if (rv != REDISMODULE_OK) {
    AREQ_Free(req);
    return nullptr;
  }

  const char *specName = HiddenString_GetUnsafe(spec->specName, NULL);
  RedisModuleCtx *ctx1 = RedisModule_GetDetachedThreadSafeContext(ctx);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx1, specName, true);
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
 * This function creates an unprocessed LOAD step with proper ArgsCursor that will be
 * processed during Pipeline_BuildAggregationPart, following the same pattern as handleLoad.
 *
 * @param plan The AGGPlan to add the LOAD step to
 * @param fields Array of field names to load
 * @param nfields Number of fields in the array
 */
void AddLoadStepToPlan(AGGPlan *plan, const char **fields, size_t nfields) {
  PLN_LoadStep *loadStep = (PLN_LoadStep*)rm_calloc(1, sizeof(PLN_LoadStep));
  loadStep->base.type = PLN_T_LOAD;
  loadStep->base.dtor = loadDtor;  // Use standard destructor

  // Create unprocessed LOAD step (following handleLoad pattern)
  loadStep->nkeys = 0;  // Start unprocessed

  if (nfields > 0) {
    // Initialize ArgsCursor with the field names directly (no copying needed)
    // The field names are expected to have stable lifetime during test execution
    ArgsCursor_InitCString(&loadStep->args, fields, nfields);

    // Pre-allocate keys array (will be populated during processing, same as handleLoad)
    loadStep->keys = (const RLookupKey**)rm_calloc(nfields, sizeof(RLookupKey*));
  } else {
    // Handle empty case
    memset(&loadStep->args, 0, sizeof(ArgsCursor));
    loadStep->keys = NULL;
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

/**
 * Helper function to find the HybridMerger processor in a pipeline chain.
 * Traverses the pipeline from the end processor to find the HybridMerger.
 *
 * @param endProc The end processor of the pipeline chain
 * @return Pointer to the HybridMerger processor, or NULL if not found
 */
ResultProcessor* FindHybridMergerInPipeline(ResultProcessor *endProc) {
  ResultProcessor *current = endProc;
  while (current != nullptr) {
    if (current->type == RP_HYBRID_MERGER) {
      return current;
    }
    current = current->upstream;
  }
  return nullptr;
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
  ASSERT_TRUE(hybridReq->tailPipeline->ap.steps.next != nullptr);
  // Clean up
  HybridRequest_Free(hybridReq);
}

// Test basic pipeline building with two AREQ requests and verify the pipeline structure
TEST_F(HybridRequestTest, testHybridRequestPipelineBuildingBasic) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx2", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "machine", spec, &qerr, true);
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
  AddLoadStepToPlan(&hybridReq->tailPipeline->ap, loadFields, 2);

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = HybridScoringContext_NewRRF(10.0, 100, false);

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = QEXEC_F_IS_HYBRID_TAIL,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << HREQ_GetUserError(hybridReq);

  // Verify individual request pipeline structures
  // First request should have implicit scorer and sorter added
  // Actual order: DEPLETER -> LOADER -> SORTER -> SCORER -> INDEX
  std::vector<ResultProcessorType> expectedFirstRequestPipeline = {RP_DEPLETER, RP_LOADER, RP_SORTER, RP_SCORER, RP_INDEX};
  VerifyPipelineChain(hybridReq->requests[0]->pipeline.qctx.endProc, expectedFirstRequestPipeline, "First request pipeline");

  // Second request should have the original structure (no implicit sorting)
  std::vector<ResultProcessorType> expectedSecondRequestPipeline = {RP_DEPLETER, RP_LOADER, RP_INDEX};
  VerifyPipelineChain(hybridReq->requests[1]->pipeline.qctx.endProc, expectedSecondRequestPipeline, "Second request pipeline");

  // Verify tail pipeline structure (hybrid merger + implicit sort-by-score)
  std::vector<ResultProcessorType> expectedTailPipeline = {RP_SORTER, RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->tailPipeline->qctx.endProc, expectedTailPipeline, "Tail pipeline");

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
  AREQ *req1 = CreateTestAREQ(ctx, "artificial", spec, &qerr, true);
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
  AddLoadStepToPlan(&hybridReq->tailPipeline->ap, loadFields, 3);

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = HybridScoringContext_NewRRF(10.0, 100, false);

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = QEXEC_F_IS_HYBRID_TAIL,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << HREQ_GetUserError(hybridReq);

  // Verify individual request pipeline structures
  // First request should have implicit scorer and sorter added
  std::vector<ResultProcessorType> expectedFirstRequestPipeline = {RP_DEPLETER, RP_LOADER, RP_SORTER, RP_SCORER, RP_INDEX};
  VerifyPipelineChain(hybridReq->requests[0]->pipeline.qctx.endProc, expectedFirstRequestPipeline, "First request pipeline");

  // Other requests should have the original structure
  std::vector<ResultProcessorType> expectedOtherRequestPipeline = {RP_DEPLETER, RP_LOADER, RP_INDEX};
  for (size_t i = 1; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];
    std::string pipelineName = "Request " + std::to_string(i) + " pipeline";
    VerifyPipelineChain(areq->pipeline.qctx.endProc, expectedOtherRequestPipeline, pipelineName);
  }

  // Verify tail pipeline structure (hybrid merger + implicit sort-by-score)
  std::vector<ResultProcessorType> expectedTailPipeline = {RP_SORTER, RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->tailPipeline->qctx.endProc, expectedTailPipeline, "Tail pipeline");

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
  AREQ *req1 = CreateTestAREQ(ctx, "test", spec, &qerr, true);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create AREQ: " << QueryError_GetUserError(&qerr);

  // Create array with single request
  AREQ **requests = array_new(AREQ*, 1);
  requests = array_ensure_append_1(requests, req1);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 1);
  ASSERT_TRUE(hybridReq != nullptr);

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = HybridScoringContext_NewRRF(10.0, 100, false);

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = QEXEC_F_IS_HYBRID_TAIL,
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
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build should handle missing LOAD step: " << HREQ_GetUserError(hybridReq);

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
  AREQ *req1 = CreateTestAREQ(ctx, "artificial", spec, &qerr, true);
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
  AddLoadStepToPlan(&hybridReq->tailPipeline->ap, loadFields, 3);

  // Add SORT step
  const char *sortFields[] = {"score"};
  AddSortStepToPlan(&hybridReq->tailPipeline->ap, sortFields, 1, SORTASCMAP_INIT);

  // Add APPLY step to create a boosted score field
  AddApplyStepToPlan(&hybridReq->tailPipeline->ap, "@score * 2", "boosted_score");

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = HybridScoringContext_NewRRF(10.0, 100, false);

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = QEXEC_F_IS_HYBRID_TAIL,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 5,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Complex pipeline build failed: " << HREQ_GetUserError(hybridReq);

  // Verify individual request pipeline structures (should include filter for field queries)
  // First request should have implicit scorer and sorter added
  std::vector<ResultProcessorType> expectedFirstRequestPipeline = {RP_DEPLETER, RP_LOADER, RP_SORTER, RP_SCORER, RP_INDEX};
  VerifyPipelineChain(hybridReq->requests[0]->pipeline.qctx.endProc, expectedFirstRequestPipeline, "First request pipeline");

  // Second request should have the original structure
  std::vector<ResultProcessorType> expectedSecondRequestPipeline = {RP_DEPLETER, RP_LOADER, RP_INDEX};
  VerifyPipelineChain(hybridReq->requests[1]->pipeline.qctx.endProc, expectedSecondRequestPipeline, "Second request pipeline");

  std::vector<ResultProcessorType> expectedComplexTailPipeline = {RP_PROJECTOR, RP_SORTER, RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->tailPipeline->qctx.endProc, expectedComplexTailPipeline, "Complex tail pipeline");

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

TEST_F(HybridRequestTest, testHybridRequestImplicitLoad) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_implicit_basic", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "machine", spec, &qerr, true);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create first AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req2 = CreateTestAREQ(ctx, "learning", spec, &qerr);
  ASSERT_TRUE(req2 != nullptr) << "Failed to create second AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 2);
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  // Create HybridRequest WITHOUT adding any explicit LOAD step
  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);

  // Verify no LOAD step exists initially in any pipeline
  PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(&hybridReq->tailPipeline->ap, NULL, NULL, PLN_T_LOAD);
  EXPECT_EQ(nullptr, loadStep) << "No LOAD step should exist initially";

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = HybridScoringContext_NewRRF(10.0, 100, false);

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = QEXEC_F_IS_HYBRID_TAIL,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << HREQ_GetUserError(hybridReq);

  // Verify that implicit LOAD functionality is implemented via RPLoader result processors
  // (not PLN_LoadStep aggregation plan steps) in individual request pipelines

  // Define expected pipelines for each request
  std::vector<std::vector<ResultProcessorType>> expectedPipelines = {
    {RP_DEPLETER, RP_LOADER, RP_SORTER, RP_SCORER, RP_INDEX},  // First request pipeline
    {RP_DEPLETER, RP_LOADER, RP_INDEX}                         // Other requests pipeline
  };

  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];
    PLN_LoadStep *requestLoadStep = (PLN_LoadStep *)AGPLN_FindStep(&areq->pipeline.ap, NULL, NULL, PLN_T_LOAD);
    EXPECT_NE(nullptr, requestLoadStep) << "Request " << i << " should have PLN_LoadStep for implicit load";
    EXPECT_EQ(2, requestLoadStep->nkeys) << "Request " << i << " should have 2 keys for implicit load: " << HYBRID_IMPLICIT_KEY_FIELD << " and " << UNDERSCORE_SCORE;
    std::string pipelineName = "Request " + std::to_string(i) + " pipeline with implicit LOAD";
    VerifyPipelineChain(areq->pipeline.qctx.endProc, expectedPipelines[i], pipelineName);

    // Verify implicit load creates "__key" field with path "__key"
    RLookup *lookup = AGPLN_GetLookup(&areq->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST);
    ASSERT_NE(nullptr, lookup);

    bool foundKeyField = false;
    for (RLookupKey *key = lookup->head; key != nullptr; key = key->next) {
      if (key->name && strcmp(key->name, HYBRID_IMPLICIT_KEY_FIELD) == 0) {
        EXPECT_STREQ(HYBRID_IMPLICIT_KEY_FIELD, key->path);
        foundKeyField = true;
        break;
      }
    }
    EXPECT_TRUE(foundKeyField);
  }

  ResultProcessor *hybridMerger = FindHybridMergerInPipeline(hybridReq->tailPipeline->qctx.endProc);
  const RLookupKey *scoreKey = RPHybridMerger_GetScoreKey(hybridMerger);
  ASSERT_NE(nullptr, scoreKey) << "scoreKey should be set for implicit load case";
  EXPECT_STREQ(UNDERSCORE_SCORE, scoreKey->name) << "scoreKey should point to UNDERSCORE_SCORE field";

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

// Test explicit LOAD preservation: verify existing LOAD steps are not modified by implicit logic
TEST_F(HybridRequestTest, testHybridRequestExplicitLoadPreserved) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_explicit_preserved", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "artificial", spec, &qerr, true);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create first AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req2 = CreateTestAREQ(ctx, "intelligence", spec, &qerr);
  ASSERT_TRUE(req2 != nullptr) << "Failed to create second AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 2);
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);

  // Add explicit LOAD step with custom fields
  const char *loadFields[] = {"title", "category"};
  AddLoadStepToPlan(&hybridReq->tailPipeline->ap, loadFields, 2);

  // Verify explicit LOAD step exists
  PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(&hybridReq->tailPipeline->ap, NULL, NULL, PLN_T_LOAD);
  ASSERT_NE(nullptr, loadStep) << "Explicit LOAD step should exist";
  EXPECT_EQ(2, loadStep->args.argc) << "Explicit LOAD should have 3 fields (before processing)";

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = HybridScoringContext_NewRRF(10.0, 100, false);

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = QEXEC_F_IS_HYBRID_TAIL,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << HREQ_GetUserError(hybridReq);

  // Verify that the explicit LOAD step is preserved in individual AREQ pipelines (processed with 3 keys)
  // The tail pipeline should still have the unprocessed LOAD step
  loadStep = (PLN_LoadStep *)AGPLN_FindStep(&hybridReq->tailPipeline->ap, NULL, NULL, PLN_T_LOAD);
  ASSERT_NE(nullptr, loadStep) << "Explicit LOAD step should still exist in tail pipeline";
  EXPECT_EQ(2, loadStep->args.argc) << "Tail pipeline LOAD should still have 3 fields in args (unprocessed)";
  EXPECT_EQ(0, loadStep->nkeys) << "Tail pipeline LOAD should be unprocessed (nkeys = 0)";

  // Individual AREQ pipelines should have processed LOAD steps with 3 keys
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    PLN_LoadStep *areqLoadStep = (PLN_LoadStep *)AGPLN_FindStep(&hybridReq->requests[i]->pipeline.ap, NULL, NULL, PLN_T_LOAD);
    ASSERT_NE(nullptr, areqLoadStep) << "AREQ " << i << " should have cloned LOAD step";
    EXPECT_EQ(2, areqLoadStep->nkeys) << "AREQ " << i << " LOAD should be processed with 3 keys";
  }

  ResultProcessor *hybridMerger = FindHybridMergerInPipeline(hybridReq->tailPipeline->qctx.endProc);
  const RLookupKey *scoreKey = RPHybridMerger_GetScoreKey(hybridMerger);
  EXPECT_EQ(nullptr, scoreKey) << "scoreKey should be NULL for explicit load case";

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

// Test that implicit sort-by-score is NOT added when explicit SORTBY exists
TEST_F(HybridRequestTest, testHybridRequestNoImplicitSortWithExplicitSort) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_no_implicit_sort", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "machine", spec, &qerr, true);
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

  // Add explicit LOAD and SORT steps
  const char *loadFields[] = {"title", "score"};
  AddLoadStepToPlan(&hybridReq->tailPipeline->ap, loadFields, 2);

  const char *sortFields[] = {"title"};  // Sort by title, not score
  AddSortStepToPlan(&hybridReq->tailPipeline->ap, sortFields, 1, SORTASCMAP_INIT);

  // Verify explicit SORT step exists
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&hybridReq->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_NE(nullptr, arrangeStep) << "Explicit SORT step should exist";

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  HybridScoringContext *scoringCtx = HybridScoringContext_NewRRF(10.0, 100, false);

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = QEXEC_F_IS_HYBRID_TAIL,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 10,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << HREQ_GetUserError(hybridReq);

  // Verify tail pipeline structure: should have explicit sorter from aggregation, NOT implicit sort-by-score
  // The pipeline should be: SORTER (from aggregation) -> HYBRID_MERGER
  std::vector<ResultProcessorType> expectedTailPipeline = {RP_SORTER, RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->tailPipeline->qctx.endProc, expectedTailPipeline, "Tail pipeline with explicit sort");

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

// Test that implicit sort-by-score IS added when no explicit SORTBY exists
TEST_F(HybridRequestTest, testHybridRequestImplicitSortByScore) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_implicit_sort", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "artificial", spec, &qerr, true);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create first AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req2 = CreateTestAREQ(ctx, "intelligence", spec, &qerr);
  ASSERT_TRUE(req2 != nullptr) << "Failed to create second AREQ: " << QueryError_GetUserError(&qerr);

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 2);
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);

  // Add LOAD step but NO SORT step - this should trigger implicit sort-by-score
  const char *loadFields[] = {"title", "category"};
  AddLoadStepToPlan(&hybridReq->tailPipeline->ap, loadFields, 2);

  // Verify NO explicit SORT step exists
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&hybridReq->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  EXPECT_EQ(nullptr, arrangeStep) << "No explicit SORT step should exist initially";

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  double weights[] = {0.7, 0.3};
  HybridScoringContext *scoringCtx = HybridScoringContext_NewLinear(weights, 2);

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = QEXEC_F_IS_HYBRID_TAIL,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 20,  // Test with different limit
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << HREQ_GetUserError(hybridReq);

  // Verify tail pipeline structure: should have implicit sort-by-score added
  // The pipeline should be: SORTER (implicit sort-by-score) -> HYBRID_MERGER
  std::vector<ResultProcessorType> expectedTailPipeline = {RP_SORTER, RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->tailPipeline->qctx.endProc, expectedTailPipeline, "Tail pipeline with implicit sort-by-score");


  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

// Test that implicit sort-by-score is NOT added when first request already has explicit arrange step
TEST_F(HybridRequestTest, testHybridRequestNoImplicitSortWithExplicitFirstRequestSort) {
  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_no_implicit_first_sort", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  // Create AREQ requests
  AREQ *req1 = CreateTestAREQ(ctx, "machine", spec, &qerr, true);
  ASSERT_TRUE(req1 != nullptr) << "Failed to create first AREQ: " << QueryError_GetUserError(&qerr);

  AREQ *req2 = CreateTestAREQ(ctx, "learning", spec, &qerr);
  ASSERT_TRUE(req2 != nullptr) << "Failed to create second AREQ: " << QueryError_GetUserError(&qerr);

  // Add explicit arrange step to the FIRST REQUEST's plan (not tail pipeline)
  AGGPlan *firstRequestPlan = AREQ_AGGPlan(req1);
  const char *sortFields[] = {"title"};  // Sort by title, not score
  AddSortStepToPlan(firstRequestPlan, sortFields, 1, SORTASCMAP_INIT);

  // Verify explicit SORT step exists in first request's plan
  const PLN_BaseStep *existingArrangeStep = AGPLN_FindStep(firstRequestPlan, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_NE(nullptr, existingArrangeStep) << "First request should have explicit SORT step";

  // Create array of requests
  AREQ **requests = array_new(AREQ*, 2);
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  // Create HybridRequest
  HybridRequest *hybridReq = HybridRequest_New(requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);

  // Allocate HybridScoringContext on heap since it will be freed by the hybrid merger
  double weights[] = {0.6, 0.4};
  HybridScoringContext *scoringCtx = HybridScoringContext_NewLinear(weights, 2);

  HybridPipelineParams params = {
      .aggregationParams = {
        .common = {
          .sctx = hybridReq->requests[0]->sctx,
          .reqflags = QEXEC_F_IS_HYBRID_TAIL,
          .optimizer = hybridReq->requests[0]->optimizer,
        },
        .outFields = &hybridReq->requests[0]->outFields,
        .maxResultsLimit = 15,
      },
      .synchronize_read_locks = true,
      .scoringCtx = scoringCtx,
  };

  int rc = HybridRequest_BuildPipeline(hybridReq, &params);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed: " << HREQ_GetUserError(hybridReq);

  // Verify that the first request's plan still has exactly ONE arrange step (the explicit one)
  // and that no additional implicit score sorter was added
  size_t arrangeStepCount = 0;
  for (const DLLIST_node *nn = firstRequestPlan->steps.next; nn != &firstRequestPlan->steps; nn = nn->next) {
    const PLN_BaseStep *step = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    if (step->type == PLN_T_ARRANGE) {
      arrangeStepCount++;
    }
  }
  EXPECT_EQ(1, arrangeStepCount) << "First request should have exactly one arrange step (the explicit one)";

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}
