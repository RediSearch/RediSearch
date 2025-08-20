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
#include "hybrid/hybrid_request.h"
#include "redismock/util.h"

class HybridRequestBasicTest : public ::testing::Test {};

// Tests that don't require full Redis Module integration

// Test basic HybridRequest creation and initialization with multiple AREQ requests
TEST_F(HybridRequestBasicTest, testHybridRequestCreationBasic) {
  // Test basic HybridRequest creation without Redis dependencies
  AREQ **requests = array_new(AREQ*, 2);
  // Initialize the AREQ structures
  AREQ *req1 = AREQ_New();
  AREQ *req2 = AREQ_New();

  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  HybridRequest *hybridReq = HybridRequest_New(NULL, requests, 2);
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



// Test that verifies key correspondence between search subqueries and tail pipeline
// This test uses a hybrid query with LOAD clause to ensure that
// RLookup_CloneInto properly handles loaded fields
TEST_F(HybridRequestTest, testKeyCorrespondenceBetweenSearchAndTailPipelines) {
  QueryError status = {QueryErrorCode(0)};

  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx_keys", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  const char *specName = HiddenString_GetUnsafe(spec->specName, NULL);

  // Create a hybrid query with SEARCH and VSIM subqueries, plus LOAD and APPLY steps
  RMCK::ArgvList args(ctx, "FT.HYBRID", specName,
                      "SEARCH", "@title:machine",
                      "VSIM", "@vector_field", TEST_BLOB_DATA,
                      "LOAD", "3", "@title", "@vector", "@category");

  // Create a fresh sctx for this test since parseHybridCommand takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, specName, true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* hybridReq = MakeDefaultHyabridRequest();
  ParseHybridCommandCtx cmd = {0};
  cmd.search = hybridReq->requests[0];
  cmd.vector = hybridReq->requests[1];
  cmd.tailPlan = &hybridReq->tailPipeline->ap;
  // Parse the hybrid command
  int rc = parseHybridCommand(ctx, args, args.size(), test_sctx, specName, &cmd, &status);
  ASSERT_TRUE(rc == REDISMODULE_OK) << "Failed to parse hybrid command: " << QueryError_GetUserError(&status);
  ASSERT_EQ(status.code, QUERY_OK);

  // Build the pipeline using the parsed hybrid parameters
  rc = HybridRequest_BuildPipeline(hybridReq, &cmd.hybridParams);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed";

  // Get the tail pipeline lookup (this is where RLookup_CloneInto was used)
  RLookup *tailLookup = AGPLN_GetLookup(&hybridReq->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
  ASSERT_TRUE(tailLookup != NULL) << "Tail pipeline should have a lookup";

  // Verify that the tail lookup has been properly initialized and populated
  ASSERT_GE(tailLookup->rowlen, 3) << "Tail lookup should have at least 3 keys: 'title', 'vector', and 'category'";

  int tailKeyCount = 0;
  for (RLookupKey *key = tailLookup->head; key; key = key->next) {
    if (key->name) {
      tailKeyCount++;
    }
  }
  ASSERT_GE(tailKeyCount, 3) << "Tail lookup should have at least 3 keys: 'title', 'vector', and 'category'";

  // Test all upstream subqueries in the hybrid request
  for (size_t reqIdx = 0; reqIdx < hybridReq->nrequests; reqIdx++) {
    AREQ *upstreamReq = hybridReq->requests[reqIdx];
    RLookup *upstreamLookup = AGPLN_GetLookup(&upstreamReq->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST);
    ASSERT_TRUE(upstreamLookup != NULL) << "Upstream request " << reqIdx << " should have a lookup";

    // Verify that the upstream lookup has been properly populated
    ASSERT_GE(upstreamLookup->rowlen, 3) << "Upstream request " << reqIdx << " should have at least 3 keys: 'title', 'vector', and 'category'";

    // Verify that every key in the upstream subquery has a corresponding key in the tail subquery
    for (RLookupKey *upstreamKey = upstreamLookup->head; upstreamKey; upstreamKey = upstreamKey->next) {
      if (!upstreamKey->name) {
        continue; // Skip overridden keys
      }

      // Find corresponding key in tail lookup by name
      RLookupKey *tailKey = NULL;
      for (RLookupKey *tk = tailLookup->head; tk; tk = tk->next) {
        if (tk->name && strcmp(tk->name, upstreamKey->name) == 0) {
          tailKey = tk;
          break;
        }
      }

      ASSERT_TRUE(tailKey != NULL)
        << "Key '" << upstreamKey->name << "' from upstream request " << reqIdx << " not found in tail pipeline";

      // Verify that the keys point to the same indices
      EXPECT_EQ(upstreamKey->dstidx, tailKey->dstidx)
        << "Key '" << upstreamKey->name << "' has different dstidx in upstream request " << reqIdx << " ("
        << upstreamKey->dstidx << ") vs tail (" << tailKey->dstidx << ")";

      EXPECT_EQ(upstreamKey->svidx, tailKey->svidx)
        << "Key '" << upstreamKey->name << "' has different svidx in upstream request " << reqIdx << " ("
        << upstreamKey->svidx << ") vs tail (" << tailKey->svidx << ")";

      // Verify path matches
      if (upstreamKey->path && tailKey->path) {
        EXPECT_STREQ(upstreamKey->path, tailKey->path)
          << "Key '" << upstreamKey->name << "' has different path in upstream request " << reqIdx << " vs tail";
      } else {
        EXPECT_EQ(upstreamKey->path, tailKey->path)
          << "Key '" << upstreamKey->name << "' path nullness differs between upstream request " << reqIdx << " and tail";
      }

      // Verify name length matches
      EXPECT_EQ(upstreamKey->name_len, tailKey->name_len)
        << "Key '" << upstreamKey->name << "' has different name_len in upstream request " << reqIdx << " vs tail";
    }
  }

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}

// Test key correspondence between search and tail pipelines with implicit loading (no LOAD clause)
TEST_F(HybridRequestTest, testKeyCorrespondenceBetweenSearchAndTailPipelinesImplicit) {
  QueryError status = {QueryErrorCode(0)};

  // Create test index spec
  IndexSpec *spec = CreateTestIndexSpec(ctx, "test_idx_keys_implicit", &qerr);
  ASSERT_TRUE(spec != nullptr) << "Failed to create index spec: " << QueryError_GetUserError(&qerr);

  const char *specName = HiddenString_GetUnsafe(spec->specName, NULL);

  // Create a hybrid query with SEARCH and VSIM subqueries, but NO LOAD clause (implicit loading)
  RMCK::ArgvList args(ctx, "FT.HYBRID", specName,
                      "SEARCH", "@title:machine",
                      "VSIM", "@vector_field", TEST_BLOB_DATA);

  // Create a fresh sctx for this test since parseHybridCommand takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, specName, true);
  ASSERT_TRUE(test_sctx != NULL);

  HybridRequest* hybridReq = MakeDefaultHyabridRequest();
  ParseHybridCommandCtx cmd = {0};
  cmd.search = hybridReq->requests[0];
  cmd.vector = hybridReq->requests[1];
  cmd.tailPlan = &hybridReq->tailPipeline->ap;

  // Parse the hybrid command
  int rc = parseHybridCommand(ctx, args, args.size(), test_sctx, specName, &cmd,&status);
  ASSERT_TRUE(rc == REDISMODULE_OK) << "Failed to parse hybrid command: " << QueryError_GetUserError(&status);
  ASSERT_EQ(status.code, QUERY_OK);

  // Build the pipeline using the parsed hybrid parameters
  rc = HybridRequest_BuildPipeline(hybridReq, &cmd.hybridParams);
  EXPECT_EQ(REDISMODULE_OK, rc) << "Pipeline build failed";

  // Get the tail pipeline lookup (this is where RLookup_CloneInto was used)
  RLookup *tailLookup = AGPLN_GetLookup(&hybridReq->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
  ASSERT_TRUE(tailLookup != NULL) << "Tail pipeline should have a lookup";

  // Verify that the tail lookup has been properly initialized and populated
  ASSERT_GE(tailLookup->rowlen, 2) << "Tail lookup should have at least 2 keys: '__key' and '__score'";

  int tailKeyCount = 0;
  for (RLookupKey *key = tailLookup->head; key; key = key->next) {
    if (key->name) {
      tailKeyCount++;
    }
  }
  ASSERT_GE(tailKeyCount, 2) << "Tail lookup should have at least 2 keys: '__key' and '__score'";

  // Verify that implicit loading creates the "__key" field in the tail pipeline
  RLookupKey *tailKeyField = NULL;
  for (RLookupKey *tk = tailLookup->head; tk; tk = tk->next) {
    const char *keyName = HYBRID_IMPLICIT_KEY_FIELD;
    if (tk->name && strcmp(tk->name, keyName) == 0) {
      tailKeyField = tk;
      break;
    }
  }
  ASSERT_TRUE(tailKeyField != NULL) << "Tail pipeline should have implicit '__key' field";
  EXPECT_STREQ(HYBRID_IMPLICIT_KEY_FIELD, tailKeyField->path) << "Implicit key field should have path '__key'";

  // Test all upstream subqueries in the hybrid request
  for (size_t reqIdx = 0; reqIdx < hybridReq->nrequests; reqIdx++) {
    AREQ *upstreamReq = hybridReq->requests[reqIdx];
    RLookup *upstreamLookup = AGPLN_GetLookup(&upstreamReq->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST);
    ASSERT_TRUE(upstreamLookup != NULL) << "Upstream request " << reqIdx << " should have a lookup";

    // Verify that the upstream lookup has been properly populated
    ASSERT_GE(upstreamLookup->rowlen, 2) << "Upstream request " << reqIdx << " should have at least 2 keys: '__key' and '__score'";

    // Verify that the upstream subquery also has the implicit "__key" field
    RLookupKey *upstreamKeyField = NULL;
    for (RLookupKey *uk = upstreamLookup->head; uk; uk = uk->next) {
      if (uk->name && strcmp(uk->name, HYBRID_IMPLICIT_KEY_FIELD) == 0) {
        upstreamKeyField = uk;
        break;
      }
    }
    ASSERT_TRUE(upstreamKeyField != NULL) << "Upstream request " << reqIdx << " should have implicit '__key' field";
    EXPECT_STREQ(HYBRID_IMPLICIT_KEY_FIELD, upstreamKeyField->path) << "Implicit key field should have path '__key' in request " << reqIdx;

    // Verify that every key in the upstream subquery has a corresponding key in the tail subquery
    for (RLookupKey *upstreamKey = upstreamLookup->head; upstreamKey; upstreamKey = upstreamKey->next) {
      if (!upstreamKey->name) {
        continue; // Skip overridden keys
      }

      // Find corresponding key in tail lookup by name
      RLookupKey *tailKey = NULL;
      for (RLookupKey *tk = tailLookup->head; tk; tk = tk->next) {
        if (tk->name && strcmp(tk->name, upstreamKey->name) == 0) {
          tailKey = tk;
          break;
        }
      }

      ASSERT_TRUE(tailKey != NULL)
        << "Key '" << upstreamKey->name << "' from upstream request " << reqIdx << " not found in tail pipeline";

      // Verify that the keys point to the same indices
      EXPECT_EQ(upstreamKey->dstidx, tailKey->dstidx)
        << "Key '" << upstreamKey->name << "' has different dstidx in upstream request " << reqIdx << " ("
        << upstreamKey->dstidx << ") vs tail (" << tailKey->dstidx << ")";

      EXPECT_EQ(upstreamKey->svidx, tailKey->svidx)
        << "Key '" << upstreamKey->name << "' has different svidx in upstream request " << reqIdx << " ("
        << upstreamKey->svidx << ") vs tail (" << tailKey->svidx << ")";

      // Verify path matches
      if (upstreamKey->path && tailKey->path) {
        EXPECT_STREQ(upstreamKey->path, tailKey->path)
          << "Key '" << upstreamKey->name << "' has different path in upstream request " << reqIdx << " vs tail";
      } else {
        EXPECT_EQ(upstreamKey->path, tailKey->path)
          << "Key '" << upstreamKey->name << "' path nullness differs between upstream request " << reqIdx << " and tail";
      }

      // Verify name length matches
      EXPECT_EQ(upstreamKey->name_len, tailKey->name_len)
        << "Key '" << upstreamKey->name << "' has different name_len in upstream request " << reqIdx << " vs tail";
    }
  }

  // Clean up
  HybridRequest_Free(hybridReq);
  IndexSpec_RemoveFromGlobals(spec->own_ref, false);
}
