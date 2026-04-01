
#include "gtest/gtest.h"
#include "aggregate/aggregate.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_construction.h"
#include "hybrid/hybrid_request.h"
#include "hybrid/hybrid_scoring.h"
#include "hybrid/parse_hybrid.h"
#include "result_processor.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "spec.h"
#include "common.h"
#include "module.h"

#include <vector>

// Macro for BLOB data that all tests using $BLOB should use
#define TEST_BLOB_DATA "AQIDBAUGBwgJCg=="
#define VECTOR_REQUEST_INDEX 1
#define SEARCH_REQUEST_INDEX 0

class HybridRequestParseTest : public ::testing::Test {
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
  QueryError qerr = QueryError_Default();
};

// Helper function to get error message from HybridRequest for test assertions
std::string HREQ_GetUserError(HybridRequest* req) {
  QueryError error = QueryError_Default();

  HybridRequest_GetError(req, &error);
  HybridRequest_ClearErrors(req);
  return QueryError_GetUserError(&error);
}

// Helper function to verify pipeline chain structure
static void VerifyPipelineChain(ResultProcessor *endProc, const std::vector<ResultProcessorType>& expectedTypes, const std::string& pipelineName) {
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

  std::stringstream expectedStream;
  std::stringstream actualStream;
  for (size_t i = 0; i < expectedTypes.size(); i++) {
    expectedStream << RPTypeToString(expectedTypes[i]) << " ";
    actualStream << RPTypeToString(actualTypes[i]) << " ";
  }
  const std::string expected = expectedStream.str();
  const std::string actual = actualStream.str();
  for (size_t i = 0; i < expectedTypes.size(); i++) {
    EXPECT_EQ(expectedTypes[i], actualTypes[i])
      << pipelineName << " processor " << i << " is " << RPTypeToString(actualTypes[i])
      << ", expected " << RPTypeToString(expectedTypes[i]) << ", pipeline is: " << actual << " vs " << expected;
  }
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

/**
 * Helper function to create a test index spec with standard schema.
 * Reduces code duplication across tests.
 */
IndexSpec* CreateStandardTestIndexSpec(RedisModuleCtx *ctx, const char* indexName, QueryError *status) {
  RMCK::ArgvList createArgs(ctx, "FT.CREATE", indexName, "ON", "HASH", "SKIPINITIALSCAN",
                            "SCHEMA", "title", "TEXT", "score", "NUMERIC",
                            "category", "TEXT", "vector_field", "VECTOR", "FLAT", "6",
                            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE");
  return IndexSpec_CreateNew(ctx, createArgs, createArgs.size(), status);
}

/**
 * Helper function to parse a hybrid command and build the pipeline.
 * Reduces code duplication across tests by handling the common pattern of:
 * 1. Create index spec
 * 2. Parse hybrid command
 * 3. Build pipeline
 * 4. Return the built HybridRequest
 *
 * Note: The caller is responsible for calling HybridRequest_DecrRef() and IndexSpec cleanup.
 */
HybridRequest* ParseAndBuildHybridRequest(RedisModuleCtx *ctx, const char* indexName,
                                          RMCK::ArgvList& args, QueryError *status, IndexSpec **outSpec = nullptr) {
  // Create test index spec
  IndexSpec *spec = CreateStandardTestIndexSpec(ctx, indexName, status);
  if (!spec) {
    return nullptr;
  }

  if (outSpec) {
    *outSpec = spec;
  }

  const char *specName = HiddenString_GetUnsafe(spec->specName, NULL);

  // Create a fresh sctx for this test since parseHybridCommand takes ownership
  RedisSearchCtx *test_sctx = NewSearchCtxC(ctx, specName, true);
  if (!test_sctx) {
    return nullptr;
  }

  // Create HybridRequest and allocate hybrid params
  HybridRequest* hybridReq = MakeDefaultHybridRequest(test_sctx);
  if (!hybridReq) {
    return nullptr;
  }

  HybridPipelineParams hybridParams = {0};
  RequestConfig reqConfig = {0};
  CursorConfig cursorConfig = {0};

  ParseHybridCommandCtx cmd = {
    .search = hybridReq->requests[SEARCH_REQUEST_INDEX],
    .vector = hybridReq->requests[VECTOR_REQUEST_INDEX],
    .tailPlan = &hybridReq->tailPipeline->ap,
    .hybridParams = &hybridParams,
    .reqConfig = &reqConfig,
    .cursorConfig = &cursorConfig
  };

  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hybridReq, &ac, args, args.size());
  // Parse the hybrid command - this fills out hybridParams
  int rc = parseHybridCommand(ctx, &ac, test_sctx, &cmd, status, false, EXEC_NO_FLAGS);
  if (rc != REDISMODULE_OK) {
    HybridRequest_DecrRef(hybridReq);
    return nullptr;
  }

  // Build the pipeline using the parsed hybrid parameters
  rc = HybridRequest_BuildPipeline(hybridReq, cmd.hybridParams, true);
  if (rc != REDISMODULE_OK) {
    HybridRequest_DecrRef(hybridReq);
    return nullptr;
  }
  return hybridReq;
}


/**
 * Macro to create and parse/build a hybrid request with automatic cleanup.
 * Reduces boilerplate code in every test.
 *
 * Usage: HYBRID_TEST_SETUP("index_name", args_list);
 */
#define HYBRID_TEST_SETUP(indexName, argsList) \
  QueryError status = QueryError_Default(); \
  IndexSpec *spec = nullptr; \
  HybridRequest* hybridReq = ParseAndBuildHybridRequest(ctx, indexName, argsList, &status, &spec); \
  ASSERT_TRUE(hybridReq != nullptr) << "Failed to parse and build hybrid request: " << QueryError_GetUserError(&status); \
  \
  /* RAII cleanup helper */ \
  struct HybridTestCleanup { \
    HybridRequest* req; \
    IndexSpec* sp; \
    ~HybridTestCleanup() { \
      if (req) HybridRequest_DecrRef(req); \
      if (sp) IndexSpec_RemoveFromGlobals(sp->own_ref, false); \
    } \
  } cleanup{hybridReq, spec};

/**
 * Macro to verify that a hybrid request has exactly 2 subqueries (SEARCH + VSIM).
 * This is a common verification across many tests.
 */
#define VERIFY_TWO_SUBQUERIES(hybridReq) \
  ASSERT_EQ(2, hybridReq->nrequests) << "Should have exactly 2 subqueries (SEARCH and VSIM)";

/**
 * Macro to verify LOAD steps exist in all individual request pipelines.
 * This is a common verification pattern across many tests.
 */
#define VERIFY_REQUEST_LOAD_STEPS(hybridReq, expectedFieldCount) \
  do { \
    for (size_t i = 0; i < hybridReq->nrequests; i++) { \
      PLN_LoadStep *requestLoadStep = (PLN_LoadStep *)AGPLN_FindStep(&hybridReq->requests[i]->pipeline.ap, NULL, NULL, PLN_T_LOAD); \
      ASSERT_NE(nullptr, requestLoadStep) << "Request " << i << " should have LOAD step"; \
      EXPECT_EQ(expectedFieldCount, requestLoadStep->nkeys) << "Request " << i << " LOAD should have " << expectedFieldCount << " processed keys"; \
    } \
  } while(0)

// Test basic pipeline building with two AREQ requests and verify the pipeline structure
TEST_F(HybridRequestParseTest, testHybridRequestPipelineBuildingBasic) {
  // Create a hybrid query with SEARCH and VSIM subqueries, plus LOAD clause
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_idx2",
                      "SEARCH", "machine",
                      "VSIM", "@vector_field", "$BLOB",
                      "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
                      "LOAD", "2", "@title", "@score",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_idx2", args);

  // Verify that individual request pipelines have proper LOAD steps
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    PLN_LoadStep *requestLoadStep = (PLN_LoadStep *)AGPLN_FindStep(&hybridReq->requests[i]->pipeline.ap, NULL, NULL, PLN_T_LOAD);
    ASSERT_NE(nullptr, requestLoadStep) << "Request " << i << " should have LOAD step";
    EXPECT_EQ(2, requestLoadStep->nkeys) << "Request " << i << " LOAD should have 2 processed keys";
  }

  // Verify that hybrid request has the expected number of subqueries
  VERIFY_TWO_SUBQUERIES(hybridReq);
}

// Test hybrid request with RRF scoring and custom K parameter
TEST_F(HybridRequestParseTest, testHybridRequestRRFScoringWithCustomConstant) {
  // Create a hybrid query with SEARCH and VSIM subqueries, RRF scoring with custom K parameter
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_rrf_custom_constant",
                      "SEARCH", "artificial",
                      "VSIM", "@vector_field", "$BLOB",
                      "COMBINE", "RRF", "2", "CONSTANT", "10.0",
                      "LOAD", "3", "@title", "@score", "@category",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_rrf_custom_constant", args);

  // Verify that RRF scoring with custom K was properly configured
  // This is tested by verifying the pipeline builds successfully with RRF K=10.0 parameters
  VERIFY_TWO_SUBQUERIES(hybridReq);
}

// Test pipeline building with minimal hybrid query (no LOAD, no COMBINE - should use defaults)
TEST_F(HybridRequestParseTest, testHybridRequestBuildPipelineMinimal) {
  // Create a minimal hybrid query with just SEARCH and VSIM (no LOAD, no COMBINE - should use defaults)
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_idx4",
                      "SEARCH", "test",
                      "VSIM", "@vector_field", "$BLOB",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_idx4", args);

  // Verify that default RRF scoring is used when no COMBINE is specified
  // This is tested by verifying the pipeline builds successfully with default parameters
  VERIFY_TWO_SUBQUERIES(hybridReq);
}

// Test complex tail pipeline construction with LOAD, SORT, and APPLY steps in the aggregation plan
TEST_F(HybridRequestParseTest, testHybridRequestBuildPipelineTail) {
  // Create a complex hybrid query with SEARCH and VSIM subqueries, plus LOAD, SORTBY, and APPLY steps
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_idx_complex",
                      "SEARCH", "artificial",
                      "VSIM", "@vector_field", "$BLOB",
                      "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
                      "LOAD", "3", "@title", "@score", "@category",
                      "SORTBY", "1", "@score",
                      "APPLY", "@score * 2", "AS", "boosted_score",
                      "LIMIT", "0", "5",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_idx_complex", args);

  // Verify that SORT step exists in tail pipeline
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&hybridReq->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_NE(nullptr, arrangeStep) << "SORT step should exist in tail pipeline";

  // Verify that APPLY step exists in tail pipeline
  const PLN_BaseStep *applyStep = AGPLN_FindStep(&hybridReq->tailPipeline->ap, NULL, NULL, PLN_T_APPLY);
  ASSERT_NE(nullptr, applyStep) << "APPLY step should exist in tail pipeline";

  VERIFY_REQUEST_LOAD_STEPS(hybridReq, 3);
}

TEST_F(HybridRequestParseTest, testHybridRequestImplicitLoad) {
  // Create a hybrid query with SEARCH and VSIM subqueries, but NO LOAD clause (implicit loading)
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_implicit_basic",
                      "SEARCH", "machine",
                      "VSIM", "@vector_field", "$BLOB",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_implicit_basic", args);

  // Verify that implicit LOAD functionality is implemented via RPLoader result processors
  // (not PLN_LoadStep aggregation plan steps) in individual request pipelines

  // Define expected pipelines for each request
  std::vector<std::vector<ResultProcessorType>> expectedPipelines = {
    {RP_SAFE_DEPLETER, RP_LOADER, RP_SORTER, RP_SCORER, RP_INDEX},  // First request pipeline
    {RP_SAFE_DEPLETER, RP_LOADER, RP_VECTOR_NORMALIZER, RP_METRICS, RP_INDEX}  // Other requests pipeline
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
    RLookupIterator iter = RLookup_Iter(lookup);
    const RLookupKey* key;
    while (RLookupIterator_Next(&iter, &key)) {
      if (RLookupKey_GetName(key) && strcmp(RLookupKey_GetName(key), HYBRID_IMPLICIT_KEY_FIELD) == 0) {
        EXPECT_STREQ(HYBRID_IMPLICIT_KEY_FIELD, RLookupKey_GetPath(key));
        foundKeyField = true;
        break;
      }
    }
    EXPECT_TRUE(foundKeyField);
  }

  ResultProcessor *hybridMerger = FindHybridMergerInPipeline(hybridReq->tailPipeline->qctx.endProc);
  const RLookupKey *scoreKey = RPHybridMerger_GetScoreKey(hybridMerger);
  ASSERT_NE(nullptr, scoreKey) << "scoreKey should be set for implicit load case";
  EXPECT_STREQ(UNDERSCORE_SCORE, RLookupKey_GetName(scoreKey)) << "scoreKey should point to UNDERSCORE_SCORE field";
}


TEST_F(HybridRequestParseTest, testHybridRequestMultipleLoads) {
  // Create a hybrid query with SEARCH and VSIM subqueries, plus multiple LOAD clauses
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_multiple_loads",
                      "SEARCH", "machine",
                      "VSIM", "@vector_field", "$BLOB",
                      "LOAD", "2", "@__score", "@title",
                      "LOAD", "1", "@__key",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_multiple_loads", args);

  // Verify that the tail plan should have no LOAD steps remaining (they should all be moved to subqueries)
  const PLN_BaseStep *tailLoadStep = AGPLN_FindStep(&hybridReq->tailPipeline->ap, NULL, NULL, PLN_T_LOAD);
  EXPECT_EQ(nullptr, tailLoadStep) << "Tail pipeline should have no LOAD steps after distribution";

  // Verify that each subquery received ALL the load steps (not just one)
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    AREQ *areq = hybridReq->requests[i];

    // Count the number of LOAD steps in this subquery - should be 2 (one for each original LOAD clause)
    int loadStepCount = 0;
    PLN_LoadStep *loadStep;
    while ((loadStep = (PLN_LoadStep *)AGPLN_FindStep(&areq->pipeline.ap, NULL, NULL, PLN_T_LOAD)) != nullptr) {
      loadStepCount++;
      AGPLN_PopStep(&loadStep->base);  // Pop it so we can find the next one
      loadStep->base.dtor(&loadStep->base);  // Clean up the popped step
    }
    EXPECT_EQ(2, loadStepCount) << "Request " << i << " should have 2 LOAD steps (cloned from both original LOAD clauses)";

    // Verify the lookup contains all expected fields
    RLookup *lookup = AGPLN_GetLookup(&areq->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST);
    ASSERT_NE(nullptr, lookup);

    // Check for presence of all expected loaded fields
    std::vector<std::string> expectedFields = {"__score", "title", "__key"};
    for (const std::string& expectedField : expectedFields) {
      bool foundField = false;
      RLookupIterator iter = RLookup_Iter(lookup);
      const RLookupKey* key;
      while (RLookupIterator_Next(&iter, &key)) {
        if (RLookupKey_GetName(key) && strcmp(RLookupKey_GetName(key), expectedField.c_str()) == 0) {
          foundField = true;
          break;
        }
      }
      EXPECT_TRUE(foundField) << "Request " << i << " should contain field " << expectedField;
    }
  }
}


// Test explicit LOAD preservation: verify existing LOAD steps are not modified by implicit logic
TEST_F(HybridRequestParseTest, testHybridRequestExplicitLoadPreserved) {
  // Create a hybrid query with SEARCH and VSIM subqueries, plus explicit LOAD clause
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_explicit_preserved",
                      "SEARCH", "artificial",
                      "VSIM", "@vector_field", "$BLOB",
                      "LOAD", "2", "@title", "@category",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_explicit_preserved", args);

  // Individual AREQ pipelines should have processed LOAD steps with 3 keys
  for (size_t i = 0; i < hybridReq->nrequests; i++) {
    PLN_LoadStep *areqLoadStep = (PLN_LoadStep *)AGPLN_FindStep(&hybridReq->requests[i]->pipeline.ap, NULL, NULL, PLN_T_LOAD);
    ASSERT_NE(nullptr, areqLoadStep) << "AREQ " << i << " should have cloned LOAD step";
    EXPECT_EQ(2, areqLoadStep->nkeys) << "AREQ " << i << " LOAD should be processed with 3 keys";
  }

  ResultProcessor *hybridMerger = FindHybridMergerInPipeline(hybridReq->tailPipeline->qctx.endProc);
  const RLookupKey *scoreKey = RPHybridMerger_GetScoreKey(hybridMerger);
  EXPECT_EQ(nullptr, scoreKey) << "scoreKey should be NULL for explicit load case";
}

// Test that implicit sort-by-score is NOT added when explicit SORTBY exists
TEST_F(HybridRequestParseTest, testHybridRequestNoImplicitSortWithExplicitSort) {
  // Create a hybrid query with SEARCH and VSIM subqueries, plus LOAD and SORTBY clauses
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_no_implicit_sort",
                      "SEARCH", "machine",
                      "VSIM", "@vector_field", "$BLOB",
                      "LOAD", "2", "@title", "@score",
                      "SORTBY", "1", "@title",  // Sort by title, not score
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);


  HYBRID_TEST_SETUP("test_no_implicit_sort", args);

  // Verify that explicit SORT step exists in tail pipeline
  const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&hybridReq->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_NE(nullptr, arrangeStep) << "Explicit SORT step should exist";

  // Verify tail pipeline structure: should have explicit sorter from aggregation, NOT implicit sort-by-score
  // The pipeline should be: SORTER (from aggregation) -> HYBRID_MERGER
  std::vector<ResultProcessorType> expectedTailPipeline = {RP_SORTER, RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->tailPipeline->qctx.endProc, expectedTailPipeline, "Tail pipeline with explicit sort");
}

// Test that implicit sort-by-score IS added when no explicit SORTBY exists
TEST_F(HybridRequestParseTest, testHybridRequestImplicitSortByScore) {
  // Create a hybrid query with SEARCH and VSIM subqueries, plus LOAD but NO SORTBY (should trigger implicit sort)
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_implicit_sort",
                      "SEARCH", "artificial",
                      "VSIM", "@vector_field", "$BLOB",
                      "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
                      "LOAD", "2", "@title", "@category",
                      "LIMIT", "0", "20",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_implicit_sort", args);

  // Verify tail pipeline structure: should have implicit sort-by-score added
  // The pipeline should be: SORTER (implicit sort-by-score) -> HYBRID_MERGER
  std::vector<ResultProcessorType> expectedTailPipeline = {RP_SORTER, RP_HYBRID_MERGER};
  VerifyPipelineChain(hybridReq->tailPipeline->qctx.endProc, expectedTailPipeline, "Tail pipeline with implicit sort-by-score");
}

// Test hybrid request with LINEAR scoring and custom LIMIT
TEST_F(HybridRequestParseTest, testHybridRequestLinearScoringWithLimit) {
  // Create a hybrid query with SEARCH and VSIM subqueries, LINEAR scoring, and custom LIMIT
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_linear_scoring",
                      "SEARCH", "machine",
                      "VSIM", "@vector_field", "$BLOB",
                      "COMBINE", "LINEAR", "4", "ALPHA", "0.6", "BETA", "0.4",
                      "LIMIT", "0", "15",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_linear_scoring", args);

  // Verify that LINEAR scoring was properly configured
  // This is tested by verifying the pipeline builds successfully with LINEAR scoring parameters
  VERIFY_TWO_SUBQUERIES(hybridReq);
}

// Test that RRF window parameter properly propagates to search subquery's arrange step limit
TEST_F(HybridRequestParseTest, testHybridRequestRRFWindowArrangeStep) {
  // Create a hybrid query with RRF scoring and WINDOW=5
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_rrf_window_arrange",
                      "SEARCH", "machine",
                      "VSIM", "@vector_field", "$BLOB",
                      "COMBINE", "RRF", "4", "CONSTANT", "60.0", "WINDOW", "5",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_rrf_window_arrange", args);
  VERIFY_TWO_SUBQUERIES(hybridReq);

  // Verify that the RRF window size propagated to the arrange step limit in search subquery
  AREQ *searchReq = hybridReq->requests[0]; // First request should be SEARCH
  ASSERT_NE(nullptr, searchReq);

  // Find the arrange step in the search request pipeline
  PLN_ArrangeStep *arrangeStep = (PLN_ArrangeStep *)AGPLN_FindStep(&searchReq->pipeline.ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_NE(nullptr, arrangeStep) << "Search request should have an arrange step";

  // Verify that the arrange step limit matches the RRF window size
  EXPECT_EQ(5, arrangeStep->limit) << "ArrangeStep limit should match RRF WINDOW parameter";
  EXPECT_EQ(0, arrangeStep->offset) << "ArrangeStep offset should be 0";

}

// Test that LINEAR window parameter properly propagates to search subquery's arrange step limit
TEST_F(HybridRequestParseTest, testHybridRequestLinearWindowArrangeStep) {
  // Create a hybrid query with LINEAR scoring and WINDOW=5
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_linear_window_arrange",
                      "SEARCH", "artificial",
                      "VSIM", "@vector_field", "$BLOB",
                      "COMBINE", "LINEAR", "6", "ALPHA", "0.7", "BETA", "0.3", "WINDOW", "5",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_linear_window_arrange", args);

  // Verify that the LINEAR window size propagated to the arrange step limit in search subquery
  AREQ *searchReq = hybridReq->requests[0]; // First request should be SEARCH
  ASSERT_NE(nullptr, searchReq);

  // Find the arrange step in the search request pipeline
  PLN_ArrangeStep *arrangeStep = (PLN_ArrangeStep *)AGPLN_FindStep(&searchReq->pipeline.ap, NULL, NULL, PLN_T_ARRANGE);
  ASSERT_NE(nullptr, arrangeStep) << "Search request should have an arrange step";

  // Verify that the arrange step limit matches the LINEAR window size
  EXPECT_EQ(5, arrangeStep->limit) << "ArrangeStep limit should match LINEAR WINDOW parameter";
  EXPECT_EQ(0, arrangeStep->offset) << "ArrangeStep offset should be 0";

  VERIFY_TWO_SUBQUERIES(hybridReq);
}

// Test that verifies key correspondence between search subqueries and tail pipeline
// This test uses a hybrid query with LOAD clause to ensure that
// RLookup_CloneInto properly handles loaded fields
TEST_F(HybridRequestParseTest, testKeyCorrespondenceBetweenSearchAndTailPipelines) {
  // Create a hybrid query with SEARCH and VSIM subqueries, plus LOAD and APPLY steps
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_idx_keys",
                      "SEARCH", "@title:machine",
                      "VSIM", "@vector_field", "$BLOB",
                      "LOAD", "3", "@title", "@vector", "@category",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_idx_keys", args);

  // Get the tail pipeline lookup (this is where RLookup_CloneInto was used)
  RLookup *tailLookup = AGPLN_GetLookup(&hybridReq->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
  ASSERT_TRUE(tailLookup != NULL) << "Tail pipeline should have a lookup";

  // Verify that the tail lookup has been properly initialized and populated
  ASSERT_GE(RLookup_GetRowLen(tailLookup), 3) << "Tail lookup should have at least 3 keys: 'title', 'vector', and 'category'";

  int tailKeyCount = 0;
  RLookupIterator iter = RLookup_Iter(tailLookup);
  const RLookupKey* key;
  while (RLookupIterator_Next(&iter, &key)) {
    if (RLookupKey_GetName(key)) {
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
    ASSERT_GE(RLookup_GetRowLen(upstreamLookup), 3) << "Upstream request " << reqIdx << " should have at least 3 keys: 'title', 'vector', and 'category'";

    // Verify that every key in the upstream subquery has a corresponding key in the tail subquery
    RLookupIterator iter = RLookup_Iter(upstreamLookup);
    const RLookupKey* upstreamKey;
    while (RLookupIterator_Next(&iter, &upstreamKey)) {
      if (!RLookupKey_GetName(upstreamKey)) {
        continue; // Skip overridden keys
      }

      // Find corresponding key in tail lookup by name
      const RLookupKey *tailKey = NULL;
      RLookupIterator iter = RLookup_Iter(tailLookup);
      const RLookupKey* tk;
      while (RLookupIterator_Next(&iter, &tk)) {
        if (RLookupKey_GetName(tk) && strcmp(RLookupKey_GetName(tk), RLookupKey_GetName(upstreamKey)) == 0) {
          tailKey = tk;
          break;
        }
      }

      ASSERT_TRUE(tailKey != NULL)
        << "Key '" << RLookupKey_GetName(upstreamKey) << "' from upstream request " << reqIdx << " not found in tail pipeline";

      // Verify path matches
      if (RLookupKey_GetPath(upstreamKey) && RLookupKey_GetPath(tailKey)) {
        EXPECT_STREQ(RLookupKey_GetPath(upstreamKey), RLookupKey_GetPath(tailKey))
          << "Key '" << RLookupKey_GetName(upstreamKey) << "' has different path in upstream request " << reqIdx << " vs tail";
      } else {
        EXPECT_EQ(RLookupKey_GetPath(upstreamKey), RLookupKey_GetPath(tailKey))
          << "Key '" << RLookupKey_GetName(upstreamKey) << "' path nullness differs between upstream request " << reqIdx << " and tail";
      }

      // Verify name length matches
      EXPECT_EQ(RLookupKey_GetNameLen(upstreamKey), RLookupKey_GetNameLen(tailKey))
        << "Key '" << RLookupKey_GetName(upstreamKey) << "' has different name_len in upstream request " << reqIdx << " vs tail";
    }
  }
}

// Test key correspondence between search and tail pipelines with implicit loading (no LOAD clause)
TEST_F(HybridRequestParseTest, testKeyCorrespondenceBetweenSearchAndTailPipelinesImplicit) {
  // Create a hybrid query with SEARCH and VSIM subqueries, but NO LOAD clause (implicit loading)
  RMCK::ArgvList args(ctx, "FT.HYBRID", "test_idx_keys_implicit",
                      "SEARCH", "@title:machine",
                      "VSIM", "@vector_field", "$BLOB",
                      "PARAMS", "2", "BLOB", TEST_BLOB_DATA);

  HYBRID_TEST_SETUP("test_idx_keys_implicit", args);

  // Get the tail pipeline lookup (this is where RLookup_CloneInto was used)
  RLookup *tailLookup = AGPLN_GetLookup(&hybridReq->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
  ASSERT_TRUE(tailLookup != NULL) << "Tail pipeline should have a lookup";

  // Verify that the tail lookup has been properly initialized and populated
  ASSERT_GE(RLookup_GetRowLen(tailLookup), 2) << "Tail lookup should have at least 2 keys: '__key' and '__score'";

  int tailKeyCount = 0;
  RLookupIterator iter = RLookup_Iter(tailLookup);
  const RLookupKey* key;
  while (RLookupIterator_Next(&iter, &key)) {
    if (RLookupKey_GetName(key)) {
      tailKeyCount++;
    }
  }
  ASSERT_GE(tailKeyCount, 2) << "Tail lookup should have at least 2 keys: '__key' and '__score'";

  // Verify that implicit loading creates the "__key" field in the tail pipeline
  const RLookupKey *tailKeyField = NULL;
  RLookupIterator iter2 = RLookup_Iter(tailLookup);
  const RLookupKey* tk;
  while (RLookupIterator_Next(&iter2, &tk)) {
    const char *keyName = HYBRID_IMPLICIT_KEY_FIELD;
    if (RLookupKey_GetName(tk) && strcmp(RLookupKey_GetName(tk), keyName) == 0) {
      tailKeyField = tk;
      break;
    }
  }
  ASSERT_TRUE(tailKeyField != NULL) << "Tail pipeline should have implicit '__key' field";
  EXPECT_STREQ(HYBRID_IMPLICIT_KEY_FIELD, RLookupKey_GetPath(tailKeyField)) << "Implicit key field should have path '__key'";

  // Test all upstream subqueries in the hybrid request
  for (size_t reqIdx = 0; reqIdx < hybridReq->nrequests; reqIdx++) {
    AREQ *upstreamReq = hybridReq->requests[reqIdx];
    RLookup *upstreamLookup = AGPLN_GetLookup(&upstreamReq->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST);
    ASSERT_TRUE(upstreamLookup != NULL) << "Upstream request " << reqIdx << " should have a lookup";

    // Verify that the upstream lookup has been properly populated
    ASSERT_GE(RLookup_GetRowLen(upstreamLookup), 2) << "Upstream request " << reqIdx << " should have at least 2 keys: '__key' and '__score'";

    // Verify that the upstream subquery also has the implicit "__key" field
    const RLookupKey *upstreamKeyField = NULL;
    RLookupIterator iter = RLookup_Iter(upstreamLookup);
    const RLookupKey* uk;
    while (RLookupIterator_Next(&iter, &uk)) {
      if (RLookupKey_GetName(uk) && strcmp(RLookupKey_GetName(uk), HYBRID_IMPLICIT_KEY_FIELD) == 0) {
        upstreamKeyField = uk;
        break;
      }
    }
    ASSERT_TRUE(upstreamKeyField != NULL) << "Upstream request " << reqIdx << " should have implicit '__key' field";
    EXPECT_STREQ(HYBRID_IMPLICIT_KEY_FIELD, RLookupKey_GetPath(upstreamKeyField)) << "Implicit key field should have path '__key' in request " << reqIdx;

    // Verify that every key in the upstream subquery has a corresponding key in the tail subquery
    RLookupIterator iter2 = RLookup_Iter(upstreamLookup);
    const RLookupKey* upstreamKey;
    while (RLookupIterator_Next(&iter2, &upstreamKey)) {
      if (!RLookupKey_GetName(upstreamKey)) {
        continue; // Skip overridden keys
      }

      // Find corresponding key in tail lookup by name
      const RLookupKey *tailKey = NULL;
      RLookupIterator iter = RLookup_Iter(tailLookup);
      const RLookupKey* tk;
      while (RLookupIterator_Next(&iter, &tk)) {
        if (RLookupKey_GetName(tk) && strcmp(RLookupKey_GetName(tk), RLookupKey_GetName(upstreamKey)) == 0) {
          tailKey = tk;
          break;
        }
      }

      ASSERT_TRUE(tailKey != NULL)
        << "Key '" << RLookupKey_GetName(upstreamKey) << "' from upstream request " << reqIdx << " not found in tail pipeline";
      // Verify path matches
      if (RLookupKey_GetPath(upstreamKey) && RLookupKey_GetPath(tailKey)) {
        EXPECT_STREQ(RLookupKey_GetPath(upstreamKey), RLookupKey_GetPath(tailKey))
          << "Key '" << RLookupKey_GetName(upstreamKey) << "' has different path in upstream request " << reqIdx << " vs tail";
      } else {
        EXPECT_EQ(RLookupKey_GetPath(upstreamKey), RLookupKey_GetPath(tailKey))
          << "Key '" << RLookupKey_GetName(upstreamKey) << "' path nullness differs between upstream request " << reqIdx << " and tail";
      }

      // Verify name length matches
      EXPECT_EQ(RLookupKey_GetNameLen(upstreamKey), RLookupKey_GetNameLen(tailKey))
        << "Key '" << RLookupKey_GetName(upstreamKey) << "' has different name_len in upstream request " << reqIdx << " vs tail";
    }
  }
}
