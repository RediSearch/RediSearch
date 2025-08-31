#pragma once
#include "aggregate/aggregate.h"
#include "pipeline/pipeline.h"
#include "hybrid/hybrid_scoring.h"
#include "util/references.h"
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

// Number of requests in a hybrid command: SEARCH + VSIM
#define HYBRID_REQUEST_NUM_SUBQUERIES 2
// Field name for implicit key loading in hybrid requests
#define HYBRID_IMPLICIT_KEY_FIELD "__key"

typedef struct HybridRequest {
    arrayof(AREQ*) requests;
    size_t nrequests;
    QueryError tailPipelineError;
    QueryError *errors;
    Pipeline *tailPipeline;
    RequestConfig reqConfig;
    CursorConfig cursorConfig;
    clock_t initClock;  // For timing execution
    RedisSearchCtx *sctx;
    QEFlags reqflags;
} HybridRequest;

// Blocked client context for HybridRequest background execution
typedef struct blockedClientHybridCtx {
  HybridRequest *hreq;
  HybridPipelineParams *hybridParams;
  RedisModuleBlockedClient *blockedClient;
  WeakRef spec_ref;
  // We need to know what kind of cursor to open, either multiple cursors if it is an internal command(shard) or single if it is a user command(coordinator)
  bool internal;
} blockedClientHybridCtx;

/*
 * Create a new HybridRequest that manages multiple search requests for hybrid search.
 * This function initializes the hybrid request structure and sets up the tail pipeline
 * that will be used to merge and process results from all individual search requests.
 * @param sctx The main search context for the hybrid request - the redisCtx inside can change if moving to different thread
 * @param requests Array of AREQ pointers representing individual search requests, the hybrid request will take ownership of the array
 * @param nrequests Number of requests in the array
*/
HybridRequest *HybridRequest_New(RedisSearchCtx *sctx, AREQ **requests, size_t nrequests);

/**
 * Build the depletion pipeline for hybrid search processing.
 * This function constructs the first part of the hybrid search pipeline that:
 * 1. Builds individual pipelines for each AREQ (search request)
 * 2. Creates depleter processors to extract results from each pipeline concurrently
 * 3. Sets up synchronization between depleters for thread-safe operation
 *
 * The depletion pipeline architecture:
 * AREQ1 -> [Individual Pipeline] -> Depleter1
 * AREQ2 -> [Individual Pipeline] -> Depleter2
 * AREQ3 -> [Individual Pipeline] -> Depleter3
 *
 * @param req The HybridRequest containing multiple AREQ search requests
 * @param params Pipeline parameters including synchronization settings
 * @return Array of depleter processors that will feed the merge pipeline, or NULL on failure
 */
arrayof(ResultProcessor*) HybridRequest_BuildDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params);

/**
 * Build the merge pipeline for hybrid search processing.
 * This function constructs the second part of the hybrid search pipeline that:
 * 1. Sets up a hybrid merger to combine and score results from all depleter processors
 * 2. Applies aggregation processing (sorting, filtering, field loading) to merged results
 * 3. Configures the final output pipeline for result delivery
 *
 * The merge pipeline architecture:
 * Depleter1 \
 * Depleter2  -> HybridMerger -> Aggregation -> Output
 * Depleter3 /
 *
 * @param req The HybridRequest containing the tail pipeline for merging
 * @param params Pipeline parameters including aggregation settings and scoring context, this function takes ownership of the scoring context
 * @param depleters Array of depleter processors from the depletion pipeline
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on failure
 */
int HybridRequest_BuildMergePipeline(HybridRequest *req, HybridPipelineParams *params, arrayof(ResultProcessor*) depleters);

/**
 * Build the complete hybrid search pipeline.
 * This function orchestrates the construction of both the depletion and merge pipelines.
 *
 * @param req The HybridRequest to build the pipeline for
 * @param params Pipeline parameters including aggregation settings and scoring context, this function takes ownership of the scoring context
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on failure
 */
int HybridRequest_BuildPipeline(HybridRequest *req, HybridPipelineParams *params);

void HybridRequest_Free(HybridRequest *req);

int HybridRequest_GetError(HybridRequest *req, QueryError *status);

HybridRequest *MakeDefaultHybridRequest(RedisSearchCtx *sctx);

#ifdef __cplusplus
}
#endif
