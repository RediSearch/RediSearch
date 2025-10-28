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
#define SEARCH_INDEX 0
#define VECTOR_INDEX 1
// Field name for implicit key loading in hybrid requests
#define HYBRID_IMPLICIT_KEY_FIELD "__key"

typedef struct HybridRequest {
    /* Arguments converted to sds. Received on input */
    // We need to copy the arguments so rlookup keys can point to them
    // in short lifetime of the strings
    sds *args;
    size_t nargs;

    arrayof(AREQ*) requests;
    size_t nrequests;
    QueryError tailPipelineError;
    QueryError *errors;
    Pipeline *tailPipeline;
    RequestConfig reqConfig;
    CursorConfig cursorConfig;
    clock_t initClock;  // For timing execution
    RPStatus *subqueriesReturnCodes;  // Array to store return codes from each subquery
    RedisSearchCtx *sctx;
    QEFlags reqflags;
} HybridRequest;

// Blocked client context for HybridRequest background execution
typedef struct blockedClientHybridCtx {
  // We keep a strong ref mainly for the sake of cursors amd life time management
  // On the caller side it needs to know when he can free the hybrid request - especially when an error occurred.
  StrongRef hybrid_ref;
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

/*
* We need to clone the arguments so the objects that rely on them can use them throughout the lifetime of the hybrid request
* For example lookup keys
*/
void HybridRequest_InitArgsCursor(HybridRequest *req, ArgsCursor* ac, RedisModuleString **argv, int argc);

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
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on failure
 */
int HybridRequest_BuildDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params);

/**
 * Open the score key in the tail lookup for writing the final score.
 * If a score alias is provided, create a new key with that alias.
 * Otherwise, use the default score key.
 *
 * @param tailLookup The tail lookup to open the score key in
 * @param scoreAlias The alias to use for the score key, or NULL to use the default
 * @param status Query error status to report any errors
 * @return Pointer to the opened score key, or NULL on error
 */
const RLookupKey *OpenMergeScoreKey(RLookup *tailLookup, const char *scoreAlias, QueryError *status);

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
 * @param lookupCtx The lookup context for field merging
 * @param scoreKey The score key to use for writing the final score, could be null - won't write score in this case to the rlookup
 * @param params Pipeline parameters including aggregation settings and scoring context, this function takes ownership of the scoring context
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on failure
 */
int HybridRequest_BuildMergePipeline(HybridRequest *req, HybridLookupContext *lookupCtx, const RLookupKey *scoreKey, HybridPipelineParams *params);

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

void HybridRequest_ClearErrors(HybridRequest *req);

int HybridRequest_GetError(HybridRequest *req, QueryError *status);

HybridRequest *MakeDefaultHybridRequest(RedisSearchCtx *sctx);

/**
 * Add information to validation error messages based on request type (VSIM/SEARCH subquery).
 *
 * @param req    The aggregate request containing request flags for context determination
 * @param status The query error status to potentially modify with additional context
 */
void AddValidationErrorContext(AREQ *req, QueryError *status);

inline AGGPlan *HybridRequest_TailAGGPlan(HybridRequest *hreq) {
  return &hreq->tailPipeline->ap;
}

#ifdef __cplusplus
}
#endif
