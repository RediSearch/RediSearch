/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "aggregate/aggregate.h"
#include "query_request.h"
#include "pipeline/pipeline.h"
#include "hybrid/hybrid_scoring.h"
#include "hybrid/hybrid_debug.h"
#include "util/references.h"
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

struct Cursor;

// Number of requests in a hybrid command: SEARCH + VSIM
#define HYBRID_REQUEST_NUM_SUBQUERIES 2
#define SEARCH_INDEX 0
#define VECTOR_INDEX 1
// Field name for implicit key loading in hybrid requests
#define HYBRID_IMPLICIT_KEY_FIELD "__key"

typedef struct HybridRequest {
    QueryRequest base;

    arrayof(AREQ*) requests;
    size_t nrequests;
    QueryError tailPipelineError;
    Pipeline *tailPipeline;
    RequestConfig reqConfig;
    CursorConfig cursorConfig;
    RPStatus *subqueriesReturnCodes;  // Array to store return codes from each subquery
    RedisSearchCtx *sctx;
    QEFlags reqflags;
    ProfileClocks profileClocks;
    profiler_func profile;
    ProfilePrinterCtx profileCtx;

    // Optional debug parameters for _FT.DEBUG FT.HYBRID.
    // When non-NULL, debug timeouts are applied after pipeline building.
    // Heap-allocated and owned by HybridRequest — freed in HybridRequest_Free.
    HybridDebugParams *debugParams;

    // Thread pool ID used for coordinator depletion and tail continuation jobs.
    // Set once before pipeline construction; read by BuildDistributedDepletionPipeline
    // and scheduleHybridTail.
    int poolId;
    // Index of the K value argument in the MRCommand for SHARD_K_RATIO
    // optimization.
    // Set during command building, used by command modifier callback. -1 if
    // not applicable.
    int kArgIndex;
} HybridRequest;

#ifdef __cplusplus
static_assert(offsetof(HybridRequest, base) == 0,
              "QueryRequest must be HybridRequest's first member");
#else
_Static_assert(offsetof(HybridRequest, base) == 0,
               "QueryRequest must be HybridRequest's first member");
#endif

static inline HybridRequest *QueryRequest_GetHybrid(QueryRequest *request) {
  RS_ASSERT(request != NULL);
  RS_ASSERT(request->kind == QUERY_REQUEST_KIND_HYBRID);
  return (HybridRequest *)request;
}

// The pipeline stage the hybrid request had reached, used to attribute a timeout.
static inline QueryTimeoutStage HybridRequest_ExecutionStage(HybridRequest *req) {
  return (QueryTimeoutStage)QueryRequest_GetExecutionPhase(&req->base);
}
// Advance the hybrid request's execution-phase marker (QUEUE -> PIPELINE -> REPLY).
static inline void HybridRequest_SetExecutionStage(HybridRequest *req, QueryTimeoutStage stage) {
  QueryRequest_SetExecutionPhase(&req->base, (int)stage);
}
// Propagates a hybrid timeout to every subquery AREQ so blocked RPNet waits
// observe the abort after their channels are woken.
void HybridRequest_PropagateTimeoutToSubqueries(HybridRequest *req);

static inline bool HybridRequest_RequiresThreadsSyncResults(HybridRequest *req) {
  return req->base.async.requiresAggregateResultsSync;
}

bool HybridRequest_TryClaimAggregateResults(HybridRequest *req);

void HybridRequest_SignalAggregateResultsComplete(HybridRequest *req);

void HybridRequest_WaitForAggregateResultsComplete(HybridRequest *req);

// Blocked client context for HybridRequest background execution
typedef struct blockedClientHybridCtx {
  // Borrowed; the cycle owns the request (see QueryRequest).
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
 * @param argv The command argv, not NULL; the container and every sub-request
 *   hold the full command (main-thread only — see QueryRequestArgs.argv)
 * @param argc Number of strings in argv
*/
HybridRequest *HybridRequest_New(RedisSearchCtx *sctx, AREQ **requests, size_t nrequests, RedisModuleString **argv, uint32_t argc);

/**
 * Initialize an already-allocated (zeroed) HybridRequest.
 * Used when the HybridRequest is reachable from another owner (e.g. the blocked-client cycle).
 *
 * @param hybridReq Pointer to zeroed HybridRequest to initialize
 * @param sctx The search context for the hybrid request
 * @param requests Array of AREQ pointers, the hybrid request takes ownership
 * @param nrequests Number of requests in the array
 * @param argv The full command argv each sub-request holds; not NULL
 * @param argc Number of strings in argv
 */
void HybridRequest_Init(HybridRequest *hybridReq, RedisSearchCtx *sctx, AREQ **requests, size_t nrequests, RedisModuleString **argv, uint32_t argc);

/** Starts the selected timeout source for the container and every subquery. */
void HybridRequest_BeginTimeoutCycle(HybridRequest *req, QueryRequestTimeoutKind kind);

/* Wrap the request's held argv (taken at construction) in a parse cursor.
 * The caller's argc bounds the parse; the holds may cover a superset (the
 * coordinator debug flow strips trailing debug params). */
void HybridRequest_InitArgsCursor(HybridRequest *req, ArgsCursor *ac, uint32_t argc);

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
 * @param depleteInBackground Whether the pipeline should be built for asynchronous depletion
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on failure
 */
int HybridRequest_BuildDepletionPipeline(HybridRequest *req, bool depleteInBackground);

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
 * Align the lookup keys of all source lookups with the tail lookup.
 * This function adds all keys from source lookups to the tail lookup to create a unified schema.
 *
 * @param req The HybridRequest containing multiple AREQ search requests
 */
void HybridRequest_SynchronizeLookupKeys(HybridRequest *req);

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
 * @param scoreKey The score key to use for writing the final score, could be null - won't write score in this case to the rlookup
 * @param params Pipeline parameters including aggregation settings and scoring context, this function takes ownership of the scoring context
 * @param status Query error status to report any construction errors
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on failure
 */
int HybridRequest_BuildMergePipeline(HybridRequest *req, const RLookupKey *scoreKey, HybridPipelineParams *params, QueryError *status);

/**
 * Free the heap-owned members of a HybridPipelineParams (scoring and EXPLAINSCORE
 * contexts) and NULL them out, without freeing the params struct itself.
 *
 * Safe to call repeatedly and after ownership has been transferred to the merger
 * (the relevant pointers are NULLed on transfer, so this becomes a no-op). Use it
 * to release a stack- or caller-owned HybridPipelineParams on an error path before
 * the merge pipeline is built; freeHybridParams() calls it for heap-allocated params.
 */
void HybridPipelineParams_Cleanup(HybridPipelineParams *params);

/**
 * Build the complete hybrid search pipeline.
 * This function orchestrates the construction of both the depletion and merge pipelines.
 *
 * @param req The HybridRequest to build the pipeline for
 * @param params Pipeline parameters including aggregation settings and scoring context, this function takes ownership of the scoring context
 * @param depleteInBackground Whether the pipeline should be built for asynchronous depletion
 * @param status Query error status to report any construction errors
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on failure
 */
int HybridRequest_BuildPipeline(HybridRequest *req, HybridPipelineParams *params, bool depleteInBackground, QueryError *status);

/**
 * Free a HybridRequest and all its associated resources.
 * Owner-only: see the ownership contract on QueryRequest.
 */
void HybridRequest_Free(HybridRequest *req);

int HybridRequest_GetError(HybridRequest *req, QueryError *status);

void HybridRequest_ClearErrors(HybridRequest *req);

HybridRequest *MakeDefaultHybridRequest(RedisSearchCtx *sctx, RedisModuleString **argv, uint32_t argc);

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
