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
    RPStatus *subqueriesReturnCodes;  // Array to store return codes from each subquery
    RedisSearchCtx *sctx;
    QEFlags reqflags;
} HybridRequest;

// Blocked client context for HybridRequest background execution
typedef struct blockedClientHybridCtx {
  HybridRequest *hreq;
  HybridPipelineParams *hybridParams;
  RedisModuleBlockedClient *blockedClient;
  WeakRef spec_ref;
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
arrayof(ResultProcessor*) HybridRequest_BuildDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params);
int HybridRequest_BuildMergePipeline(HybridRequest *req, const HybridPipelineParams *params, arrayof(ResultProcessor*) depleters);
int HybridRequest_BuildPipeline(HybridRequest *req, HybridPipelineParams *params);
void HybridRequest_Free(HybridRequest *req);
int HybridRequest_GetError(HybridRequest *req, QueryError *status);
HybridRequest *MakeDefaultHybridRequest(RedisSearchCtx *sctx);

/**
 * Add information to validation error messages based on request type (VSIM/SEARCH subquery).
 *
 * @param req    The aggregate request containing request flags for context determination
 * @param status The query error status to potentially modify with additional context
 */
void AddValidationErrorContext(AREQ *req, QueryError *status);

#ifdef __cplusplus
}
#endif
