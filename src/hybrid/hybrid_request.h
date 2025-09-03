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
    HybridPipelineParams *hybridParams;
    clock_t initClock;  // For timing execution
    RPStatus *subqueriesReturnCodes;  // Array to store return codes from each subquery
} HybridRequest;

// Blocked client context for HybridRequest background execution
typedef struct blockedClientHybridCtx {
  HybridRequest *hreq;
  RedisModuleBlockedClient *blockedClient;
  WeakRef spec_ref;
} blockedClientHybridCtx;

HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests);
int HybridRequest_BuildPipeline(HybridRequest *req, const HybridPipelineParams *params);
int HREQ_GetError(HybridRequest *hreq, QueryError *status);
void HybridRequest_Free(HybridRequest *req);

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
