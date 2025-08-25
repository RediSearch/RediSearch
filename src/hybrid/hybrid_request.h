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

#ifdef __cplusplus
}
#endif
