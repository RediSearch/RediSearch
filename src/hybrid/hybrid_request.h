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
    AGGPlan *ap;
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
  bool internal;
  bool coordinator;
} blockedClientHybridCtx;

HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests);
arrayof(ResultProcessor*) HybridRequest_BuildDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params);
int HybridRequest_BuildMergePipeline(HybridRequest *req, const HybridPipelineParams *params, arrayof(ResultProcessor*) depleters);
int HybridRequest_BuildPipeline(HybridRequest *req, HybridPipelineParams *params, QueryError *status);
void HybridRequest_Free(HybridRequest *req);
HybridRequest *MakeDefaultHybridRequest();

#ifdef __cplusplus
}
#endif
