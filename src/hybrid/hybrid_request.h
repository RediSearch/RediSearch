#pragma once
#include "aggregate/aggregate.h"
#include "pipeline/pipeline.h"
#include "hybrid/hybrid_scoring.h"

#ifdef __cplusplus
extern "C" {
#endif

// Number of requests in a hybrid command: SEARCH + VSIM
#define HYBRID_REQUEST_NUM_SUBQUERIES 2

typedef struct {
    arrayof(AREQ*) requests;
    size_t nrequests;
    QueryError tailPipelineError;
    QueryError *errors;
    Pipeline *tailPipeline;
    RequestConfig reqConfig;
    HybridPipelineParams *hybridParams;
} HybridRequest;

HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests);
int HybridRequest_BuildPipeline(HybridRequest *req, const HybridPipelineParams *params);
void HybridRequest_Execute(HybridRequest *req, RedisModuleCtx *ctx);
void HybridRequest_Free(HybridRequest *req);

#ifdef __cplusplus
}
#endif
