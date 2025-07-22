#pragma once
#include "aggregate/aggregate.h"
#include "pipeline/pipeline.h"
#include "hybrid/hybrid_scoring.h"

#ifdef __cplusplus
extern "C" {
#endif

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
int HybridRequest_BuildPipeline(HybridRequest *hybridReq, const HybridPipelineParams *params);
void HybridRequest_Free(HybridRequest *hybridReq);

#ifdef __cplusplus
}
#endif
