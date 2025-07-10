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
    QueryError pipelineError;
    QueryError *errors;
    Pipeline pipeline;
} HybridRequest;

HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests);
int HybridRequest_BuildPipeline(HybridRequest *req, const HybridPipelineParams *params);
void HybridRequest_Free(HybridRequest *req);

#ifdef __cplusplus
}
#endif
