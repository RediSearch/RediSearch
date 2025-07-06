#pragma once
#include "aggregate/aggregate.h"
#include "hybrid/hybrid_scoring.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    arrayof(AREQ*) requests;
    size_t nrequests;
    QueryError tailError;
    QueryError *errors;
    QueryPipeline tail;
    HybridScoringContext scoringCtx;
} HybridRequest;

HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests);
int HybridRequest_BuildPipeline(HybridRequest *req, const AggregationPipelineParams *params, bool synchronize_read_locks);
void HybridRequest_Free(HybridRequest *req);

#ifdef __cplusplus
}
#endif