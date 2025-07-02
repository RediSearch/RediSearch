#pragma once
#include "aggregate/aggregate.h"
#include "hybrid/hybrid_scoring.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    arrayof(AREQ*) requests;
    size_t nrequests;
    AggregationPipeline merge;
    HybridScoringContext scoringCtx;
} HybridRequest;

HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests, AGGPlan *plan);
int HybridRequest_BuildPipeline(HybridRequest *req, QueryError *status, RSSearchOptions *searchOpts);
void HybridRequest_Free(HybridRequest *req);

#ifdef __cplusplus
}
#endif