#pragma once
#include "aggregate/aggregate.h"

typedef struct {
    arrayof(AREQ) requests;
    size_t nrequests;
    AggregationPipeline merge;
} HybridRequest;

HybridRequest *HybridRequest_New(AREQ *requests, size_t nrequests, AGGPlan *plan);
void HybridRequest_Free(HybridRequest *req);
