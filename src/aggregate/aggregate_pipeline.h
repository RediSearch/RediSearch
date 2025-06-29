#pragma once
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

struct QOptimizer;
int BuildPipeline(AggregationPipeline *pipeline, struct QOptimizer* optimizer, RSSearchOptions* searchOpts, QueryError *status, RSTimeoutPolicy timeoutPolicy);
/**
 * Constructs the pipeline objects needed to actually start processing
 * the requests. This does not yet start iterating over the objects
 */
int AREQ_BuildPipeline(AREQ *req, QueryError *status);

bool hasQuerySortby(const AGGPlan *pln);

#ifdef __cplusplus
}
#endif