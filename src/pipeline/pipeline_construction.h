#pragma once
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Build the indexing part of the pipeline */
void QueryPipeline_BuildIndexingPart(QueryPipeline *pipeline, const IndexingPipelineParams *params);

/** Build the aggregation part of the pipeline */
int QueryPipeline_BuildAggregationPart(QueryPipeline *pipeline, const AggregationPipelineParams *params, uint32_t *outStateFlags);

bool hasQuerySortby(const AGGPlan *pln);

#ifdef __cplusplus
}
#endif