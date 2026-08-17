#pragma once
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Build the document search and scoring part of the pipeline.
 *  This creates the initial pipeline components that execute the query against
 *  the index to find matching documents and calculate their relevance scores.
 *  Construction errors are reported into `status`. */
void Pipeline_BuildQueryPart(Pipeline *pipeline, QueryPipelineParams *params, QueryError *status);

/** Build the result processing and output formatting part of the pipeline.
 *  This creates pipeline components that process search results through operations
 *  like filtering, sorting, grouping, field loading, and output formatting.
 *  There is a hidden assumption that the pipeline already contains at least one result processor to be used as an upstream.
 *  Construction errors are reported into `status`. */
int Pipeline_BuildAggregationPart(Pipeline *pipeline, const AggregationPipelineParams *params, uint32_t *outStateFlags, QueryError *status);

bool hasQuerySortby(const AGGPlan *pln);

#ifdef __cplusplus
}
#endif
