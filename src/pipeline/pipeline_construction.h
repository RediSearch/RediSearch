#pragma once
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Build the document search and scoring part of the pipeline.
 *  This creates the initial pipeline components that execute the query against
 *  the index to find matching documents and calculate their relevance scores. */
void Pipeline_BuildQueryPart(Pipeline *pipeline, const QueryPipelineParams *params);

/** Build the result processing and output formatting part of the pipeline.
 *  This creates pipeline components that process search results through operations
 *  like filtering, sorting, grouping, field loading, and output formatting.
 *  There is a hidden assumption that the pipeline already contains at least one result processor to be used as an upstream */
int Pipeline_BuildAggregationPart(Pipeline *pipeline, const AggregationPipelineParams *params, uint32_t *outStateFlags);

bool hasQuerySortby(const AGGPlan *pln);



/**
 * Process a complete LOAD step: parse arguments, create RPLoader, and handle JSON specs.

 *
 * @param loadStep The LOAD step to process
 * @param lookup The RLookup context to use for creating keys
 * @param sctx The search context
 * @param reqflags Request flags
 * @param loadFlags Flags to pass to RLookup_GetKey_LoadEx
 * @param forceLoad Force loading flag for RPLoader_New
 * @param outStateFlags Output state flags pointer
 * @param status Error status object for reporting failures
 * @return ResultProcessor* on success, NULL on failure
 */
ResultProcessor *processLoadStep(PLN_LoadStep *loadStep, RLookup *lookup,
                                RedisSearchCtx *sctx, uint32_t reqflags, uint32_t loadFlags,
                                bool forceLoad, uint32_t *outStateFlags, QueryError *status);

#ifdef __cplusplus
}
#endif
