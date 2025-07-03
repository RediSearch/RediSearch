#pragma once

#include "aggregate/aggregate_plan.h"
#include "query.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct CommonPipelineParams {
  AGGPlan *pln;
  /** Context, owned by request */
  RedisSearchCtx *sctx;
  /** Flags controlling query output */
  uint32_t reqflags;
  /* Hold parameters for query optimizer */
  struct QOptimizer *optimizer;        
} CommonPipelineParams;

// All the fields needed in order to construct a pipeline
// All the members of this struct are const, they should not be modified during the pipeline construction
typedef struct AggregationPipelineParams {
  /** Common parameters */
  CommonPipelineParams common;

  /** Fields to be output and otherwise processed */
  FieldList *outFields;

  /** Maximum number of results to return */
  size_t maxResultsLimit;

  /** Language for highlighting **/
  RSLanguage language;
} AggregationPipelineParams;


typedef struct IndexingPipelineParams {
    CommonPipelineParams common;

    const QueryAST *ast;
    const IndexIterator *rootiter;
    const char *scorerName;
    ConcurrentSearchCtx *conc;
    RequestConfig *reqConfig;
} IndexingPipelineParams;

typedef struct QueryPipeline {
  /* plan containing the logical sequence of steps */
  AGGPlan ap;

  /** Context for iterating over the queries themselves */
  QueryProcessingCtx qctx;

} QueryPipeline;

/** Initialize the pipeline with the given status */
void QueryPipeline_Initialize(QueryPipeline *pipeline, RSTimeoutPolicy timeoutPolicy, QueryError *status);

/** Free the pipeline */
void QueryPipeline_Clean(QueryPipeline *pipeline);

#ifdef __cplusplus
}
#endif
