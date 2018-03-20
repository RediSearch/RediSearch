#ifndef QUERY_PLAN_H_
#define QUERY_PLAN_H_

#include "redisearch.h"
#include "search_ctx.h"
#include "index_iterator.h"
#include "result_processor.h"
#include "query.h"

/******************************************************************************************************
 *   Query Plan - the actual binding context of the whole execution plan - from filters to
 *   processors
 ******************************************************************************************************/

typedef struct QueryPlan {
  RedisSearchCtx *ctx;

  IndexIterator *rootFilter;

  ResultProcessor *rootProcessor;

  QueryProcessingCtx execCtx;

  ConcurrentSearchCtx *conc;

  RSSearchOptions opts;

  int done;  // Whether we've received an EOF from the processor
} QueryPlan;

/* Set the concurrent mode of the QueryParseCtx. By default it's on, setting here to 0 will turn
 * it off, resulting in the QueryParseCtx not performing context switches */
void Query_SetConcurrentMode(QueryPlan *q, int concurrent);

typedef ResultProcessor *(*ProcessorChainBuilder)(QueryPlan *plan, void *privdata, char **err);

/* Build the processor chain of the QueryParseCtx, returning the root processor */
QueryPlan *Query_BuildPlan(RedisSearchCtx *ctx, QueryParseCtx *parsedQuery, RSSearchOptions *opts,
                           ProcessorChainBuilder pcb, void *chainBuilderContext, char **err);

ResultProcessor *Query_BuildProcessorChain(QueryPlan *q, void *privdata, char **err);

/** Run the query plan, */
void QueryPlan_Run(QueryPlan *plan, RedisModuleCtx *outputCtx);

void QueryPlan_Free(QueryPlan *plan);

#endif