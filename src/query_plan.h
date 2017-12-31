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
  RedisModuleBlockedClient *bc;
  IndexIterator *rootFilter;

  ResultProcessor *rootProcessor;

  QueryProcessingCtx execCtx;

  ConcurrentSearchCtx *conc;

  RSSearchOptions opts;

} QueryPlan;

/* Set the concurrent mode of the QueryParseCtx. By default it's on, setting here to 0 will turn
 * it off, resulting in the QueryParseCtx not performing context switches */
void Query_SetConcurrentMode(QueryPlan *q, int concurrent);

typedef ResultProcessor *(*ProcessorChainBuilder)(QueryPlan *plan, void *privdata);

/* Build the processor chain of the QueryParseCtx, returning the root processor */
QueryPlan *Query_BuildPlan(RedisSearchCtx *ctx, QueryParseCtx *parsedQuery, RSSearchOptions *opts,
                           ProcessorChainBuilder pcb, void *chainBuilderContext);

ResultProcessor *Query_BuildProcessorChain(QueryPlan *q, void *privdata);

int QueryPlan_ProcessMainThread(RedisSearchCtx *sctx, QueryPlan *plan);

int QueryPlan_ProcessInThreadpool(RedisModuleCtx *ctx, QueryPlan *plan);

/* Lazily execute the parsed QueryParseCtx and all its stages, and return a final result
 * object */
int QueryPlan_Execute(QueryPlan *ctx, const char **err);

void QueryPlan_Free(QueryPlan *plan);

#endif