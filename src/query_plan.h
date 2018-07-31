#ifndef QUERY_PLAN_H_
#define QUERY_PLAN_H_

#include "redisearch.h"
#include "search_ctx.h"
#include "index_iterator.h"
#include "result_processor.h"
#include "query.h"
#include "query_error.h"

/******************************************************************************************************
 *   Query Plan - the actual binding context of the whole execution plan - from filters to
 *   processors
 ******************************************************************************************************/

/** Indicates that all rows have been returned and no further chunks will follow */
#define QP_OUTPUT_FLAG_DONE 0x01

/**
 * Indicates that an error has been written to the output stream. More
 * information cannot be appended
 */
#define QP_OUTPUT_FLAG_ERROR 0x02

typedef int (*QueryHookCallback)(RedisModuleCtx *ctx, QueryProcessingCtx *qcx, void *privdata);
/* Hooks are callbacks that can be called before or after the query execution */
typedef struct {
  // The callback should return the number of responses it wrote to the context
  QueryHookCallback callback;
  void *privdata;
  void (*free)(void *p);
} QueryPlanHook;

typedef enum { QueryPlanHook_Pre, QueryPlanHook_Post } QueryPlanHookType;

typedef struct QueryPlan {
  RedisSearchCtx *ctx;

  IndexIterator *rootFilter;

  ResultProcessor *rootProcessor;

  QueryProcessingCtx execCtx;

  ConcurrentSearchCtx *conc;

  RSSearchOptions opts;

  // right now we allow a single pre and post hook
  // TODO: Add more
  QueryPlanHook preHook;
  QueryPlanHook postHook;

  /** Whether all rows have been returned */
  unsigned outputFlags;

  /** Whether the query should be paused temporarily */
  unsigned pause;

  /** Deferred count for RM_ReplyArray */
  unsigned count;
} QueryPlan;

/* Set the concurrent mode of the QueryParseCtx. By default it's on, setting here to 0 will turn
 * it off, resulting in the QueryParseCtx not performing context switches */
void Query_SetConcurrentMode(QueryPlan *q, int concurrent);

typedef ResultProcessor *(*ProcessorChainBuilder)(QueryPlan *plan, void *privdata,
                                                  QueryError *status);

/* Build the processor chain of the QueryParseCtx, returning the root processor */
QueryPlan *Query_BuildPlan(RedisSearchCtx *ctx, QueryParseCtx *parsedQuery, RSSearchOptions *opts,
                           ProcessorChainBuilder pcb, void *chainBuilderContext,
                           QueryError *status);

ResultProcessor *Query_BuildProcessorChain(QueryPlan *q, void *privdata, QueryError *status);

void QueryPlan_SetHook(QueryPlan *plan, QueryPlanHookType ht, QueryHookCallback cb, void *privdata,
                       void (*free)(void *));

/** Run the query plan, */
void QueryPlan_Run(QueryPlan *plan, RedisModuleCtx *outputCtx);

void QueryPlan_Free(QueryPlan *plan);

#define QueryPlan_HasError(plan) ((plan)->execCtx.state != QueryState_OK)

#endif