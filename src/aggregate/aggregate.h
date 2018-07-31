#ifndef RS_AGGREGATE_H__
#define RS_AGGREGATE_H__
#include <result_processor.h>
#include <query.h>
#include "reducer.h"
#include "expr/expression.h"
#include <query_plan.h>
#include "aggregate_plan.h"
#include <value.h>

#ifndef GROUPBY_C_
typedef struct Grouper Grouper;
#endif

CmdSchemaNode *GetAggregateRequestSchema();

typedef struct {
  QueryPlan *plan;
  QueryParseCtx *parseCtx;
  AggregatePlan ap;
  CmdArg *args;

  /**
   * If this pointer is heap allocated, in which case the pointer itself is
   * freed during AR_Free()
   */
  int isHeapAlloc;
} AggregateRequest;

void Aggregate_BuildSchema();

ResultProcessor *Aggregate_DefaultChainBuilder(QueryPlan *plan, void *ctx, QueryError *status);

// Don't enable concurrent mode.
#define AGGREGATE_REQUEST_NO_CONCURRENT 0x01

// Only generate the plan
#define AGGREGATE_REQUEST_NO_PARSE_QUERY 0x02

// Don't attempt to open the spec
#define AGGREGATE_REQUEST_SPECLESS 0x04

typedef struct {
  ProcessorChainBuilder pcb;
  const char *cursorLookupName;  // Override the index name in the SearchCtx
  int flags;                     // AGGREGATE_REQUEST_XXX
} AggregateRequestSettings;

/**
 * Note that this does not initialize the structure; use
 */
int AggregateRequest_Start(AggregateRequest *req, RedisSearchCtx *sctx,
                           const AggregateRequestSettings *settings, RedisModuleString **argv,
                           int argc, QueryError *status);
void AggregateRequest_Run(AggregateRequest *req, RedisModuleCtx *outCtx);
void AggregateRequest_Free(AggregateRequest *req);

/**
 * Persist the request. This safely converts a stack allocated request to
 * one allocated on the heap. This assumes that `req` lives on the stack.
 *
 * The current implementation simply does a malloc and memcpy, but this is
 * abstracted in case the request's own members contain references to it.
 */
AggregateRequest *AggregateRequest_Persist(AggregateRequest *req);

Grouper *NewGrouper(RSMultiKey *keys, RSSortingTable *tbl);
void Grouper_Free(Grouper *p);
ResultProcessor *NewGrouperProcessor(Grouper *g, ResultProcessor *upstream);
void Grouper_AddReducer(Grouper *g, Reducer *r);

ResultProcessor *GetProjector(ResultProcessor *upstream, const char *name, const char *alias,
                              CmdArg *args, QueryError *status);

ResultProcessor *NewFilter(RedisSearchCtx *sctx, ResultProcessor *upstream, const char *expr,
                           size_t len, QueryError *status);

// Entry points
void AggregateCommand_ExecAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                    struct ConcurrentCmdCtx *cmdCtx);
void AggregateCommand_ExecAggregateEx(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                      struct ConcurrentCmdCtx *cmdCtx,
                                      const AggregateRequestSettings *setings);
void AggregateCommand_ExecCursor(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                 struct ConcurrentCmdCtx *);

#endif