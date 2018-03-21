#ifndef RS_AGGREGATE_H__
#define RS_AGGREGATE_H__
#include <result_processor.h>
#include <query.h>
#include "reducer.h"
#include "query_plan.h"
#include <value.h>

#ifndef GROUPBY_C_
typedef struct Grouper Grouper;
#endif

typedef struct {
  QueryPlan *plan;
  QueryParseCtx *parseCtx;
  CmdArg *args;
} AggregateRequest;

void Aggregate_BuildSchema();

/**
 * Note that this does not initialize the structure; use
 */
int AggregateRequest_Start(AggregateRequest *req, RedisSearchCtx *sctx, RedisModuleString **argv,
                           int argc, const char **err);
void AggregateRequest_Run(AggregateRequest *req, RedisModuleCtx *outCtx);
void AggregateRequest_Free(AggregateRequest *req);

Grouper *NewGrouper(RSMultiKey *keys, RSSortingTable *tbl);
void Grouper_Free(Grouper *p);
ResultProcessor *NewGrouperProcessor(Grouper *g, ResultProcessor *upstream);
void Grouper_AddReducer(Grouper *g, Reducer *r);

ResultProcessor *GetProjector(ResultProcessor *upstream, const char *name, const char *alias,
                              CmdArg *args, char **err);
#endif