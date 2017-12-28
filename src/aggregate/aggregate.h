#ifndef RS_AGGREGATE_H__
#define RS_AGGREGATE_H__
#include <result_processor.h>
#include <query.h>
#include "reducer.h"
#include <value.h>

#ifndef GROUPBY_C_
typedef struct Grouper Grouper;
#endif

void Aggregate_BuildSchema();

CmdArg *Aggregate_ParseRequest(RedisModuleString **argv, int argc, char **err);
Grouper *NewGrouper(RSMultiKey *keys, RSSortingTable *tbl);
ResultProcessor *NewGrouperProcessor(Grouper *g, ResultProcessor *upstream);
void Grouper_AddReducer(Grouper *g, Reducer *r);
ResultProcessor *Query_BuildAggregationChain(QueryPlan *q, RedisSearchCtx *sctx, CmdArg *cmd,
                                             char **err);

ResultProcessor *GetProjector(ResultProcessor *upstream, const char *name, const char *alias,
                              CmdArg *args, char **err);
#endif