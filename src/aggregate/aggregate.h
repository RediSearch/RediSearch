#ifndef RS_AGGREGATE_H__
#define RS_AGGREGATE_H__
#include <result_processor.h>
#include <query.h>
#include "reducer.h"

typedef struct Groper Grouper;

void Aggregate_BuildSchema();

CmdArg *Aggregate_ParseRequest(RedisModuleString **argv, int argc, char **err);
Grouper *NewGrouper(const char *property, const char *alias, RSSortingTable *tbl);
ResultProcessor *NewGrouperProcessor(Grouper *g, ResultProcessor *upstream);
void Grouper_AddReducer(Grouper *g, Reducer *r);
ResultProcessor *Query_BuildAggregationChain(QueryPlan *q, RSSearchRequest *req, CmdArg *cmd);
#endif