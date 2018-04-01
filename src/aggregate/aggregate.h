#ifndef RS_AGGREGATE_H__
#define RS_AGGREGATE_H__
#include <result_processor.h>
#include <query.h>
#include "reducer.h"
#include "expr/expression.h"
#include "query_plan.h"
#include <value.h>

#ifndef GROUPBY_C_
typedef struct Grouper Grouper;
#endif

CmdSchemaNode *GetAggregateRequestSchema();

struct AggregatePlan;

typedef struct {
  RSMultiKey *keys;
} AggregateLoadStep;

typedef struct {
  const char *reducer;
  RSValue *args;
  const char *alias;
} AggregateGroupReduce;

typedef struct {
  RSMultiKey *properties;
  AggregateGroupReduce *reducers;
  size_t numReducers;
  const char *alias;
} AggregateGroupStep;

typedef struct {
  const char *rawExpr;
  RSExpr *parsedExpr;
  const char *alias;
} AggregateApplyStep;

typedef enum {
  Property_Field = 1,
  Property_Aggregate = 2,
  Property_Projection = 3,
} AggregatePropertyKind;

typedef struct {
  const char *property;
  RSValueType type;
  AggregatePropertyKind kind;
} AggregateProperty;

typedef AggregateProperty *AggregateSchema;

typedef struct {
  RSMultiKey *keys;
  uint64_t ascMap;
  long long max;
} AggregateSortStep;

typedef struct {
  long long offset;
  long long num;
} AggregateLimitStep;

typedef enum {
  AggregateStep_Group,
  AggregateStep_Sort,
  AggregateStep_Apply,
  AggregateStep_Limit,
  AggregateStep_Load,
  AggregateStep_Distribute,
} AggregateStepType;

typedef struct {
  struct AggregatePlan *plan;
} AggregateDistributeStep;

typedef struct AggregateStep {
  union {
    AggregateApplyStep apply;
    AggregateGroupStep group;
    AggregateLoadStep load;
    AggregateLimitStep limit;
    AggregateSortStep sort;
    AggregateDistributeStep dist;
  };
  AggregateStepType type;
  struct AggregateStep *next;
} AggregateStep;

typedef struct {
  size_t count;
  int maxIdle;
} AggregateCursor;

typedef struct AggregatePlan {
  const char *index;
  const char *query;
  size_t queryLen;
  AggregateStep *head;
  AggregateStep *tail;
  size_t size;
  int hasCursor;
  AggregateCursor cursor;
} AggregatePlan;

char **AggregatePlan_Serialize(AggregatePlan *plan);
int AggregatePlan_Build(AggregatePlan *plan, CmdArg *cmd, char **err);
AggregateSchema AggregatePlan_GetSchema(AggregatePlan *plan, RSSortingTable *tbl);
int AggregatePlan_MakeDistributed(AggregatePlan *src, AggregatePlan *dist);
typedef struct {
  QueryPlan *plan;
  QueryParseCtx *parseCtx;
  CmdArg *args;

  /**
   * If this pointer is heap allocated, in which case the pointer itself is
   * freed during AR_Free()
   */
  int isHeapAlloc;
} AggregateRequest;

void Aggregate_BuildSchema();

/**
 * Note that this does not initialize the structure; use
 */
int AggregateRequest_Start(AggregateRequest *req, RedisSearchCtx *sctx, RedisModuleString **argv,
                           int argc, const char **err);
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
                              CmdArg *args, char **err);
#endif