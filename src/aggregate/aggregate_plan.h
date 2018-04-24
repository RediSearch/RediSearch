#ifndef AGGREGATE_PLAN_H_
#define AGGREGATE_PLAN_H_
#include <value.h>
#include <search_options.h>
#include <aggregate/expr/expression.h>

/* A structure representing an aggregation execution plan and all its various steps.
 * This is used to safely manipulate and validate plans */
struct AggregatePlan;

/* Load step - load properties */
typedef struct {
  RSMultiKey *keys;
  FieldList fl;
} AggregateLoadStep;

/* query step - search for query text */
typedef struct {
  char *str;

} AggregateQueryStep;

/* Group step single reducer, a function and its args */
typedef struct {
  const char *reducer;
  RSValue **args;
  char *alias;
} AggregateGroupReduce;

/* Group step - group by properties and reduce by several reducers */
typedef struct {
  RSMultiKey *properties;
  AggregateGroupReduce *reducers;
  int idx;
} AggregateGroupStep;

/* Apply step - evaluate an expression per record */
typedef struct {
  char *rawExpr;
  RSExpr *parsedExpr;
  char *alias;
} AggregateApplyStep;

/* Schema property kind (not type!) is this a field from the result, a projection or an aggregation?
 */
typedef enum {
  Property_Field = 1,
  Property_Aggregate = 2,
  Property_Projection = 3,
} AggregatePropertyKind;

/* Aggregation property for the schema */
typedef struct {
  const char *property;
  RSValueType type;
  AggregatePropertyKind kind;
} AggregateProperty;

/* A schema is just an array of properties */
typedef AggregateProperty *AggregateSchema;

/* Sortby step - by one or more properties */
typedef struct {
  RSMultiKey *keys;
  uint64_t ascMap;
  long long max;
} AggregateSortStep;

/* limit paging */
typedef struct {
  long long offset;
  long long num;
} AggregateLimitStep;

typedef enum {
  AggregateStep_Query,
  AggregateStep_Group,
  AggregateStep_Sort,
  AggregateStep_Apply,
  AggregateStep_Limit,
  AggregateStep_Load,
  AggregateStep_Distribute,
  AggregateStep_Dummy,  // dummy step representing an empty plan's head
} AggregateStepType;

/* Distribute step - send a sub-plan to all shards and collect the results */
typedef struct {
  struct AggregatePlan *plan;
} AggregateDistributeStep;

/* unifying all steps */
typedef struct AggregateStep {
  union {
    AggregateApplyStep apply;
    AggregateGroupStep group;
    AggregateLoadStep load;
    AggregateLimitStep limit;
    AggregateSortStep sort;
    AggregateDistributeStep dist;
    AggregateQueryStep query;
  };
  AggregateStepType type;
  struct AggregateStep *next;
  struct AggregateStep *prev;

} AggregateStep;

/* A plan is a linked list of all steps */
typedef struct AggregatePlan {
  const char *index;
  AggregateStep *head;
  AggregateStep *tail;
  int hasCursor;
  int withSchema;
  int verbatim;
  // cursor configuraion
  struct {
    size_t count;
    int maxIdle;
  } cursor;
} AggregatePlan;

/* Serialize the plan into an array of string args, to create a command to be sent over the network.
 * The strings need to be freed with free and the array needs to be freed with array_free(). The
 * length can be extracted with array_len */
char **AggregatePlan_Serialize(AggregatePlan *plan);

/* Build the plan from the parsed command args. Sets the error and return 0 if there's a failure */
int AggregatePlan_Build(AggregatePlan *plan, CmdArg *cmd, char **err);

/* Get the estimated schema from the plan, with best effort to guess the types of values based on
 * function types. The schema can be freed with array_free */
AggregateSchema AggregatePlan_GetSchema(AggregatePlan *plan, RSSortingTable *tbl);

AggregateProperty *AggregateSchema_Get(AggregateSchema sc, const char *prop);

/* return 1 if a schema contains a property */
int AggregateSchema_Contains(AggregateSchema schema, const char *property);

/* Free the plan resources, not the plan itself */
void AggregatePlan_Free(AggregatePlan *plan);

/* Print the plan */
void AggregatePlan_Print(AggregatePlan *plan);

/* Callback to dump the schema to redis */
int AggregatePlan_DumpSchema(RedisModuleCtx *sctx, QueryProcessingCtx *qpc, void *privdata);
#endif