#include "aggregate.h"
#include "reducer.h"
#include "project.h"

#include <query.h>
#include <result_processor.h>
#include <rmutil/cmdparse.h>
#include <util/arr.h>
#include <search_request.h>
#include "functions/function.h"

static CmdSchemaNode *requestSchema = NULL;

CmdSchemaNode *GetAggregateRequestSchema() {
  return requestSchema;
}
// validator for property names
int validatePropertyName(CmdArg *arg, void *p) {
  return (CMDARG_TYPE(arg) == CmdArg_String && CMDARG_STRLEN(arg) > 1 &&
          CMDARG_STRPTR(arg)[0] == '@');
}

int validatePropertyVector(CmdArg *arg, void *p) {
  if (CMDARG_TYPE(arg) != CmdArg_Array || CMDARG_ARRLEN(arg) == 0) {
    return 0;
  }
  for (size_t i = 0; i < CMDARG_ARRLEN(arg); i++) {
    if (!validatePropertyName(CMDARG_ARRELEM(arg, i), NULL)) {
      return 0;
    }
  }
  return 1;
}

void Aggregate_BuildSchema() {
  if (requestSchema) return;
  /*
  FT.AGGREGATE {index}
      FILTER {query}
      SELECT {nargs} {@field} ...
      [
        GROUPBY {nargs} {property} ...
        GROUPREDUCE {function} {nargs} {arg} ... [AS {alias}]
        ...
      ]
      [SORTBY {nargs} {property} ... ]
      [APPLY {expression} [AS {alias}]]
      [LIMIT {count} {offset}]
      ...
      */

  // Initialize projection functions registry
  RegisterMathFunctions();
  RegisterStringFunctions();
  RegisterDateFunctions();

  requestSchema = NewSchema("FT.AGGREGATE", NULL);
  CmdSchema_AddPostional(requestSchema, "idx", CmdSchema_NewArgAnnotated('s', "index_name"),
                         CmdSchema_Required);

  CmdSchema_AddPostional(requestSchema, "query", CmdSchema_NewArgAnnotated('s', "query_string"),
                         CmdSchema_Required);

  CmdSchema_AddNamedWithHelp(
      requestSchema, "LOAD",
      CmdSchema_Validate(CmdSchema_NewVector('s'), validatePropertyVector, NULL),
      CmdSchema_Optional,
      "Optionally load non-sortable properties from the HASH object. Do not use unless as last "
      "resort, this hurts performance badly.");

  CmdSchemaNode *grp = CmdSchema_AddSubSchema(requestSchema, "GROUPBY",
                                              CmdSchema_Optional | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(grp, "BY",
                         CmdSchema_Validate(CmdSchema_NewVector('s'), validatePropertyVector, NULL),
                         CmdSchema_Required);

  CmdSchemaNode *red =
      CmdSchema_AddSubSchema(grp, "REDUCE", CmdSchema_Optional | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(red, "FUNC", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddPostional(red, "ARGS", CmdSchema_NewVector('s'), CmdSchema_Required);
  CmdSchema_AddNamed(red, "AS", CmdSchema_NewArgAnnotated('s', "name"), CmdSchema_Optional);

  CmdSchemaNode *sort = CmdSchema_AddSubSchema(requestSchema, "SORTBY",
                                               CmdSchema_Optional | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(sort, "by", CmdSchema_NewVector('s'), CmdSchema_Required);
  // SORT can have its own MAX limitation that speeds up things
  CmdSchema_AddNamed(sort, "MAX", CmdSchema_NewArgAnnotated('l', "num"),
                     CmdSchema_Optional | CmdSchema_Repeating);

  CmdSchemaNode *prj = CmdSchema_AddSubSchema(requestSchema, "APPLY",
                                              CmdSchema_Optional | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(prj, "EXPR", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddNamed(prj, "AS", CmdSchema_NewArgAnnotated('s', "name"), CmdSchema_Required);

  CmdSchema_AddNamed(requestSchema, "LIMIT",
                     CmdSchema_NewTuple("ll", (const char *[]){"offset", "num"}),
                     CmdSchema_Optional | CmdSchema_Repeating);

  CmdSchemaNode *cursorSchema =
      CmdSchema_AddSubSchema(requestSchema, "WITHCURSOR", CmdSchema_Optional, "Use cursor");

  CmdSchema_AddNamed(cursorSchema, "COUNT", CmdSchema_NewArgAnnotated('l', "row_count"),
                     CmdSchema_Optional);

  CmdSchema_AddNamed(cursorSchema, "MAXIDLE", CmdSchema_NewArgAnnotated('l', "idle_timeout"),
                     CmdSchema_Optional);

  CmdSchema_Print(requestSchema);
}

CmdArg *Aggregate_ParseRequest(RedisModuleString **argv, int argc, char **err) {

  CmdArg *ret = NULL;

  if (CMDPARSE_ERR != CmdParser_ParseRedisModuleCmd(requestSchema, &ret, argv, argc, err, 0)) {
    return ret;
  }
  return NULL;
}

ResultProcessor *buildGroupBy(AggregateGroupStep *grp, RedisSearchCtx *sctx,
                              ResultProcessor *upstream, char **err) {

  Grouper *g = NewGrouper(RSMultiKey_Copy(grp->properties, 0),
                          sctx && sctx->spec ? sctx->spec->sortables : NULL);

  array_foreach(grp->reducers, red, {
    Reducer *r = GetReducer(sctx, red.reducer, red.alias, red.args, array_len(red.args), err);
    if (!r) {
      goto fail;
    }
    Grouper_AddReducer(g, r);
  });

  return NewGrouperProcessor(g, upstream);

fail:
  RedisModule_Log(sctx->redisCtx, "warning", "Error parsing GROUPBY: %s", *err);

  Grouper_Free(g);
  return NULL;
}

ResultProcessor *buildSortBY(AggregateSortStep *srt, ResultProcessor *upstream, char **err) {
  return NewSorterByFields(RSMultiKey_Copy(srt->keys, 0), srt->ascMap, srt->max, upstream);
}

ResultProcessor *buildProjection(AggregateApplyStep *a, ResultProcessor *upstream,
                                 RedisSearchCtx *sctx, char **err) {
  return NewProjector(sctx, upstream, a->alias, a->rawExpr, strlen(a->rawExpr), err);
}

ResultProcessor *addLimit(AggregateLimitStep *l, ResultProcessor *upstream, char **err) {

  if (l->offset < 0 || l->num <= 0) {
    *err = strdup("Invalid offset/num for LIMIT");
    return NULL;
  }
  return NewPager(upstream, (uint32_t)l->offset, (uint32_t)l->num);
}

int getAggregateFields(FieldList *l, RedisModuleCtx *ctx, CmdArg *cmd) {
  *l = (FieldList){};
  CmdArg *select = CmdArg_FirstOf(cmd, "LOAD");
  if (select) {
    l->explicitReturn = 1;
    CmdArgIterator it = CmdArg_Children(select);
    CmdArg *child;
    while (NULL != (child = CmdArgIterator_Next(&it, NULL))) {
      const char *k = CMDARG_STRPTR(child);
      size_t len = CMDARG_STRLEN(child);
      if (len > 0 && *k == '@') {
        k++;
        len--;
      }
      ReturnedField *rf = FieldList_GetCreateField(l, RedisModule_CreateString(ctx, k, len));

      rf->explicitReturn = 1;
    }
  }
  return l->numFields;
}

ResultProcessor *AggregatePlan_BuildProcessorChain(AggregatePlan *plan, RedisSearchCtx *sctx,
                                                   ResultProcessor *root, char **err) {
  ResultProcessor *prev = NULL;
  ResultProcessor *next = root;
  // Load LOAD based stuff from hash vals

  // if (getAggregateFields(&plan->opts.fields, plan->ctx->redisCtx, cmd)) {
  //   next = NewLoader(next, plan->ctx, &plan->opts.fields);
  // }

  // Walk the children and evaluate them
  AggregateStep *current = plan->head;
  const char *key;
  while (current) {
    prev = next;
    switch (current->type) {

      case AggregateStep_Group:

        next = buildGroupBy(&current->group, sctx, next, err);
        break;
      case AggregateStep_Sort:
        next = buildSortBY(&current->sort, next, err);
        break;
      case AggregateStep_Apply:
        next = buildProjection(&current->apply, next, sctx, err);
        break;
      case AggregateStep_Limit:

        next = addLimit(&current->limit, next, err);
        break;
      case AggregateStep_Load:
        fprintf(stderr, "PLEASE IMPLEMENT LOAD...\n");
        // next = load
        break;
      case AggregateStep_Distribute:
      case AggregateStep_Dummy:
      case AggregateStep_Query:

        break;
    }
    current = current->next;

    if (!next) {
      goto fail;
    }
  }

  return next;

fail:
  if (prev) {
    ResultProcessor_Free(prev);
  }

  RedisModule_Log(sctx->redisCtx, "warning", "Could not parse aggregate request: %s", *err);
  return NULL;
}

static ResultProcessor *Aggregate_BuildProcessorChain(QueryPlan *plan, void *ctx, char **err) {

  AggregatePlan *ap = ctx;
  // The base processor translates index results into search results
  ResultProcessor *root = NewBaseProcessor(plan, &plan->execCtx);

  if (!root) return NULL;

  return AggregatePlan_BuildProcessorChain(ap, plan->ctx, root, err);
}

int AggregateRequest_Start(AggregateRequest *req, RedisSearchCtx *sctx, RedisModuleString **argv,
                           int argc, const char **err) {
#define MAYBE_SET_ERR(s) \
  if (!*err) {           \
    *err = s;            \
  }

  req->args = Aggregate_ParseRequest(argv, argc, (char **)err);
  if (!req->args) {
    MAYBE_SET_ERR("Could not parse aggregate request");
    return REDISMODULE_ERR;
  }

  req->ap = (AggregatePlan){};
  if (!AggregatePlan_Build(&req->ap, req->args, (char **)err)) {
    printf("err:%s\n", *err);
    MAYBE_SET_ERR("Could not build aggregate plan");
    return REDISMODULE_ERR;
  }

  RedisModuleCtx *ctx = sctx->redisCtx;

  CmdString *str = &CMDARG_STR(CmdArg_FirstOf(req->args, "query"));

  RSSearchOptions opts = RS_DEFAULT_SEARCHOPTS;
  // mark the query as an aggregation query
  opts.flags |= Search_AggregationQuery;

  req->parseCtx = NewQueryParseCtx(sctx, str->str, str->len, &opts);

  if (!Query_Parse(req->parseCtx, (char **)err)) {
    MAYBE_SET_ERR("Unknown error");
    return REDISMODULE_ERR;
  }

  Query_Expand(req->parseCtx, opts.expander);
  req->plan = Query_BuildPlan(sctx, req->parseCtx, &opts, Aggregate_BuildProcessorChain, &req->ap,
                              (char **)&err);
  if (!req->plan) {
    MAYBE_SET_ERR(QUERY_ERROR_INTERNAL_STR);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

void AggregateRequest_Run(AggregateRequest *req, RedisModuleCtx *outCtx) {
  QueryPlan_Run(req->plan, outCtx);
}

void AggregateRequest_Free(AggregateRequest *req) {
  if (req->plan) {
    if (req->plan->opts.fields.numFields) {
      FieldList_Free(&req->plan->opts.fields);
    }
    QueryPlan_Free(req->plan);
  }
  if (req->parseCtx) {
    Query_Free(req->parseCtx);
  }
  AggregatePlan_Free(&req->ap);
  if (req->args) {
    CmdArg_Free(req->args);
  }

  if (req->isHeapAlloc) {
    rm_free(req);
  }
}

AggregateRequest *AggregateRequest_Persist(AggregateRequest *req) {
  AggregateRequest *ret = rm_malloc(sizeof(*ret));
  *ret = *req;
  ret->isHeapAlloc = 1;
  return ret;
}