#include "aggregate.h"
#include "reducer.h"
#include "project.h"

#include <query.h>
#include <result_processor.h>
#include <rmutil/cmdparse.h>
#include <util/arr.h>
#include <search_request.h>
#include "functions/function.h"
#include <err.h>

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
  CmdSchema_AddFlag(requestSchema, "WITHSCHEMA");
  CmdSchema_AddFlag(requestSchema, "VERBATIM");

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

  // FILTER expr -> return only results matching post filter
  CmdSchema_AddNamed(requestSchema, "FILTER", CmdSchema_NewArg('s'),
                     CmdSchema_Optional | CmdSchema_Repeating);

  CmdSchemaNode *cursorSchema =
      CmdSchema_AddSubSchema(requestSchema, "WITHCURSOR", CmdSchema_Optional, "Use cursor");

  CmdSchema_AddNamed(cursorSchema, "COUNT", CmdSchema_NewArgAnnotated('l', "row_count"),
                     CmdSchema_Optional);

  CmdSchema_AddNamed(cursorSchema, "MAXIDLE", CmdSchema_NewArgAnnotated('l', "idle_timeout"),
                     CmdSchema_Optional);
}

CmdArg *Aggregate_ParseRequest(RedisModuleString **argv, int argc, QueryError *status) {

  CmdArg *ret = NULL;

  if (CMDPARSE_ERR !=
      CmdParser_ParseRedisModuleCmd(requestSchema, &ret, argv, argc, &status->detail, 0)) {
    QueryError_MaybeSetCode(status, QUERY_EKEYWORD);
    return ret;
  }
  return NULL;
}

ResultProcessor *buildGroupBy(AggregateGroupStep *grp, RedisSearchCtx *sctx,
                              ResultProcessor *upstream, QueryError *status) {

  Grouper *g = NewGrouper(RSMultiKey_Copy(grp->properties, 0),
                          sctx && sctx->spec ? sctx->spec->sortables : NULL);

  array_foreach(grp->reducers, red, {
    Reducer *r = GetReducer(sctx, red.reducer, red.alias, red.args, array_len(red.args), status);
    if (!r) {
      goto fail;
    }
    Grouper_AddReducer(g, r);
  });

  return NewGrouperProcessor(g, upstream);

fail:
  if (sctx && sctx->redisCtx)
    RedisModule_Log(sctx->redisCtx, "warning", "Error parsing GROUPBY: %s",
                    QueryError_GetError(status));

  Grouper_Free(g);
  return NULL;
}

ResultProcessor *buildSortBY(AggregateSortStep *srt, ResultProcessor *upstream,
                             QueryError *status) {
  return NewSorterByFields(RSMultiKey_Copy(srt->keys, 0), srt->ascMap, srt->max, upstream);
}

ResultProcessor *buildProjection(AggregateApplyStep *a, ResultProcessor *upstream,
                                 RedisSearchCtx *sctx, QueryError *status) {
  return NewProjector(sctx, upstream, a->alias, a->rawExpr, strlen(a->rawExpr), status);
}

ResultProcessor *buildFilter(AggregateFilterStep *f, ResultProcessor *upstream,
                             RedisSearchCtx *sctx, QueryError *status) {
  return NewFilter(sctx, upstream, f->rawExpr, strlen(f->rawExpr), status);
}
ResultProcessor *addLimit(AggregateLimitStep *l, ResultProcessor *upstream, QueryError *status) {

  if (l->offset < 0 || l->num <= 0) {
    QueryError_SetError(status, QUERY_EKEYWORD, "Invalid offset/num for LIMIT");
  }
  return NewPager(upstream, (uint32_t)l->offset, (uint32_t)l->num);
}

ResultProcessor *buildLoader(ResultProcessor *upstream, RedisSearchCtx *ctx,
                             AggregateLoadStep *ls) {
  ls->fl = (FieldList){.explicitReturn = 1};
  for (int i = 0; i < ls->keys->len; i++) {
    const char *k = RSKEY(ls->keys->keys[i].key);
    ReturnedField *rf =
        FieldList_GetCreateField(&ls->fl, RedisModule_CreateString(ctx->redisCtx, k, strlen(k)));

    rf->explicitReturn = 1;
  }

  return NewLoader(upstream, ctx, &ls->fl);
}

ResultProcessor *AggregatePlan_BuildProcessorChain(AggregatePlan *plan, RedisSearchCtx *sctx,
                                                   ResultProcessor *root, QueryError *status) {
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

        next = buildGroupBy(&current->group, sctx, next, status);
        break;
      case AggregateStep_Sort:
        next = buildSortBY(&current->sort, next, status);
        break;
      case AggregateStep_Apply:
        next = buildProjection(&current->apply, next, sctx, status);
        break;
      case AggregateStep_Limit:

        next = addLimit(&current->limit, next, status);
        break;

      case AggregateStep_Filter:
        next = buildFilter(&current->filter, next, sctx, status);
        break;
      case AggregateStep_Load:
        if (current->load.keys->len > 0 && sctx != NULL) {
          next = buildLoader(next, sctx, &current->load);
        }
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
  if (sctx && sctx->redisCtx)
    RedisModule_Log(sctx->redisCtx, "warning", "Could not parse aggregate request: %s",
                    QueryError_GetError(status));
  return NULL;
}

ResultProcessor *Aggregate_DefaultChainBuilder(QueryPlan *plan, void *ctx, QueryError *status) {

  AggregatePlan *ap = ctx;
  // The base processor translates index results into search results
  ResultProcessor *root = NewBaseProcessor(plan, &plan->execCtx);

  if (!root) return NULL;

  return AggregatePlan_BuildProcessorChain(ap, plan->ctx, root, status);
}

int AggregateRequest_Start(AggregateRequest *req, RedisSearchCtx *sctx,
                           const AggregateRequestSettings *settings, RedisModuleString **argv,
                           int argc, QueryError *status) {

  req->args = Aggregate_ParseRequest(argv, argc, status);
  if (!req->args) {
    QueryError_MaybeSetCode(status, QUERY_EPARSEARGS);
    return REDISMODULE_ERR;
  }

  req->ap = (AggregatePlan){};
  if (!AggregatePlan_Build(&req->ap, req->args, &status->detail)) {
    QueryError_MaybeSetCode(status, QUERY_EAGGPLAN);
    return REDISMODULE_ERR;
  }

  RedisModuleCtx *ctx = sctx->redisCtx;

  CmdString *str = &CMDARG_STR(CmdArg_FirstOf(req->args, "query"));

  RSSearchOptions opts = RS_DEFAULT_SEARCHOPTS;
  // mark the query as an aggregation query
  opts.flags |= Search_AggregationQuery;
  // pass VERBATIM to the aggregate query
  if (req->ap.verbatim) {
    opts.flags |= Search_Verbatim;
  }
  if (settings->flags & AGGREGATE_REQUEST_NO_CONCURRENT) {
    opts.concurrentMode = 0;
  }
  if (settings->flags & AGGREGATE_REQUEST_NO_PARSE_QUERY) {
    req->parseCtx = NULL;
  } else {
    req->parseCtx = NewQueryParseCtx(sctx, str->str, str->len, &opts);

    if (!Query_Parse(req->parseCtx, (char **)status->detail)) {
      QueryError_MaybeSetCode(status, QUERY_ESYNTAX);
      return REDISMODULE_ERR;
    }

    if (!req->ap.verbatim) {
      Query_Expand(req->parseCtx, opts.expander);
    }
  }
  req->plan = Query_BuildPlan(sctx, req->parseCtx, &opts, settings->pcb, &req->ap, status);
  if (!req->plan) {
    QueryError_MaybeSetCode(status, QUERY_EBUILDPLAN);
    return REDISMODULE_ERR;
  }

  if (req->ap.withSchema) {
    AggregateSchema sc = AggregatePlan_GetSchema(&req->ap, SEARCH_CTX_SORTABLES(req->plan->ctx));
    QueryPlan_SetHook(req->plan, QueryPlanHook_Pre, AggregatePlan_DumpSchema, sc, array_free);
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