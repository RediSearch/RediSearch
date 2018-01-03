#include "aggregate.h"
#include "reducer.h"
#include <query.h>
#include <result_processor.h>
#include <rmutil/cmdparse.h>
#include <search_request.h>

static CmdSchemaNode *requestSchema = NULL;

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
      [PROJECT {function} {nargs} {args} [AS {alias}]]
      [LIMIT {count} {offset}]
      ...
      */
  requestSchema = NewSchema("FT.AGGREGATE", NULL);
  CmdSchema_AddPostional(requestSchema, "idx", CmdSchema_NewArgAnnotated('s', "index_name"),
                         CmdSchema_Required);

  CmdSchema_AddPostional(requestSchema, "query", CmdSchema_NewArgAnnotated('s', "query_string"),
                         CmdSchema_Required);

  CmdSchema_AddNamed(requestSchema, "SELECT",
                     CmdSchema_Validate(CmdSchema_NewVector('s'), validatePropertyVector, NULL),
                     CmdSchema_Required);
  CmdSchemaNode *grp = CmdSchema_AddSubSchema(requestSchema, "GROUPBY",
                                              CmdSchema_Required | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(grp, "by",
                         CmdSchema_Validate(CmdSchema_NewVector('s'), validatePropertyVector, NULL),
                         CmdSchema_Required);

  CmdSchemaNode *red =
      CmdSchema_AddSubSchema(grp, "REDUCE", CmdSchema_Required | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(red, "FUNC", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddPostional(red, "ARGS", CmdSchema_NewVector('s'), CmdSchema_Required);
  CmdSchema_AddNamed(red, "AS", CmdSchema_NewArgAnnotated('s', "name"), CmdSchema_Optional);

  CmdSchema_AddNamed(requestSchema, "SORTBY",
                     CmdSchema_Validate(CmdSchema_NewVector('s'), validatePropertyVector, NULL),
                     CmdSchema_Optional | CmdSchema_Repeating);

  CmdSchemaNode *prj = CmdSchema_AddSubSchema(requestSchema, "APPLY",
                                              CmdSchema_Optional | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(prj, "FUNC", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddPostional(prj, "ARGS", CmdSchema_NewVector('s'), CmdSchema_Required);
  CmdSchema_AddNamed(prj, "AS", CmdSchema_NewArgAnnotated('s', "name"), CmdSchema_Optional);

  CmdSchema_AddNamed(requestSchema, "LIMIT",
                     CmdSchema_NewTuple("ll", (const char *[]){"offset", "num"}),
                     CmdSchema_Optional | CmdSchema_Repeating);

  CmdSchema_Print(requestSchema);
}

CmdArg *Aggregate_ParseRequest(RedisModuleString **argv, int argc, char **err) {

  CmdArg *ret = NULL;

  if (CMDPARSE_ERR != CmdParser_ParseRedisModuleCmd(requestSchema, &ret, argv, argc, err, 0)) {
    return ret;
  }
  return NULL;
}

int parseReducer(Grouper *g, CmdArg *red, char **err) {

  const char *func = CMDARG_STRPTR(CmdArg_FirstOf(red, "func"));
  CmdArg *args = CmdArg_FirstOf(red, "args");
  const char *alias = CMDARG_ORNULL(CmdArg_FirstOf(red, "AS"), CMDARG_STRPTR);

  Reducer *r = GetReducer(func, alias, &CMDARG_ARR(args), err);
  if (!r) {
    return 0;
  }
  Grouper_AddReducer(g, r);

  return 1;
}

ResultProcessor *buildGroupBy(CmdArg *grp, RedisSearchCtx *sctx, ResultProcessor *upstream,
                              char **err) {

  CmdArg *by = CmdArg_FirstOf(grp, "by");
  if (!by || CMDARG_ARRLEN(by) == 0) return NULL;

  RSMultiKey *keys = RS_NewMultiKeyFromArgs(&CMDARG_ARR(by));
  Grouper *g = NewGrouper(keys, sctx && sctx->spec ? sctx->spec->sortables : NULL);

  // Add reducerss
  CMD_FOREACH_SELECT(grp, "REDUCE", {
    if (!parseReducer(g, result, err)) goto fail;
  });

  return NewGrouperProcessor(g, upstream);

fail:
  RedisModule_Log(sctx->redisCtx, "warning", "Error paring GROUPBY: %s", *err);
  // TODO: Grouper_Free(g);
  return NULL;
}

ResultProcessor *buildSortBY(CmdArg *arg, ResultProcessor *upstream, char **err) {
  assert(arg && CMDARG_TYPE(arg) == CmdArg_Array);
  if (CMDARG_ARRLEN(arg) == 0) {
    asprintf(err, "Missing parameters for SORTBY");
    return NULL;
  }

  RSMultiKey *mk = RS_NewMultiKey(CMDARG_ARRLEN(arg));
  for (size_t i = 0; i < mk->len; i++) {
    mk->keys[i] = CMDARG_STRPTR(CMDARG_ARRELEM(arg, i));
  }

  return NewSorterByFields(mk, 1, 0, upstream);
}

ResultProcessor *buildProjection(CmdArg *arg, ResultProcessor *upstream, char **err) {
  CmdArg *func = CmdArg_FirstOf(arg, "func");
  if (!func || CMDARG_TYPE(func) != CmdArg_String) {
    asprintf(err, "Missing or invalid projection function");
    return NULL;
  }

  const char *fname = CMDARG_STRPTR(func);
  CmdArg *args = CmdArg_FirstOf(arg, "args");
  const char *alias = CMDARG_ORNULL(CmdArg_FirstOf(arg, "AS"), CMDARG_STRPTR);

  return GetProjector(upstream, fname, alias, args, err);
}

ResultProcessor *addLimit(CmdArg *arg, ResultProcessor *upstream, char **err) {
  long long offset, limit;
  offset = CMDARG_INT(CMDARG_ARRELEM(arg, 0));
  limit = CMDARG_INT(CMDARG_ARRELEM(arg, 1));

  if (offset < 0 || limit <= 0) {
    *err = strdup("Invalid offset/num for LIMIT");
    return NULL;
  }
  return NewPager(upstream, (uint32_t)offset, (uint32_t)limit);
}

FieldList *getAggregateFields(RedisModuleCtx *ctx, CmdArg *cmd) {
  FieldList *ret = NULL;
  CmdArg *select = CmdArg_FirstOf(cmd, "SELECT");
  if (select) {
    ret = calloc(1, sizeof(*ret));
    ret->explicitReturn = 1;
    CmdArgIterator it = CmdArg_Children(select);
    CmdArg *child;
    while (NULL != (child = CmdArgIterator_Next(&it, NULL))) {
      const char *k = CMDARG_STRPTR(child);
      size_t len = CMDARG_STRLEN(child);
      if (len > 0 && *k == '@') {
        k++;
        len--;
      }
      ReturnedField *rf = FieldList_GetCreateField(ret, RedisModule_CreateString(ctx, k, len));

      rf->explicitReturn = 1;
    }
  }
  return ret;
}

ResultProcessor *Aggregate_BuildProcessorChain(QueryPlan *plan, void *ctx) {

  CmdArg *cmd = ctx;
  // The base processor translates index results into search results
  ResultProcessor *next = NewBaseProcessor(plan, &plan->execCtx);
  ResultProcessor *prev = NULL;
  FieldList *lst = getAggregateFields(plan->ctx->redisCtx, cmd);
  next = NewLoader(next, plan->ctx, lst);

  CmdArgIterator it = CmdArg_Children(cmd);
  CmdArg *child;
  const char *key;
  char *err = NULL;
  while (NULL != (child = CmdArgIterator_Next(&it, &key))) {
    prev = next;
    if (!strcasecmp(key, "GROUPBY")) {
      next = buildGroupBy(child, plan->ctx, next, &err);
    } else if (!strcasecmp(key, "SORTBY")) {
      next = buildSortBY(child, next, &err);
    } else if (!strcasecmp(key, "APPLY")) {
      next = buildProjection(child, next, &err);
    } else if (!strcasecmp(key, "LIMIT")) {
      next = addLimit(child, next, &err);
    }
    if (!next) {
      goto fail;
    }
  }

  return next;

fail:
  if (prev) {
    ResultProcessor_Free(prev);
  }
  // if (!*err) {
  //   *err = (char *)strdup("Could not parse aggregate request");
  // }
  return NULL;
}

int Aggregate_ProcessRequest(RedisSearchCtx *sctx, RedisModuleString **argv, int argc) {
  char *err = NULL;
  CmdArg *cmd = Aggregate_ParseRequest(argv, argc, &err);
  if (!cmd) {
    return RedisModule_ReplyWithError(sctx->redisCtx,
                                      err ? err : "Could not parse aggregate request");
  }

  RedisModuleCtx *ctx = sctx->redisCtx;

  CmdString *str = &CMDARG_STR(CmdArg_FirstOf(cmd, "query"));

  RSSearchOptions opts = RS_DEFAULT_SEARCHOPTS;
  // mark the query as an aggregation query
  opts.flags |= Search_AggregationQuery;

  QueryParseCtx *q = NewQueryParseCtx(sctx, str->str, str->len, &opts);

  if (!Query_Parse(q, &err)) {
    Query_Free(q);
    RedisModule_ReplyWithError(ctx, err ? err : "Unkonown Error");
    return REDISMODULE_ERR;
  }
  Query_Expand(q, opts.expander);

  QueryPlan *plan = Query_BuildPlan(sctx, q, &opts, Aggregate_BuildProcessorChain, cmd);
  if (!plan || err != NULL) {
    RedisModule_ReplyWithError(ctx, err ? err : QUERY_ERROR_INTERNAL_STR);
    return REDISMODULE_ERR;
  }
  // Execute the query
  int rc = QueryPlan_Run(plan, &err);
  if (rc == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, QUERY_ERROR_INTERNAL_STR);
  }
  QueryPlan_Free(plan);
  Query_Free(q);
  SearchCtx_Free(sctx);
  return rc;
}
