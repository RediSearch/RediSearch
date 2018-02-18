#include "aggregate.h"
#include "reducer.h"
#include "project.h"

#include <query.h>
#include <result_processor.h>
#include <rmutil/cmdparse.h>
#include <search_request.h>
#include "functions/function.h"

static CmdSchemaNode *requestSchema = NULL;
static RSFunctionRegistry functions_g = {0};

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
  RegisterMathFunctions(&functions_g);
  RegisterStringFunctions(&functions_g);
  RegisterDateFunctions(&functions_g);

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
      CmdSchema_AddSubSchema(grp, "REDUCE", CmdSchema_Required | CmdSchema_Repeating, NULL);
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

  CmdSchema_Print(requestSchema);
}

CmdArg *Aggregate_ParseRequest(RedisModuleString **argv, int argc, char **err) {

  CmdArg *ret = NULL;

  if (CMDPARSE_ERR != CmdParser_ParseRedisModuleCmd(requestSchema, &ret, argv, argc, err, 0)) {
    return ret;
  }
  return NULL;
}

int parseReducer(RedisSearchCtx *ctx, Grouper *g, CmdArg *red, char **err) {

  const char *func = CMDARG_STRPTR(CmdArg_FirstOf(red, "func"));
  CmdArg *args = CmdArg_FirstOf(red, "args");
  const char *alias = CMDARG_ORNULL(CmdArg_FirstOf(red, "AS"), CMDARG_STRPTR);

  Reducer *r = GetReducer(ctx, func, alias, &CMDARG_ARR(args), err);
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

  RSMultiKey *keys = RS_NewMultiKeyFromArgs(&CMDARG_ARR(by), 1);
  Grouper *g = NewGrouper(keys, sctx && sctx->spec ? sctx->spec->sortables : NULL);

  // Add reducerss
  CMD_FOREACH_SELECT(grp, "REDUCE", {
    if (!parseReducer(sctx, g, result, err)) goto fail;
  });

  return NewGrouperProcessor(g, upstream);

fail:
  RedisModule_Log(sctx->redisCtx, "warning", "Error paring GROUPBY: %s", *err);
  // TODO: Grouper_Free(g);
  return NULL;
}

ResultProcessor *buildSortBY(CmdArg *srt, ResultProcessor *upstream, char **err) {
  CmdArg *by = CmdArg_FirstOf(srt, "by");
  if (!by || CMDARG_ARRLEN(by) == 0) return NULL;

  RSMultiKey *keys = RS_NewMultiKey(CMDARG_ARRLEN(by));
  // We build a bitmap of maximum 64 sorting parameters. 1 means asc, 2 desc
  // By default all bits are 1. Whenever we encounter DESC we flip the corresponding bit
  uint64_t ascMap = 0xFFFFFFFFFFFFFFFF;
  int n = 0;

  // since ASC/DESC are optional, we need a stateful parser
  // state 0 means we are open only to property names
  // state 1 means we are open to either a new property or ASC/DESC
  int state = 0, i = 0;
  for (i = 0; i < CMDARG_ARRLEN(by) && i < sizeof(ascMap) * 8; i++) {
    const char *str = CMDARG_STRPTR(CMDARG_ARRELEM(by, i));
    // New properties are accepted in either state
    if (*str == '@') {
      keys->keys[n++] = RS_KEY(RSKEY(str));
      state = 1;
    } else if (state == 0) {
      // At state 0 we only accept properties, so we don't even need to see what this is
      goto err;
    } else if (!strcasecmp(str, "asc")) {
      // For as - we put a 1 in the map. We don't actually need to, this is just for readability
      ascMap |= 1 << (n - 1);
      // switch back to state 0, ASC/DESC cannot follow ASC
      state = 0;
    } else if (!strcasecmp(str, "desc")) {
      // We turn the current bit to 0 meaning desc for the Nth property
      ascMap &= ~(1 << (n - 1));
      // switch back to state 0, ASC/DESC cannot follow ASC
      state = 0;
    } else {
      // Unkown token - neither a property nor ASC/DESC
      goto err;
    }
  }

  // Parse optional MAX
  CmdArg *max = CmdArg_FirstOf(srt, "MAX");
  long long mx = 0;
  if (max) {
    mx = CMDARG_INT(max);
    if (mx < 0) mx = 0;
  }
  return NewSorterByFields(keys, ascMap, mx, upstream);
err:
  asprintf(err, "Invalid sortby arguments near '%s'", CMDARG_STRPTR(CMDARG_ARRELEM(by, i)));
  if (keys) RSMultiKey_Free(keys);
  return NULL;
}

ResultProcessor *buildProjection(CmdArg *arg, ResultProcessor *upstream, RedisSearchCtx *sctx,
                                 char **err) {
  CmdArg *expr = CmdArg_FirstOf(arg, "expr");
  if (!expr || CMDARG_TYPE(expr) != CmdArg_String) {
    asprintf(err, "Missing or invalid projection expression");
    return NULL;
  }

  const char *alias = CMDARG_ORNULL(CmdArg_FirstOf(arg, "AS"), CMDARG_STRPTR);

  return NewProjector(sctx, &functions_g, upstream, alias, CMDARG_STRPTR(expr), CMDARG_STRLEN(expr),
                      err);
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
  CmdArg *select = CmdArg_FirstOf(cmd, "LOAD");
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

static ResultProcessor *Aggregate_BuildProcessorChain(QueryPlan *plan, void *ctx, char **err) {

  CmdArg *cmd = ctx;
  // The base processor translates index results into search results
  ResultProcessor *next = NewBaseProcessor(plan, &plan->execCtx);
  ResultProcessor *prev = NULL;
  // Load LOAD based stuff from hash vals
  FieldList *lst = getAggregateFields(plan->ctx->redisCtx, cmd);
  if (lst != NULL) {
    next = NewLoader(next, plan->ctx, lst);
  }

  // Walk the children and evaluate them
  CmdArgIterator it = CmdArg_Children(cmd);
  CmdArg *child;
  const char *key;
  while (NULL != (child = CmdArgIterator_Next(&it, &key))) {
    prev = next;
    if (!strcasecmp(key, "GROUPBY")) {
      next = buildGroupBy(child, plan->ctx, next, err);
    } else if (!strcasecmp(key, "SORTBY")) {
      next = buildSortBY(child, next, err);
    } else if (!strcasecmp(key, "APPLY")) {
      next = buildProjection(child, next, plan->ctx, err);
    } else if (!strcasecmp(key, "LIMIT")) {
      next = addLimit(child, next, err);
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

  RedisModule_Log(plan->ctx->redisCtx, "warning", "Could not parse aggregate reuqest: %s", *err);
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
    CmdArg_Free(cmd);

    RedisModule_ReplyWithError(ctx, err ? err : "Unkonown Error");
    return REDISMODULE_ERR;
  }
  Query_Expand(q, opts.expander);

  // TODO: Pass err here
  QueryPlan *plan = Query_BuildPlan(sctx, q, &opts, Aggregate_BuildProcessorChain, cmd, &err);
  if (!plan || err != NULL) {
    Query_Free(q);
    CmdArg_Free(cmd);

    RedisModule_ReplyWithError(ctx, err ? err : QUERY_ERROR_INTERNAL_STR);
    free(err);
    return REDISMODULE_ERR;
  }
  // Execute the query
  int rc = QueryPlan_Run(plan, &err);
  if (rc == REDISMODULE_ERR) {

    RedisModule_ReplyWithError(ctx, err ? err : QUERY_ERROR_INTERNAL_STR);
  }
  if (err) free(err);
  QueryPlan_Free(plan);
  Query_Free(q);
  CmdArg_Free(cmd);
  SearchCtx_Free(sctx);
  return rc;
}
