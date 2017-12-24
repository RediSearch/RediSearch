#include "aggregate.h"
#include "reducer.h"
#include <query.h>
#include <result_processor.h>
#include <rmutil/cmdparse.h>

static CmdSchemaNode *requestSchema = NULL;

void Aggregate_BuildSchema() {
  if (requestSchema) return;
  /*
  FT.AGGREGATE {index}
      FILTER {query}
      SELECT {nargs} {field} ...
      [
        GROUPBY {nargs} {property} ... [AS {alias}]
        GROUPREDUCE {function} {nargs} {arg} ... [AS {alias}]
        ...
      ]
      [SORTBY {nargs} {property} ... ]
      [PROJECT {function} {nargs} {args} [AS {alias}]]
      [LIMIT {count} {offset}]
      ...
      */
  requestSchema = NewSchema("FT.AGGREGATE", NULL);
  CmdSchema_AddPostional(requestSchema, "idx", CmdSchema_NewArg('s'), CmdSchema_Required);

  CmdSchema_AddPostional(requestSchema, "query", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddNamed(requestSchema, "SELECT", CmdSchema_NewVector('s'), CmdSchema_Required);
  CmdSchemaNode *grp = CmdSchema_AddSubSchema(requestSchema, "GROUPBY",
                                              CmdSchema_Required | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(grp, "by", CmdSchema_NewVector('s'), CmdSchema_Required);
  CmdSchema_AddNamed(grp, "AS", CmdSchema_NewArg('s'), CmdSchema_Optional);

  CmdSchemaNode *red =
      CmdSchema_AddSubSchema(grp, "REDUCE", CmdSchema_Required | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(red, "FUNC", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddPostional(red, "ARGS", CmdSchema_NewVector('s'), CmdSchema_Required);
  CmdSchema_AddNamed(red, "AS", CmdSchema_NewArg('s'), CmdSchema_Optional);

  CmdSchema_AddNamed(requestSchema, "SORTBY", CmdSchema_NewVector('s'),
                     CmdSchema_Optional | CmdSchema_Repeating);
  CmdSchemaNode *prj = CmdSchema_AddSubSchema(requestSchema, "PROJECT",
                                              CmdSchema_Optional | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(prj, "FUNC", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddPostional(prj, "ARGS", CmdSchema_NewVector('s'), CmdSchema_Required);
  CmdSchema_AddNamed(prj, "AS", CmdSchema_NewArg('s'), CmdSchema_Optional);
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

ResultProcessor *buildGroupBy(CmdArg *grp, RSSearchRequest *req, ResultProcessor *upstream) {

  CmdArg *by = CmdArg_FirstOf(grp, "by");
  if (!by || CMDARG_ARRLEN(by) == 0) return NULL;

  const char *prop = CMDARG_STRPTR(CMDARG_ARRELEM(by, 0));
  const char *alias = CMDARG_ORNULL(CmdArg_FirstOf(by, "AS"), CMDARG_STRPTR);
  Grouper *g =
      NewGrouper(prop, alias, req->sctx && req->sctx->spec ? req->sctx->spec->sortables : NULL);

  if (!g) return NULL;

  char *err;
  // Add reducerss
  CMD_FOREACH_SELECT(grp, "REDUCE", {
    if (!parseReducer(g, result, &err)) goto fail;
  });

  return NewGrouperProcessor(g, upstream);

fail:
  RedisModule_Log(req->sctx->redisCtx, "warning", "Error paring GROUPBY: %s", err);
  // Grouper_Free(g);
  return NULL;
}

ResultProcessor *buildSortBY(CmdArg *arg, ResultProcessor *upstream) {
  if (!arg || arg->type != CmdArg_Array) {
    return NULL;
  }

  RSMultiKey *mk = RS_NewMultiKey(CMDARG_ARRLEN(arg));
  for (size_t i = 0; i < mk->len; i++) {
    mk->keys[i] = CMDARG_STRPTR(CMDARG_ARRELEM(arg, i));
  }

  return NewSorterByFields(mk, 1, 0, upstream);
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
      ReturnedField *rf = FieldList_GetCreateField(
          ret, RedisModule_CreateString(ctx, CMDARG_STRPTR(child), CMDARG_STRLEN(child)));
      rf->explicitReturn = 1;
    }
  }
  return ret;
}

ResultProcessor *Query_BuildAggregationChain(QueryPlan *q, RSSearchRequest *req, CmdArg *cmd) {

  // The base processor translates index results into search results
  ResultProcessor *next = NewBaseProcessor(q, &q->execCtx);

  FieldList *lst = getAggregateFields(req->sctx->redisCtx, cmd);
  next = NewLoader(next, req->sctx, lst);

  CmdArgIterator it = CmdArg_Children(cmd);
  CmdArg *child;
  const char *key;
  while (NULL != (child = CmdArgIterator_Next(&it, &key))) {
    if (!strcasecmp(key, "GROUPBY")) {
      next = buildGroupBy(child, req, next);
    } else if (!strcasecmp(key, "SORTBY")) {
      next = buildSortBY(child, next);
    }
    if (!next) {
      goto fail;
    }
  }

  return next;

fail:
  // TODO: dont leak memory here
  return NULL;
}