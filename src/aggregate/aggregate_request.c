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
  CmdSchema_AddPostional(requestSchema, "idx", CmdSchema_NewArg('s'), CmdSchema_Required);

  CmdSchema_AddPostional(requestSchema, "query", CmdSchema_NewArg('s'), CmdSchema_Required);
  CmdSchema_AddNamed(requestSchema, "SELECT", CmdSchema_NewVector('s'), CmdSchema_Required);
  CmdSchemaNode *grp = CmdSchema_AddSubSchema(requestSchema, "GROUPBY",
                                              CmdSchema_Required | CmdSchema_Repeating, NULL);
  CmdSchema_AddPostional(grp, "by", CmdSchema_NewVector('s'), CmdSchema_Required);

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

ResultProcessor *buildGroupBy(CmdArg *grp, RSSearchRequest *req, ResultProcessor *upstream,
                              char **err) {

  CmdArg *by = CmdArg_FirstOf(grp, "by");
  if (!by || CMDARG_ARRLEN(by) == 0) return NULL;

  RSMultiKey *keys = RS_NewMultiKeyFromArgs(&CMDARG_ARR(by));
  Grouper *g = NewGrouper(keys, req->sctx && req->sctx->spec ? req->sctx->spec->sortables : NULL);

  // Add reducerss
  CMD_FOREACH_SELECT(grp, "REDUCE", {
    if (!parseReducer(g, result, err)) goto fail;
  });

  return NewGrouperProcessor(g, upstream);

fail:
  RedisModule_Log(req->sctx->redisCtx, "warning", "Error paring GROUPBY: %s", *err);
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

fail:
  // RedisModule_Log(NULL, "warning", "Error parsing PROJECT: %s", err);
  // Grouper_Free(g);
  return NULL;
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
      ReturnedField *rf = FieldList_GetCreateField(
          ret, RedisModule_CreateString(ctx, CMDARG_STRPTR(child), CMDARG_STRLEN(child)));
      rf->explicitReturn = 1;
    }
  }
  return ret;
}

ResultProcessor *Query_BuildAggregationChain(QueryPlan *q, RSSearchRequest *req, CmdArg *cmd,
                                             char **err) {

  // The base processor translates index results into search results
  ResultProcessor *next = NewBaseProcessor(q, &q->execCtx);

  FieldList *lst = getAggregateFields(req->sctx->redisCtx, cmd);
  next = NewLoader(next, req->sctx, lst);

  CmdArgIterator it = CmdArg_Children(cmd);
  CmdArg *child;
  const char *key;
  while (NULL != (child = CmdArgIterator_Next(&it, &key))) {
    if (!strcasecmp(key, "GROUPBY")) {
      next = buildGroupBy(child, req, next, err);
    } else if (!strcasecmp(key, "SORTBY")) {
      next = buildSortBY(child, next, err);
    } else if (!strcasecmp(key, "PROJECT")) {
      next = buildProjection(child, next, err);
    } else if (!strcasecmp(key, "LIMIT")) {
      next = addLimit(child, next, err);
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