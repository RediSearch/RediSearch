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

  CmdSchema_AddNamed(requestSchema, "FILTER", CmdSchema_NewArg('s'), CmdSchema_Required);
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

int parseReducer(Grouper *g, CmdArg *red) {

  const char *func = CMDARG_STRPTR(CmdArg_FirstOf(red, "func")));

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

  // Add reducerss
  CMD_FOREACH_SELECT(grp, "REDUCE", {
    if (!parseReducer(g, result)) goto fail;
  });
  return NewGrouperProcessor(g, upstream);

fail:
  // Grouper_Free(g);
  return NULL;
}
ResultProcessor *Query_BuildAggregationChain(QueryPlan *q, RSSearchRequest *req, CmdArg *cmd) {

  // The base processor translates index results into search results
  ResultProcessor *next = NewBaseProcessor(q, &q->execCtx);

  CmdArg *groupBy = CmdArg_FirstOf(cmd, "GROUPBY");
  if (!groupBy) goto fail;
  // // If we are not in SORTBY mode - add a scorer to the chain
  // if (req->sortBy == NULL) {
  //   next = NewScorer(req->scorer, next, req);
  // }

  // // The sorter sorts the top-N results
  // next = NewSorter(req->sortBy, req->offset + req->num, next, req->fields.wantSummaries);

  // // The pager pages over the results of the sorter
  // next = NewPager(next, req->offset, req->num);

  // // The loader loads the documents from redis
  // // If we do not need to return any fields - we do not need the loader in the loop
  // if (!(req->flags & Search_NoContent)) {
  //   next = NewLoader(next, req);
  //   if (req->fields.wantSummaries && (req->sctx->spec->flags & Index_StoreTermOffsets) != 0) {
  //     next = NewHighlightProcessor(next, req);
  //   }
  // }

  // return next;

fail:
  return NULL;
}