#ifndef __QUERY_H__
#define __QUERY_H__

#include <stdlib.h>

#include "index.h"
#include "numeric_filter.h"
#include "rmutil/cmdparse.h"
#include "numeric_index.h"
#include "geo_index.h"
#include "query_node.h"
#include "query_parser/tokenizer.h"
#include "redis_index.h"
#include "redismodule.h"
#include "spec.h"
#include "id_filter.h"
#include "redisearch.h"
#include "rmutil/sds.h"
#include "concurrent_ctx.h"
#include "search_options.h"

/* A QueryParseCtx represents the parse tree and execution plan for a single
 * search QueryParseCtx */
typedef struct RSQuery {
  // the raw query text
  char *raw;
  // the raw text len
  size_t len;

  // the token count
  int numTokens;

  // the current token id (we assign ids to all token nodes)
  int tokenId;

  // parsing state
  int ok;

  // Index spec
  RedisSearchCtx *sctx;

  // query root
  QueryNode *root;

  char *errorMsg;

  RSSearchOptions opts;

} QueryParseCtx;

typedef struct {
  ConcurrentSearchCtx *conc;
  RedisSearchCtx *sctx;
  int numTokens;
  int tokenId;
  DocTable *docTable;
  RSSearchOptions *opts;
} QueryEvalCtx;

/* Evaluate a QueryParseCtx stage and prepare it for execution. As execution is lazy
this doesn't
actually do anything besides prepare the execution chaing */
IndexIterator *Query_EvalNode(QueryEvalCtx *q, QueryNode *n);

/* Free the QueryParseCtx execution stage and its children recursively */
void QueryNode_Free(QueryNode *n);
QueryNode *NewTokenNode(QueryParseCtx *q, const char *s, size_t len);
QueryNode *NewTokenNodeExpanded(QueryParseCtx *q, const char *s, size_t len, RSTokenFlags flags);
QueryNode *NewPhraseNode(int exact);
QueryNode *NewUnionNode();
QueryNode *NewPrefixNode(QueryParseCtx *q, const char *s, size_t len);
QueryNode *NewFuzzyNode(QueryParseCtx *q, const char *s, size_t len, int maxDist);
QueryNode *NewNotNode(QueryNode *n);
QueryNode *NewOptionalNode(QueryNode *n);
QueryNode *NewNumericNode(NumericFilter *flt);
QueryNode *NewIdFilterNode(IdFilter *flt);
QueryNode *NewWildcardNode();
QueryNode *NewGeofilterNode(GeoFilter *flt);
QueryNode *NewTagNode(const char *tag, size_t len);
void QueryNode_SetFieldMask(QueryNode *n, t_fieldMask mask);

void Query_SetNumericFilter(QueryParseCtx *q, NumericFilter *nf);
void Query_SetGeoFilter(QueryParseCtx *q, GeoFilter *gf);
void Query_SetIdFilter(QueryParseCtx *q, IdFilter *f);

/* Only used in tests, for now */
void QueryNode_Print(QueryParseCtx *q, QueryNode *qs, int depth);

#define QUERY_ERROR_INTERNAL_STR "Internal error processing QueryParseCtx"
#define QUERY_ERROR_INTERNAL -1

/* Initialize a new QueryParseCtx object from user input. This does not parse the QueryParseCtx
 * just yet */
QueryParseCtx *NewQueryParseCtx(RedisSearchCtx *sctx, const char *raw, size_t len,
                                RSSearchOptions *opts);

QueryNode *Query_Parse(QueryParseCtx *q, char **err);

void Query_Expand(QueryParseCtx *q, const char *expander);

/* Return a string representation of the QueryParseCtx parse tree. The string should be freed by
 * the
 * caller
 */
const char *Query_DumpExplain(QueryParseCtx *q);

typedef int (*QueryNode_ForEachCallback)(QueryNode *node, QueryParseCtx *q, void *ctx);
int Query_NodeForEach(QueryParseCtx *q, QueryNode_ForEachCallback callback, void *ctx);

/* Free a QueryParseCtx object */
void Query_Free(QueryParseCtx *q);

#endif
