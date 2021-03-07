
#pragma once

#include "query_error.h"
#include "query_node.h"

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct QueryAST;
struct NumericFilter;
struct GeoFilter;

//---------------------------------------------------------------------------------------------

struct QueryParse {
  const char *raw;
  size_t len;

  // the token count
  size_t numTokens;

  // Index spec
  RedisSearchCtx *sctx;

  // query root
  QueryNode *root;

  const RSSearchOptions *opts;

  QueryError *status;
};

#define QPCTX_ISOK(qpctx) (!(qpctx->HasError()->status))

//---------------------------------------------------------------------------------------------

struct Query : Object {
  ConcurrentSearchCtx *conc;
  RedisSearchCtx *sctx;
  const RSSearchOptions *opts;

  size_t numTokens;
  uint32_t tokenId;
  DocTable *docTable;

  IndexIterator *Eval(QueryNode *node);
};

//---------------------------------------------------------------------------------------------

// TODO: These APIs are helpers for the generated parser. They belong in the
// bowels of the actual parser, and should probably be a macro!

QueryNode *NewQueryNode(QueryNodeType type);
QueryNode *NewQueryNodeChildren(QueryNodeType type, QueryNode **children, size_t n);

QueryNode *NewTokenNode(QueryParse *q, const char *s, size_t len);
QueryNode *NewTokenNodeExpanded(struct QueryAST *q, const char *s, size_t len, RSTokenFlags flags);
QueryNode *NewPhraseNode(int exact);

#define NewUnionNode() NewQueryNode(QN_UNION)
#define NewWildcardNode() NewQueryNode(QN_WILDCARD)
#define NewNotNode(child) NewQueryNodeChildren(QN_NOT, &child, 1)
#define NewOptionalNode(child) NewQueryNodeChildren(QN_OPTIONAL, &child, 1)

QueryNode *NewPrefixNode(QueryParse *q, const char *s, size_t len);
QueryNode *NewFuzzyNode(QueryParse *q, const char *s, size_t len, int maxDist);
QueryNode *NewNumericNode(const struct NumericFilter *flt);
QueryNode *NewIdFilterNode(const t_docId *, size_t);
QueryNode *NewGeofilterNode(const struct GeoFilter *flt);
QueryNode *NewTagNode(const char *tag, size_t len);

///////////////////////////////////////////////////////////////////////////////////////////////
