
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

  QueryParse(char *query, size_t nquery, const RedisSearchCtx &sctx_,
             const RSSearchOptions &opts_, QueryError *status_);

  QueryNode *ParseRaw();
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

  Query(QueryAST &ast, const RSSearchOptions *opts_, RedisSearchCtx *sctx_,
        ConcurrentSearchCtx *conc_);

  IndexIterator *Eval(QueryNode *node);
};

//---------------------------------------------------------------------------------------------

// TODO: These APIs are helpers for the generated parser. They belong in the
// bowels of the actual parser, and should probably be a macro!

QueryNode *NewTokenNodeExpanded(struct QueryAST *q, const char *s, size_t len, RSTokenFlags flags);

#define NewWildcardNode() new QueryNode(QN_WILDCARD)
#define NewNotNode(child) new QueryNode(QN_NOT, &child, 1)
#define NewOptionalNode(child) new QueryNode(QN_OPTIONAL, &child, 1)

QueryNode *NewIdFilterNode(const t_docId *, size_t);

///////////////////////////////////////////////////////////////////////////////////////////////
