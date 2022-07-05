
#pragma once

#include "query_error.h"
#include "query_node.h"
#include "numeric_filter.h"
#include "geo_index.h"
#include "search_options.h"

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct QueryAST;

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

///////////////////////////////////////////////////////////////////////////////////////////////
