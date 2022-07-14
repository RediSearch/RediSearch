
#pragma once

#include "query_error.h"
#include "query_node.h"
#include "numeric_filter.h"
#include "geo_index.h"
#include "search_options.h"
#include "result_processor.h"

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct QueryAST;

//---------------------------------------------------------------------------------------------

struct QueryParse {
  const char *raw;
  size_t len;
  size_t numTokens;     // the token count
  RedisSearchCtx *sctx; // Index spec
  QueryNode *root;      // query root
  const RSSearchOptions *opts;
  QueryError *status;

  QueryParse(char *query, size_t nquery, const RedisSearchCtx &sctx_,
             const RSSearchOptions &opts_, QueryError *status_);

  QueryNode *ParseRaw();

  bool IsOk() { return status->HasError(); }
};

//---------------------------------------------------------------------------------------------

struct Query : Object {
  QueryConcurrentSearch *conc;
  RedisSearchCtx *sctx;
  const RSSearchOptions *opts;

  size_t numTokens;
  uint32_t tokenId;
  DocTable *docTable;

  Query(QueryAST &ast, const RSSearchOptions *opts_, RedisSearchCtx *sctx, QueryConcurrentSearch *conc);

  IndexIterator *Eval(QueryNode *node);
};

///////////////////////////////////////////////////////////////////////////////////////////////
