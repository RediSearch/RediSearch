
#pragma once

#include "query_error.h"
#include "query_node.h"
#include "numeric_filter.h"
#include "geo_index.h"
#include "search_options.h"
#include "result_processor.h"
#include "concurrent_ctx.h"

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct QueryAST;
struct QueryToken;

//---------------------------------------------------------------------------------------------

struct QueryParse {
  void *parser;
  const char *raw;
  size_t len;
  size_t numTokens;     // token count
  RedisSearchCtx *sctx; // Index spec
  QueryNode *root;      // query root
  const RSSearchOptions *opts;
  QueryError *status;

  QueryParse(char *query, size_t nquery, const RedisSearchCtx &sctx,
             const RSSearchOptions &opts, QueryError *status);

  QueryNode *ParseRaw();

  void Parse(int yymajor, const QueryToken &yyminor);
  bool IsOk() const { return !status->HasError(); }
};

//---------------------------------------------------------------------------------------------

struct Query : Object {
  ConcurrentSearch *conc;
  RedisSearchCtx *sctx;
  const RSSearchOptions *opts;

  size_t numTokens;
  uint32_t tokenId;
  DocTable *docTable;

  Query(size_t numTokens, const RSSearchOptions *opts_, RedisSearchCtx *sctx, ConcurrentSearch *conc);

  IndexIterator *Eval(QueryNode *node);
};

///////////////////////////////////////////////////////////////////////////////////////////////
