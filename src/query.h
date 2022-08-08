
#pragma once

#include <stdlib.h>

#include "index.h"
#include "query_node.h"
#include "query_parser/tokenizer.h"
#include "redis_index.h"
#include "redismodule.h"
#include "spec.h"
#include "redisearch.h"
#include "rmutil/sds.h"
#include "concurrent_ctx.h"
#include "search_options.h"
#include "query_error.h"
#include "query_internal.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Query AST structure
//
// To parse a query, use QAST::QAST()
// To get an iterator from the query, use, QAST::Iterate()

struct QueryAST : public Object {
  size_t numTokens;
  QueryNode *root;
  // User data and length, for use by scorers
  SimpleBuff udata;

  // Copied query and length, because it seems we modify the string in the parser (FIXME).
  // Thus, if the original query is const then it explodes.
  String query;

  QueryAST(const RedisSearchCtx &sctx, const RSSearchOptions &sopts, std::string_view query, QueryError *status);
  ~QueryAST();

  // Set global filters on the AST
  void SetGlobalFilters(const NumericFilter *numeric);
  void SetGlobalFilters(const GeoFilter *geo);
  void SetGlobalFilters(Vector<t_docId> &ids);

  IndexIterator *Iterate(const RSSearchOptions &options, RedisSearchCtx &sctx,
                         ConcurrentSearch *conc) const;

  int Expand(const char *expander, RSSearchOptions *opts, RedisSearchCtx &sctx,
             QueryError *status);

  // Return a string representation of the QueryParse parse tree.
  // The string should be freed by the caller.
  char *DumpExplain(const IndexSpec *spec) const;

  // Print a representation of the query to standard output
  void Print(const IndexSpec *spec) const;

  void applyGlobalFilters(RSSearchOptions &opts, const RedisSearchCtx &sctx);
  void setFilterNode(QueryNode *n);

  QueryTokenNode *NewTokenNodeExpanded(std::string_view s, RSTokenFlags flags);
};

//---------------------------------------------------------------------------------------------

struct LexRange {
  IndexIterators its;
  Query *q;
  QueryNodeOptions *opts;
  double weight;

  LexRange(Query *q, QueryNodeOptions *opts, double weight = 0) :
    q(q), opts(opts), weight(weight) {}

  void rangeItersAddIterator(IndexReader *ir);
};

//---------------------------------------------------------------------------------------------

/*
// This structure can be used to set global properties for the entire query.

struct QAST_GlobalFilterOptions {
  // Used only to support legacy FILTER keyword. Should not be used by newer code
  const NumericFilter *numeric;
  // Used only to support legacy GEOFILTER keyword. Should not be used by newer code
  const GeoFilter *geo;

  // List of IDs to limit to, and the length of that array
  t_docId *ids;
  size_t nids;
};
*/

///////////////////////////////////////////////////////////////////////////////////////////////
