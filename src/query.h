/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __QUERY_H__
#define __QUERY_H__

#include <stdlib.h>

#include "index.h"
#include "query_node.h"
#include "query_parser/tokenizer.h"
#include "redis_index.h"
#include "redismodule.h"
#include "spec.h"
#include "redisearch.h"
#include "hiredis/sds.h"
#include "concurrent_ctx.h"
#include "search_options.h"
#include "query_error.h"
#include "query_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

// Holds a yieldable field name, and the address to write the RLookupKey pointer later.
typedef struct MetricRequest{
  HiddenName *metric_name;
  RLookupKey **key_ptr;
} MetricRequest;


/**
 * Query AST structure.
 *
 * To parse a query, use QAST_Parse
 * To get an iterator from the query, use, QAST_Iterate()
 * To release the query AST, use QAST_Free()
 */
typedef struct QueryAST {
  size_t numTokens;
  size_t numParams;
  QueryNode *root;
  // User data and length, for use by scorers
  const void *udata;
  size_t udatalen;

  // array of additional metrics names in the AST.
  MetricRequest *metricRequests;

  // Copied query and length, because it seems we modify the string
  // in the parser (FIXME). Thus, if the original query is const
  // then it explodes
  char *query;
  size_t nquery;

  // Copy of RSGlobalConfig parameters required for query execution,
  // to ensure that they won't change during query execution.
  IteratorsConfig config;
} QueryAST;

/**
 * Parse the query string into an AST.
 * @param dst the AST structure to populate
 * @param sctx the context - this is never written to or retained
 * @param sopts options modifying parsing behavior
 * @param qstr the query string
 * @param len the length of the query string
 * @param dialectVersion parse the query according to the given dialect version
 * @param status error details set here.
 */
int QAST_Parse(QueryAST *dst, const RedisSearchCtx *sctx, const RSSearchOptions *sopts,
               const char *qstr, size_t len, unsigned int dialectVersion, QueryError *status);

IndexIterator *Query_EvalNode(QueryEvalCtx *q, QueryNode *n);

/**
 * Global filter options impact *all* query nodes. This structure can be used
 * to set global properties for the entire query
 */
typedef struct {
  // Used only to support legacy FILTER keyword. Should not be used by newer code
  const NumericFilter *numeric;
  // Used only to support legacy GEOFILTER keyword. Should not be used by newer code
  const GeoFilter *geo;

  /** List of IDs to limit to, and the length of that array */
  t_docId *ids;
  size_t nids;
} QAST_GlobalFilterOptions;

/** Set global filters on the AST */
void QAST_SetGlobalFilters(QueryAST *ast, const QAST_GlobalFilterOptions *options);

/**
 * Open the result iterator on the filters. Returns the iterator for the root node.
 *
 * @param ast the parsed tree
 * @param opts options
 * @param sctx the search context. Note that this may be retained by the iterators
 *  for the remainder of the query.
 * @param conc Used to save state on the query
 * @param reqflags Request (AGG/SEARCH) flags
 * @param status error detail
 * @return an iterator.
 */
IndexIterator *QAST_Iterate(QueryAST *ast, const RSSearchOptions *options,
                            RedisSearchCtx *sctx, ConcurrentSearchCtx *conc, uint32_t reqflags, QueryError *status);

/**
 * Expand the query using a pre-registered expander. Query expansion possibly
 * modifies or adds additional search terms to the query.
 * @param q the query
 * @param expander the name of the expander
 * @param opts query options, passed to the expander function
 * @param status error detail
 * @return REDISMODULE_OK, or REDISMODULE_ERR with more detail in `status`
 */
int QAST_Expand(QueryAST *q, const char *expander, RSSearchOptions *opts, RedisSearchCtx *sctx,
                QueryError *status);

int QAST_EvalParams(QueryAST *q, RSSearchOptions *opts, QueryError *status);
int QueryNode_EvalParams(dict *params, QueryNode *node, QueryError *status);

int QAST_CheckIsValid(QueryAST *q, IndexSpec *spec, RSSearchOptions *opts, QueryError *status);

/* Return a string representation of the QueryParseCtx parse tree. The string should be freed by the
 * caller */
char *QAST_DumpExplain(const QueryAST *q, const IndexSpec *spec);

/** Print a representation of the query to standard output */
void QAST_Print(const QueryAST *ast, const IndexSpec *spec);

/* Cleanup a query AST */
void QAST_Destroy(QueryAST *q);

QueryNode *RSQuery_ParseRaw_v1(QueryParseCtx *);
QueryNode *RSQuery_ParseRaw_v2(QueryParseCtx *);

#ifdef __cplusplus
}
#endif
#endif
