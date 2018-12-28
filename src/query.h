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
#include "rmutil/sds.h"
#include "concurrent_ctx.h"
#include "search_options.h"
#include "query_error.h"
#include "query_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct QueryAST {
  size_t numTokens;
  QueryNode *root;
  // User data and length, for use by scorers
  const void *udata;
  size_t udatalen;
  // Copied query and length, because it seems we modify the string
  // in the parser (FIXME). Thus, if the original query is const
  // then it explodes
  char *query;
  size_t nquery;
} QueryAST;

/**
 * Parse the query string into an AST.
 * TODO: Populate with options here...
 *
 * @param src the AST structure to populate
 * @param sctx the context - this is never written to or retained
 * @param sopts options modifying parsing behavior
 * @param qstr the query string
 * @param len the length of the query string
 * @param status error details set here.
 */
int QAST_Parse(QueryAST *dst, const RedisSearchCtx *sctx, const RSSearchOptions *sopts,
               const char *qstr, size_t len, QueryError *status);

typedef struct {
  const NumericFilter *numeric;
  const GeoFilter *geo;

  /** List of IDs to limit to, and the length of that array */
  t_docId *ids;
  size_t nids;
} QAST_GlobalFilterOptions;

/** Set global filters on the AST */
void QAST_SetGlobalFilters(QueryAST *ast, const QAST_GlobalFilterOptions *options);

/**
 * Open the result iterator on the filters.
 * Returns the iterator for the root node.
 * If there are no results, NULL is returned and the error is set to
 *
 * @ref QUERY_ENORESULTS
 * @param ast the parsed tree
 * @param opts options
 * @param sctx the search context. Note that this may be retained by the iterators
 *  for the remainder of the query.
 * @return an iterator.
 */
IndexIterator *QAST_Iterate(const QueryAST *ast, const RSSearchOptions *options,
                            RedisSearchCtx *sctx, ConcurrentSearchCtx *conc, QueryError *status);

int QAST_Expand(QueryAST *q, const char *expander, RSSearchOptions *opts, RedisSearchCtx *sctx,
                QueryError *status);

// TODO: These APIs are helpers for the generated parser. They belong in the
// bowels of the actual parser, and should probably be a macro!
QueryNode *NewTokenNode(QueryParseCtx *q, const char *s, size_t len);
QueryNode *NewTokenNodeExpanded(QueryAST *q, const char *s, size_t len, RSTokenFlags flags);
QueryNode *NewPhraseNode(int exact);
QueryNode *NewUnionNode();
QueryNode *NewPrefixNode(QueryParseCtx *q, const char *s, size_t len);
QueryNode *NewFuzzyNode(QueryParseCtx *q, const char *s, size_t len, int maxDist);
QueryNode *NewNotNode(QueryNode *n);
QueryNode *NewOptionalNode(QueryNode *n);
QueryNode *NewNumericNode(const NumericFilter *flt);
QueryNode *NewIdFilterNode(const t_docId *, size_t);
QueryNode *NewWildcardNode();
QueryNode *NewGeofilterNode(const GeoFilter *flt);
QueryNode *NewTagNode(const char *tag, size_t len);
void QueryNode_SetFieldMask(QueryNode *n, t_fieldMask mask);

/* Only used in tests, for now */
void QueryNode_Print(QueryParseCtx *q, QueryNode *qs, int depth);

#define QUERY_ERROR_INTERNAL_STR "Internal error processing QueryParseCtx"
#define QUERY_ERROR_INTERNAL -1

/* Evaluate a QueryParseCtx stage and prepare it for execution. As execution is lazy
this doesn't
actually do anything besides prepare the execution chaing */
IndexIterator *Query_EvalNode(QueryEvalCtx *q, QueryNode *n);

/* Free the QueryParseCtx execution stage and its children recursively */
void QueryNode_Free(QueryNode *n);

/* Return a string representation of the QueryParseCtx parse tree. The string should be freed by the
 * caller */
char *Query_DumpExplain(const QueryAST *q, const IndexSpec *spec);

/** Print a representation of the query to standard output */
void QAST_Print(const QueryAST *ast, const IndexSpec *spec);

typedef int (*QueryNode_ForEachCallback)(QueryNode *node, void *q, void *ctx);
int Query_NodeForEach(QueryAST *q, QueryNode_ForEachCallback callback, void *ctx);

/* Cleanup a query AST */
void QAST_Destroy(QueryAST *q);

#ifdef __cplusplus
}
#endif
#endif
