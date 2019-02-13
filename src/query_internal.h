#ifndef QUERY_INTERNAL_H
#define QUERY_INTERNAL_H

#include <stdlib.h>
#include <query_error.h>
#include <query_node.h>

#ifdef __cplusplus
extern "C" {
#endif
typedef struct RSQuery {
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

} QueryParseCtx;

#define QPCTX_ISOK(qpctx) (!QueryError_HasError((qpctx)->status))

typedef struct {
  ConcurrentSearchCtx *conc;
  RedisSearchCtx *sctx;
  const RSSearchOptions *opts;

  size_t numTokens;
  uint32_t tokenId;
  DocTable *docTable;
  QueryError *status;
} QueryEvalCtx;

struct QueryAST;
struct NumericFilter;
struct GeoFilter;

// TODO: These APIs are helpers for the generated parser. They belong in the
// bowels of the actual parser, and should probably be a macro!
QueryNode *NewQueryNode(QueryNodeType type);
QueryNode *NewTokenNode(QueryParseCtx *q, const char *s, size_t len);
QueryNode *NewTokenNodeExpanded(struct QueryAST *q, const char *s, size_t len, RSTokenFlags flags);
QueryNode *NewPhraseNode(int exact);
QueryNode *NewUnionNode();
QueryNode *NewPrefixNode(QueryParseCtx *q, const char *s, size_t len);
QueryNode *NewFuzzyNode(QueryParseCtx *q, const char *s, size_t len, int maxDist);
QueryNode *NewNotNode(QueryNode *n);
QueryNode *NewOptionalNode(QueryNode *n);
QueryNode *NewNumericNode(const struct NumericFilter *flt);
QueryNode *NewIdFilterNode(const t_docId *, size_t);
QueryNode *NewWildcardNode();
QueryNode *NewGeofilterNode(const struct GeoFilter *flt);
QueryNode *NewTagNode(const char *tag, size_t len);
void QueryNode_SetFieldMask(QueryNode *n, t_fieldMask mask);

/* Free the query node and its children recursively */
void QueryNode_Free(QueryNode *n);

#ifdef __cplusplus
}
#endif

#endif
