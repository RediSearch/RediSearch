#ifndef QUERY_INTERNAL_H
#define QUERY_INTERNAL_H

#include <stdlib.h>
#include <query_error.h>
#include <query_node.h>
#include "query_param.h"
#include "search_options.h"

#ifdef __cplusplus
extern "C" {
#endif
  typedef struct QueryParseCtx {
  const char *raw;
  size_t len;

  // the token count
  size_t numTokens;

  // the param count
  size_t numParams;

  // Index spec
  RedisSearchCtx *sctx;

  // query root
  QueryNode *root;

  const RSSearchOptions *opts;

  QueryError *status;

  #ifdef PARSER_DEBUG
  FILE *trace_log;
  #endif

} QueryParseCtx;

#define QPCTX_ISOK(qpctx) (!QueryError_HasError((qpctx)->status))

typedef struct {
  ConcurrentSearchCtx *conc;
  RedisSearchCtx *sctx;
  const RSSearchOptions *opts;
  QueryError *status;
  char ***vecScoreFieldNamesP;
  size_t numTokens;
  uint32_t tokenId;
  DocTable *docTable;
} QueryEvalCtx;

struct QueryAST;
struct NumericFilter;
struct GeoFilter;

// TODO: These APIs are helpers for the generated parser. They belong in the
// bowels of the actual parser, and should probably be a macro!

QueryNode *NewQueryNode(QueryNodeType type);
QueryNode *NewQueryNodeChildren(QueryNodeType type, QueryNode **children, size_t n);

QueryNode *NewTokenNode(QueryParseCtx *q, const char *s, size_t len);
QueryNode *NewTokenNodeExpanded(struct QueryAST *q, const char *s, size_t len, RSTokenFlags flags);
QueryNode *NewPhraseNode(int exact);

#define NewUnionNode() NewQueryNode(QN_UNION)
#define NewWildcardNode() NewQueryNode(QN_WILDCARD)
#define NewNotNode(child) NewQueryNodeChildren(QN_NOT, &child, 1)
#define NewOptionalNode(child) NewQueryNodeChildren(QN_OPTIONAL, &child, 1)

QueryNode *NewPrefixNode_WithParams(QueryParseCtx *q, QueryToken *qt);
QueryNode *NewFuzzyNode_WithParams(QueryParseCtx *q, QueryToken *qt, int maxDist);
QueryNode *NewNumericNode(QueryParam *p);
QueryNode *NewGeofilterNode(QueryParam *p);
QueryNode *NewVectorNode_WithParams(struct QueryParseCtx *q, VectorQueryType type, QueryToken *value, QueryToken *vec);
QueryNode *NewTagNode(const char *tag, size_t len);

QueryNode *NewTokenNode_WithParams(QueryParseCtx *q, QueryToken *qt);
void QueryNode_InitParams(QueryNode *n, size_t num);
bool QueryNode_SetParam(QueryParseCtx *q, Param *target_param, void *target_value,
                        size_t *target_len, QueryToken *source);

void QueryNode_SetFieldMask(QueryNode *n, t_fieldMask mask);

/* Free the query node and its children recursively */
void QueryNode_Free(QueryNode *n);

void RangeNumber_Free(RangeNumber *r);

#ifdef __cplusplus
}
#endif

#endif
