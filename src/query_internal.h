/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef QUERY_INTERNAL_H
#define QUERY_INTERNAL_H

#include <stdlib.h>
#include <query_error.h>
#include <query_node.h>
#include "query_param.h"
#include "vector_index.h"
#include "geometry_index.h"
#include "query_ctx.h"

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

QueryNode *NewPrefixNode_WithParams(QueryParseCtx *q, QueryToken *qt, bool prefix, bool suffix);
QueryNode *NewFuzzyNode_WithParams(QueryParseCtx *q, QueryToken *qt, int maxDist);
QueryNode *NewNumericNode(QueryParam *p);
QueryNode *NewGeometryNode_FromWkt(const char *geom, size_t len);
QueryNode *NewGeofilterNode(QueryParam *p);
QueryNode *NewVectorNode_WithParams(struct QueryParseCtx *q, VectorQueryType type, QueryToken *value, QueryToken *vec);
QueryNode *NewTagNode(const char *tag, size_t len);
QueryNode *NewVerbatimNode_WithParams(QueryParseCtx *q, QueryToken *qt);
QueryNode *NewWildcardNode_WithParams(QueryParseCtx *q, QueryToken *qt);

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
