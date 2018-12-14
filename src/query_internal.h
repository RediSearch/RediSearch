#ifndef QUERY_INTERNAL_H
#define QUERY_INTERNAL_H

#include <stdlib.h>
#include <query_error.h>

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
} QueryEvalCtx;

#ifdef __cplusplus
}
#endif

#endif