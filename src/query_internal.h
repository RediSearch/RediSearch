#ifndef QUERY_INTERNAL_H
#define QUERY_INTERNAL_H

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif
typedef struct RSQuery {
  const char *raw;
  size_t len;

  // the token count
  size_t numTokens;

  // parsing state
  int ok;

  // Index spec
  RedisSearchCtx *sctx;

  // query root
  QueryNode *root;

  char *errorMsg;
  RSSearchOptions opts;

} QueryParseCtx;

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