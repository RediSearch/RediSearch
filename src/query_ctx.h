#pragma once

#include "search_options.h"
#include "util/timeout.h"

typedef struct QueryEvalCtx {
  ConcurrentSearchCtx *conc;
  RedisSearchCtx *sctx;
  const RSSearchOptions *opts;
  QueryError *status;
  char ***vecScoreFieldNamesP;
  size_t numTokens;
  uint32_t tokenId;
  DocTable *docTable;
  uint32_t reqFlags;
} QueryEvalCtx;
