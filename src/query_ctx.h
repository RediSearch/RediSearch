#pragma once

#include "search_options.h"
#include "util/timeout.h"
struct MetricRequest;

typedef struct QueryEvalCtx {
  ConcurrentSearchCtx *conc;
  RedisSearchCtx *sctx;
  const RSSearchOptions *opts;
  QueryError *status;
  struct MetricRequest **metricRequestsP;
  size_t numTokens;
  uint32_t tokenId;
  DocTable *docTable;
  uint32_t reqFlags;
} QueryEvalCtx;
