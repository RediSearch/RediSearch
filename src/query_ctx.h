/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

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
  IteratorsConfig *config;
} QueryEvalCtx;
