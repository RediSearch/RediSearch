/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
