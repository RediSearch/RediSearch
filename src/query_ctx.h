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

struct MetricRequest;

typedef struct QueryEvalCtx {
  RedisSearchCtx *sctx;
  const RSSearchOptions *opts;
  QueryError *status;
  struct MetricRequest **metricRequestsP;
  uint32_t tokenId;
  DocTable *docTable;
  uint32_t reqFlags;
  IteratorsConfig *config;
  // True while evaluating the subtree below a `NOT` node. A `NOT` only cares
  // whether a document matches its child, never the child's score, so a
  // descendant `UNION` may quick-exit on its first matching branch instead of
  // visiting every branch to accumulate a score. Saved and restored around the
  // child evaluation so nested `NOT`s (e.g. `-(-foo)`) keep it set correctly.
  bool inNotSubTree;
} QueryEvalCtx;
