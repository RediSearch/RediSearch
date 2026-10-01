/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once
#include "redismodule.h"
#include "result_processor.h"
#include "aggregate.h"  // ChunkReplyState (anonymous-struct typedef, not forward-declarable)

typedef struct QueryError QueryError;

struct AREQ;
struct QueryRequestTimeout;

bool hasTimeoutError(QueryError *err);

bool ShouldReplyWithError(QueryErrorCode code, RSTimeoutPolicy timeoutPolicy, bool isProfile);

bool ShouldReplyWithTimeoutError(int rc, RSTimeoutPolicy timeoutPolicy, bool isProfile);

void ReplyWithTimeoutError(RedisModule_Reply *reply);

void destroyResults(SearchResult **results);

// TODO: replace `areq` with the borrowed atomic timed-out flag once it lives
// on QueryProcessingCtx; the AREQ back-pointer is a temporary plumbing
// shortcut for the main-thread timeout signal.
SearchResult **AggregateResults(ResultProcessor *rp, struct AREQ *areq, int *rc);

typedef struct CommonPipelineCtx {
  const struct QueryRequestTimeout *timeout;
  RSOomPolicy oomPolicy;

  // AREQ for the request being executed; consulted by AggregateResults (and
  // its debug pause loop) to observe the request timeout. NULL on paths without
  // a single owning AREQ (e.g. hybrid).
  // TODO: migrate to a borrowed atomic flag on QueryProcessingCtx.
  struct AREQ *areq;
} CommonPipelineCtx;

void startPipelineCommon(CommonPipelineCtx *ctx, ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc);
