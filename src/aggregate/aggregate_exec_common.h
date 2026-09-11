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
// shortcut for the main-thread RETURN-STRICT abort signal.
SearchResult **AggregateResults(ResultProcessor *rp, struct AREQ *areq, int *rc);

typedef struct CommonPipelineCtx {
  const struct QueryRequestTimeout *timeout;
  RSOomPolicy oomPolicy;
  QueryRequest *request;

  // AREQ for the request being executed; consulted by AggregateResults (and
  // its debug pause loop) to observe the request timeout. NULL on paths without
  // a single owning AREQ (e.g. hybrid).
  // TODO: migrate to a borrowed atomic flag on QueryProcessingCtx.
  struct AREQ *areq;
} CommonPipelineCtx;

void startPipelineCommon(CommonPipelineCtx *ctx, ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc);

typedef void (*SerializeResult)(void *request, RedisModule_Reply *reply, const SearchResult *row,
                                const cachedVars *cv);

// A NULL ctx->timeout drains an already-stopped pipeline under the caller's ownership.
void Pipeline_SerializeResults(const CommonPipelineCtx *ctx, ResultProcessor *rp,
                               RedisModule_Reply *rows, SerializeResult serialize, void *request,
                               const cachedVars *cv, void (*prepare)(void *request), int *rc);

/**
 * True iff draining `endProc->Next` after a RETURN-STRICT timeout produces a
 * valid (possibly empty) partial answer for the request's pipeline.
 *
 * The set of accepted shapes is selected by inspecting the pipeline's root
 * processor type -- specifically whether the root itself buffers results
 * that can be replayed after the upstream pipeline has aborted on TIMEDOUT.
 *
 * Coordinator (root is `RP_NETWORK`): RPNet maintains an internal queue of
 * shard responses received before the timeout, so all three of the
 * following shapes can be drained (top = end of pipeline):
 *   1. RPNet                                         -- bare root.
 *   2. RPPager_Limiter -> RPNet                      -- pager directly above the root.
 *   3. [RPPager_Limiter ->] RPSorter -> ...          -- end is RPSorter (optionally
 *                                                       under a pager); anything
 *                                                       between the sorter and
 *                                                       the root is allowed.
 *
 * Shard (root is `RP_INDEX`): RPIndex pulls fresh from the query iterator
 * on every call and RPPager has no buffer of its own, so shapes (1) and
 * (2) have nothing to harvest -- draining them would re-enter the QI for
 * no useful work. Only shape (3) is accepted: rpsortNext_Yield (the state
 * RPSorter enters on TIMEDOUT) pops from the sorter's heap without
 * re-entering its upstream.
 *
 * Any other root type returns false.
 *
 * Note that even when this returns false, partial results that BG already
 * serialized into `base.reply.rows` *before* the timeout fired (e.g. for a
 * trivial RPIndex -> RPPager pipeline) are still emitted via the buffered
 * results path in `serializeAndReplyResults_*`; that path is independent
 * of this classifier.
 *
 * Profile is excluded: it wraps every RP and is not yet supported under
 * RETURN-STRICT drain.
 */
bool pipelineCanYieldPartialResults(struct AREQ *r);

// Requires exclusive pipeline ownership after the worker's completion handshake.
void AREQ_DrainStoredResultsAfterTimeout(struct AREQ *req);
