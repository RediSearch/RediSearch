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
struct QueryRequestLimits;

bool hasTimeoutError(QueryError *err);

bool ShouldReplyWithError(QueryErrorCode code, RSTimeoutPolicy timeoutPolicy, bool isProfile);

bool ShouldReplyWithTimeoutError(int rc, RSTimeoutPolicy timeoutPolicy, bool isProfile);

void ReplyWithTimeoutError(RedisModule_Reply *reply);

/**
 * Turns a pipeline that stopped for memory pressure, a RS_RESULT_TIMEDOUT rc
 * with the memory stop requested, into the outcome `oomPolicy` asks
 * for, and returns any other rc unchanged:
 *
 * - OomPolicy_Fail: QUERY_ERROR_CODE_OUT_OF_MEMORY in `err` (unless it already
 *   holds an error) and RS_RESULT_ERROR. Only buffering stages can still reply
 *   with an error, which is why startPipelineCommon buffers under FAIL.
 * - Otherwise: the query OOM warning in `err` and RS_RESULT_EOF, so the rows
 *   produced so far are the final, partial result instead of a timeout.
 */
int ApplyOomStop(const struct QueryRequestLimits *limits, RSOomPolicy oomPolicy,
                 QueryError *err, int rc);

/**
 * Memory checkpoint for replaying rows a finished pipeline buffered: the
 * stored-results replay and the RETURN-STRICT drain. It reads memory only, not
 * the request's stop flags, which a timeout has already set when these run
 * from the timeout callback. On true the caller stops replaying; the query OOM
 * warning is then set in `err`. Never an error: replies may have started, and
 * ON_OOM FAIL was already decided by AggregateResults.
 */
bool StoredResults_ShouldStopForMemory(uint32_t *counter, QueryError *err);

void destroyResults(SearchResult **results);

typedef struct CommonPipelineCtx {
  struct QueryRequestTimeout *timeout;
  RSOomPolicy oomPolicy;

  // AREQ for the request being executed; its debug pause loop releases itself
  // on the request timeout. A subquery AREQ on hybrid.
  struct AREQ *areq;
} CommonPipelineCtx;

/**
 * Buffers the pipeline's rows until EOF, an error, the result limit or a stop
 * (the stop flags of `ctx->timeout`, or memory pressure, which requests an OOM
 * stop; an OOM stop under ON_OOM RETURN keeps collecting what the stages
 * hold). A stop returns the rows buffered so far with RS_RESULT_TIMEDOUT in
 * `rc`.
 */
SearchResult **AggregateResults(ResultProcessor *rp, const CommonPipelineCtx *ctx, int *rc);

void startPipelineCommon(CommonPipelineCtx *ctx, ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc);

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
 * accumulated in `state.results` *before* the timeout fired (e.g. for a
 * trivial RPIndex -> RPPager pipeline) are still emitted via the buffered
 * results path in `serializeAndReplyResults_*`; that path is independent
 * of this classifier.
 *
 * Profile is excluded: it wraps every RP and is not yet supported under
 * RETURN-STRICT drain.
 */
bool pipelineCanYieldPartialResults(struct AREQ *r);

/**
 * Drain results buffered post-timeout into `stored->results`. Generic
 * primitive shared by AREQ and HybridRequest paths -- callers gate on the
 * appropriate `qctx->canYieldPartialResults` flag and perform any
 * root-specific pre-drain setup (such as flipping RPNet's `drainOnly` mode
 * on the coordinator) before invoking this function.
 *
 * `qctx->resultLimit` (and any RPPager_Limiter's internal `remaining`)
 * reflect the post-abort budget, so this loop naturally respects the
 * user's LIMIT and terminates at EOF.
 */
void Pipeline_DrainStoredResultsAfterTimeout(QueryProcessingCtx *qctx, ChunkReplyState *stored);

/**
 * Drain results buffered post-timeout into `req->base.reply.results`.
 * Only safe for pipelines classified as yielding partial results -- caller
 * must gate on `qctx->canYieldPartialResults` and perform any root-specific
 * pre-drain setup (such as flipping RPNet's `drainOnly` mode on the
 * coordinator) before invoking this function.
 *
 * Caller must also have already flipped the request's timeout flag and
 * waited for the BG worker to exit the pipeline (e.g. via
 * AREQ_WaitForAggregateResultsComplete).
 *
 * The pager's internal `remaining` and `qctx->resultLimit` reflect the
 * post-abort budget, so this loop naturally respects the user's LIMIT and
 * terminates at EOF.
 */
void AREQ_DrainStoredResultsAfterTimeout(struct AREQ *req);
