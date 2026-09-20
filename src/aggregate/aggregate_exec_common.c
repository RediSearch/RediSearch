/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
 #include "aggregate_exec_common.h"

#ifdef ENABLE_ASSERT
#include "debug_commands.h" // IWYU pragma: keep
#endif

 #include "search_result_ffi.h"
 #include "aggregate.h"
#include "hybrid/hybrid_request.h"
 #include "util/timeout.h"
#include "query_error_ffi.h"
#include "reply.h"
#include "rmutil/rm_assert.h"

#ifdef ENABLE_ASSERT
#include <unistd.h>  // usleep, used by debugCheckAndPauseAfterAggregateResult
#endif

 bool hasTimeoutError(QueryError *err) {
   return QueryError_GetCode(err) == QUERY_ERROR_CODE_TIMED_OUT;
 }

 bool ShouldReplyWithError(QueryErrorCode code, RSTimeoutPolicy timeoutPolicy, bool isProfile) {
   return code != QUERY_ERROR_CODE_OK
       && (code != QUERY_ERROR_CODE_TIMED_OUT
           || (code == QUERY_ERROR_CODE_TIMED_OUT
               && timeoutPolicy == TimeoutPolicy_Fail
               && !isProfile));
 }

 bool ShouldReplyWithTimeoutError(int rc, RSTimeoutPolicy timeoutPolicy, bool isProfile) {
   return rc == RS_RESULT_TIMEDOUT
          && timeoutPolicy == TimeoutPolicy_Fail
          && !isProfile;
 }

 void ReplyWithTimeoutError(RedisModule_Reply *reply) {
   RedisModule_Reply_Error(reply, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));
 }

#ifdef ENABLE_ASSERT
// Helper function to check and pause after extracting a result from the
// Pipeline_SerializeResults loop (for testing pipeline state mid-aggregation).
// Self-releases the pause when the request has been marked as timed out by
// the main-thread timeout callback (RETURN-STRICT path): the callback waits
// synchronously for BG to signal completion, so the test cannot send a
// resume command while it is in flight.
// The hook is for pausing a live run; a drain of an already-stopped pipeline is not one.
static inline void debugCheckAndPauseAfterAggregateResult(QueryRequest *request, bool live) {
  if (!live) return;
  int pauseAfterN = AggregateResultsDebugCtx_GetPauseAfterN();
  if (pauseAfterN <= AGGREGATE_RESULTS_NO_PAUSE) {
    return;
  }
  AggregateResultsDebugCtx_IncrementResultsCount();
  if (AggregateResultsDebugCtx_GetResultsCount() != pauseAfterN) {
    return;
  }
  // Pause after the Nth result has been extracted (1-based)
  AggregateResultsDebugCtx_SetPause(true);
  while (AggregateResultsDebugCtx_IsPaused()) {
    if (QueryRequestTimeout_IsBlockedClientTimedOut(&request->timeout)) {
      AggregateResultsDebugCtx_SetPause(false);
      break;
    }
    usleep(1000);  // Spin-wait with 1ms sleep
  }
}
#else
// Compiler eliminates the function completely in release builds - zero overhead
static inline void debugCheckAndPauseAfterAggregateResult(QueryRequest *request, bool live) {}
#endif

static const RequestConfig *requestConfig(const QueryRequest *request) {
  QueryRequest *mutable = (QueryRequest *)request;  // the kind-checked getters only read
  return request->kind == QUERY_REQUEST_KIND_HYBRID ? &QueryRequest_GetHybrid(mutable)->reqConfig
                                                     : &QueryRequest_GetAREQ(mutable)->reqConfig;
}

static bool returnPolicy(const RequestConfig *config) {
  return config->timeoutPolicy == TimeoutPolicy_Return && config->oomPolicy != OomPolicy_Fail;
}

bool ReturnCommitsRows(const QueryRequest *request) {
  return returnPolicy(requestConfig(request)) && request->reply.rows.count > 0;
}

void Pipeline_SerializeResults(QueryRequest *request, ResultProcessor *rp, SerializeResult serialize, const cachedVars *cv, bool live, int *rc) {
  // Prepare stack data. Only a RETURN_STRICT timeout callback drains a stopped pipeline.
  RS_ASSERT(live || request->timeout.policy == TimeoutPolicy_ReturnStrict);
  const QueryRequestTimeout *timeout = &request->timeout;
  const bool clockTimeoutApplies = live && !returnPolicy(requestConfig(request));
  // Untracked runs read a flag that never flips, so the loop body has no per-row policy branches.
  RS_Atomic(bool) neverTimedOut = false;
  RS_Atomic(bool) *timedOut = live && timeout->kind == QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT
                                  ? QueryRequestTimeout_GetBlockedClientFlag(&request->timeout) : &neverTimedOut;
  QueryRequestAsyncState *async = &request->async;
  RedisModule_Reply *rows = &request->reply.rows;
  SearchResult row = SearchResult_New();

  // Serialize until the budget is spent or Next() stops yielding (EOF, error, timeout). The pipeline runs at
  // least once even on a zero row budget (MAXAGGREGATERESULTS 0), since the total it reports is only computed
  // by running it.
  do {
    *rc = rp->Next(rp, &row);
    if (*rc != RS_RESULT_OK || !rp->parent->resultLimit) break;
    rp->parent->resultLimit--;

    // REPLY brackets exactly the serialize() call; PIPELINE (the resting phase, including during Next() and the
    // debug-pause hook below) resumes right after -- a timeout observed while paused there must attribute to
    // PIPELINE, not REPLY. The marker is frozen once the timeout fires.
    if (!RS_AtomicBoolLoadRelaxed(timedOut)) QueryRequestAsyncState_SetExecutionPhase(async, QUERY_TIMEOUT_STAGE_REPLY);
    serialize(request, rows, &row, cv);
    SearchResult_Clear(&row);
    if (!RS_AtomicBoolLoadRelaxed(timedOut)) QueryRequestAsyncState_SetExecutionPhase(async, QUERY_TIMEOUT_STAGE_PIPELINE);
    debugCheckAndPauseAfterAggregateResult(request, live);
    if (RS_AtomicBoolLoadRelaxed(timedOut)) {
      *rc = RS_RESULT_TIMEDOUT;
      break;
    }
  } while (rp->parent->resultLimit);

  // Cleanup. The buffered rows (and their count) are the reply phase's remaining input.
  if (clockTimeoutApplies && QueryRequestTimeout_IsTimedOutExact(timeout)) {
    *rc = RS_RESULT_TIMEDOUT;
  }
  SearchResult_Destroy(&row);
}

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
 * Profile (`FT.PROFILE`) interleaves an RP_PROFILE wrapper around every RP,
 * so the classifier transparently skips RP_PROFILE wrappers while walking
 * from `endProc`. The root proc type is read from `qctx->rootProc`, which
 * always points at the real root (RP_INDEX / RP_NETWORK) regardless of
 * profiling, and the drain itself walks `endProc->Next` which delegates
 * through the profile wrappers.
 */
bool pipelineCanYieldPartialResults(AREQ *r) {
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
  ResultProcessor *end = qctx->endProc;
  ResultProcessor *root = qctx->rootProc;

  if (!end || !root) {
    return false;
  }

  ResultProcessor *rp = end;
  while (rp->type == RP_PROFILE || rp->type == RP_PAGER_LIMITER ||
         rp->type == RP_VECTOR_NORMALIZER) {
    rp = rp->upstream;
    RS_ASSERT(rp);
  }

  switch (root->type) {
    case RP_INDEX:
      // Shard: RPIndex / RPPager don't buffer; only RPSorter does. Reject
      // shapes (1) and (2) so the drain never re-enters the QI.
      return rp->type == RP_SORTER;
    case RP_NETWORK:
      return rp == root || rp->type == RP_SORTER;
    default:
      return false;
  }
}
