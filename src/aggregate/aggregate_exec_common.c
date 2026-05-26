/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
 #include "aggregate_exec_common.h"
 #include "search_result_ffi.h"
 #include "aggregate.h"
 #include "util/timeout.h"
 #include "info/global_stats.h"
 #include "rmalloc.h"
 #include "util/array.h"
 #include "debug_commands.h"

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

 void destroyResults(SearchResult **results) {
   if (results) {
     for (size_t i = 0; i < array_len(results); i++) {
       SearchResult_Destroy(results[i]);
       rm_free(results[i]);
     }
     array_free(results);
   }
 }

#ifdef ENABLE_ASSERT
// Helper function to check and pause after extracting a result from the
// AggregateResults loop (for testing pipeline state mid-aggregation).
// Self-releases the pause when the request has been marked as timed out by
// the main-thread timeout callback (RETURN-STRICT path): the callback waits
// synchronously for BG to signal completion, so the test cannot send a
// resume command while it is in flight.
static inline void debugCheckAndPauseAfterAggregateResult(AREQ *areq) {
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
    if (areq && AREQ_TimedOut(areq)) {
      AggregateResultsDebugCtx_SetPause(false);
      break;
    }
    usleep(1000);  // Spin-wait with 1ms sleep
  }
}
#else
// Compiler eliminates the function completely in release builds - zero overhead
static inline void debugCheckAndPauseAfterAggregateResult(AREQ *areq) {}
#endif

 SearchResult **AggregateResults(ResultProcessor *rp, AREQ *areq, int *rc) {
   SearchResult **results = array_new(SearchResult *, 8);
   SearchResult r = SearchResult_New();
   while (rp->parent->resultLimit && (*rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
     // Decrement the result limit, now that we got a valid result.
     rp->parent->resultLimit--;

     array_append(results, SearchResult_AllocateMove(&r));

     debugCheckAndPauseAfterAggregateResult(areq);

     // clean the search result
     r = SearchResult_New();

     // Honour a main-thread timeout flag at the row boundary: buffering
     // stages (safe loader, sorter yield) can keep emitting from internal
     // buffers without re-touching upstream's per-row timeout check.
     if (areq && AREQ_TimedOut(areq)) {
       *rc = RS_RESULT_TIMEDOUT;
       break;
     }
   }

   if (*rc != RS_RESULT_OK) {
     SearchResult_Destroy(&r);
   }

   return results;
 }

 void startPipelineCommon(CommonPipelineCtx *ctx, ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc) {
   if (ctx->timeoutPolicy != TimeoutPolicy_Return || ctx->oomPolicy == OomPolicy_Fail) {
     // Aggregate all results before populating the response
     *results = AggregateResults(rp, ctx->areq, rc);
     // Check timeout after aggregation
     if (!ctx->skipClockTimeoutChecks && TimedOut(ctx->timeout) == TIMED_OUT) {
       *rc = RS_RESULT_TIMEDOUT;
     }
   } else {
     // Send the results received from the pipeline as they come (no need to aggregate)
     *rc = rp->Next(rp, r);
   }
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
  * accumulated in `state.results` *before* the timeout fired (e.g. for a
  * trivial RPIndex -> RPPager pipeline) are still emitted via the buffered
  * results path in `serializeAndReplyResults_*`; that path is independent
  * of this classifier.
  *
  * Profile is excluded: it wraps every RP and is not yet supported under
  * RETURN-STRICT drain.
  */
 bool pipelineCanYieldPartialResults(AREQ *r) {
   if (IsProfile(r)) {
     return false;
   }

   QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
   ResultProcessor *end = qctx->endProc;
   ResultProcessor *root = qctx->rootProc;

   if (!end || !root) {
     return false;
   }

   ResultProcessor *rp = end;
   if (rp->type == RP_PAGER_LIMITER) {
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

 /**
  * Drain results buffered post-timeout into `req->storedReplyState.results`.
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
 void AREQ_DrainStoredResultsAfterTimeout(AREQ *req) {
   QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
   ResultProcessor *endProc = qctx->endProc;
   ChunkReplyState *stored = &req->storedReplyState;
   if (!stored->results) {
     stored->results = array_new(SearchResult *, 8);
   }

   SearchResult r = SearchResult_New();
   while (qctx->resultLimit && endProc->Next(endProc, &r) == RS_RESULT_OK) {
     qctx->resultLimit--;
     array_append(stored->results, SearchResult_AllocateMove(&r));
     r = SearchResult_New();
   }
   SearchResult_Destroy(&r);
 }
