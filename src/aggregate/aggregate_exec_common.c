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
 #include "util/timeout.h"
 #include "rmalloc.h"
#include "query_error_ffi.h"
#include "reply.h"
#include "rmutil/rm_assert.h"
#include "util/arr/arr.h"

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
    if (areq && QueryRequestTimeout_IsBlockedClientTimedOut(&areq->base.timeout)) {
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
     if (areq && QueryRequestTimeout_IsBlockedClientTimedOut(&areq->base.timeout)) {
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
   if (ctx->timeout->policy != TimeoutPolicy_Return || ctx->oomPolicy == OomPolicy_Fail) {
     // Aggregate all results before populating the response
     *results = AggregateResults(rp, ctx->areq, rc);
     // Check timeout after aggregation
     if (QueryRequestTimeout_IsTimedOutExact(ctx->timeout)) {
       *rc = RS_RESULT_TIMEDOUT;
     }
   } else {
     // Send the results received from the pipeline as they come (no need to aggregate)
     *rc = rp->Next(rp, r);
   }
 }
