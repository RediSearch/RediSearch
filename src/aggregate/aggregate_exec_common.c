/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
 #include "aggregate_exec_common.h"
 #include "util/timeout.h"
 #include "info/global_stats.h"
 #include "rmalloc.h"
 #include "util/array.h"
 #include "search_result_rs.h"

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

 SearchResult **AggregateResults(ResultProcessor *rp, int *rc) {
   SearchResult **results = array_new(SearchResult *, 8);
   SearchResult r = SearchResult_New();
   while (rp->parent->resultLimit && (*rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
     // Decrement the result limit, now that we got a valid result.
     rp->parent->resultLimit--;

     array_append(results, SearchResult_AllocateMove(&r));

     // clean the search result
     r = SearchResult_New();
   }

   if (*rc != RS_RESULT_OK) {
     SearchResult_Destroy(&r);
   }

   return results;
 }

 void startPipelineCommon(CommonPipelineCtx *ctx, ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc) {
   if (ctx->timeoutPolicy == TimeoutPolicy_Fail || ctx->oomPolicy == OomPolicy_Fail) {
     // Aggregate all results before populating the response
     *results = AggregateResults(rp, rc);
     // Check timeout after aggregation
     if (TimedOut(ctx->timeout) == TIMED_OUT) {
       *rc = RS_RESULT_TIMEDOUT;
     }
   } else {
     // Send the results received from the pipeline as they come (no need to aggregate)
     *rc = rp->Next(rp, r);
   }
 }
