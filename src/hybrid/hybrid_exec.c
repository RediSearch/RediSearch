/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "hybrid_exec.h"
#include "parse_hybrid.h"
#include "hybrid_request.h"
#include "aggregate/aggregate_exec_common.h"
#include "debug_commands.h"

#include "redismodule.h"
#include "redisearch.h"
#include "search_ctx.h"
#include "aggregate/aggregate.h"
#include "query_error.h"
#include "spec.h"
#include "rmalloc.h"
#include "cursor.h"
#include "score_explain.h"
#include "util/timeout.h"
#include "util/workers.h"
#include "info/global_stats.h"
#include "info/info_redis/block_client.h"
#include "info/info_redis/threads/current_thread.h"
#include "info/info_redis/types/blocked_queries.h"
#include "pipeline/pipeline.h"
#include "util/units.h"
#include "value.h"
#include "module.h"
#include "aggregate/reply_empty.h"
#include "profile/profile.h"
#include "search_disk_utils.h"

#include <time.h>

// Send a warning message to the client, optionally appending a suffix to identify the source
static inline void ReplyWarning(RedisModule_Reply *reply, const char *message, const char *suffix) {
  if (suffix) {
    RS_ASSERT(strlen(suffix) > 0);
    char *expanded_warning = NULL;
    rm_asprintf(&expanded_warning, "%s %s", message, suffix);
    RedisModule_Reply_SimpleString(reply, expanded_warning);
    rm_free(expanded_warning);
  } else {
    RedisModule_Reply_SimpleString(reply, message);
  }
}

// Handles query errors and sends warnings to client.
// ignoreTimeout: ignore timeout in tail if there's a timeout in subquery
// suffix: identifies where the error occurred ("SEARCH"/"VSIM"/"POST PROCESSING")
// Returns true if a timeout occurred and was processed as a warning
static inline bool handleAndReplyWarning(RedisModule_Reply *reply, QueryError *err, int returnCode, const char *suffix, bool ignoreTimeout) {
  bool timeoutOccurred = false;
  if (returnCode == RS_RESULT_TIMEDOUT && !ignoreTimeout) {
    // Track warnings in global statistics
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
    ReplyWarning(reply, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT), suffix);
    timeoutOccurred = true;
  } else if (returnCode == RS_RESULT_ERROR) {
    // Non-fatal error — convert to warning
    ReplyWarning(reply, QueryError_GetUserError(err), suffix);
    QueryError_ClearError(err);  // Free allocated message strings
  } else if (QueryError_HasReachedMaxPrefixExpansionsWarning(err)) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_REACHED_MAX_PREFIX_EXPANSIONS, 1, COORD_ERR_WARN);
    ReplyWarning(reply, QUERY_WMAXPREFIXEXPANSIONS, suffix);
  }

  return timeoutOccurred;
}

static int replyForHybridPreExecutionTimeout(RedisModuleCtx *ctx, bool internal,
                                             ProfileOptions profileOptions) {
  const bool isProfile = profileOptions & EXEC_WITH_PROFILE;
  const RSTimeoutPolicy timeoutPolicy = RSGlobalConfig.requestConfigParams.timeoutPolicy;
  const bool shouldReplyWithError =
      ShouldReplyWithTimeoutError(RS_RESULT_TIMEDOUT, timeoutPolicy, isProfile);

  if (shouldReplyWithError) {
    QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, !internal);
    return RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));
  }

  return common_hybrid_query_reply_empty(ctx, QUERY_ERROR_CODE_TIMED_OUT, internal, isProfile);
}

// Reply with warnings, adding suffixes to indicate the originating context (search/vsim/post-processing)
static void replyWarningsWithSuffixes(RedisModule_Reply *reply, HybridRequest *hreq,
                                       QueryProcessingCtx *qctx, int postProcessingRC) {
  bool timeoutInSubquery = false;

  // Handle warnings from each subquery, adding appropriate suffix
  for (size_t i = 0; i < hreq->nrequests; ++i) {
    QueryError* err = &hreq->errors[i];
    const char* suffix = i == 0 ? SEARCH_SUFFIX : VSIM_SUFFIX;
    const int subQueryReturnCode = hreq->subqueriesReturnCodes[i];
    timeoutInSubquery = handleAndReplyWarning(reply, err, subQueryReturnCode, suffix, false) || timeoutInSubquery;
  }

  // Handle warnings from post-processing stage
  handleAndReplyWarning(reply, qctx->err, postProcessingRC, POST_PROCESSING_SUFFIX, timeoutInSubquery);
}

static void HREQ_Execute_Callback(blockedClientHybridCtx *BCHCtx);

// Serializes a result for the `FT.HYBRID` command.
// The format is consistent, i.e., does not change according to the values of
// the reply, or the RESP protocol used.
static void serializeResult_hybrid(HybridRequest *hreq, RedisModule_Reply *reply, const SearchResult *r,
                              const cachedVars *cv) {
  const uint32_t options = HREQ_RequestFlags(hreq);
  const RSDocumentMetadata *dmd = SearchResult_GetDocumentMetadata(r);

  RedisModule_Reply_Map(reply); // >result

  // Reply should have the same structure of an FT.AGGREGATE reply

  if (options & QEXEC_F_SEND_SCORES) {
    RedisModule_Reply_SimpleString(reply, "score");
    if (!(options & QEXEC_F_SEND_SCOREEXPLAIN)) {
      // This will become a string in RESP2
      RedisModule_Reply_Double(reply, SearchResult_GetScore(r));
    } else {
      RedisModule_Reply_Array(reply);
      RedisModule_Reply_Double(reply, SearchResult_GetScore(r));
      SEReply(reply, SearchResult_GetScoreExplain(r));
      RedisModule_Reply_ArrayEnd(reply);
    }
  }

  if (!(options & QEXEC_F_SEND_NOFIELDS)) {
    const RLookup *lk = cv->lastLookup;

    if (SearchResult_GetFlags(r) & Result_ExpiredDoc) {
      RedisModule_Reply_Null(reply);
    } else {
      RedisSearchCtx *sctx = HREQ_SearchCtx(hreq);
      // Get the number of fields in the reply.
      // Excludes hidden fields, fields not included in RETURN and, score and language fields.
      SchemaRule *rule = (sctx && sctx->spec) ? sctx->spec->rule : NULL;
      uint32_t excludeFlags = RLOOKUP_F_HIDDEN;
      uint32_t requiredFlags = RLOOKUP_F_NOFLAGS;  // Hybrid does not use RETURN fields; it uses LOAD fields instead
      size_t skipFieldIndex_len = RLookup_GetRowLen(lk);
      bool skipFieldIndex[skipFieldIndex_len]; // After calling `RLookup_GetLength` will contain `false` for fields which we should skip below
      memset(skipFieldIndex, 0, skipFieldIndex_len * sizeof(*skipFieldIndex));
      size_t nfields = RLookup_GetLength(lk, SearchResult_GetRowData(r), skipFieldIndex, skipFieldIndex_len, requiredFlags, excludeFlags, rule);

      int i = 0;
      RLOOKUP_FOREACH(kk, lk, {
        if (!RLookupKey_GetName(kk) || !skipFieldIndex[i++]) {
          continue;
        }
        const RSValue *v = RLookupRow_Get(kk, SearchResult_GetRowData(r));
        RS_LOG_ASSERT(v, "v was found in RLookup_GetLength iteration")

        RedisModule_Reply_StringBuffer(reply, RLookupKey_GetName(kk), RLookupKey_GetNameLen(kk));

        SendReplyFlags flags = (options & QEXEC_F_TYPED) ? SENDREPLY_FLAG_TYPED : 0;
        flags |= (options & QEXEC_FORMAT_EXPAND) ? SENDREPLY_FLAG_EXPAND : 0;

        unsigned int apiVersion = sctx->apiVersion;
        if (RSValue_IsTrio(v)) {
          // Which value to use for duo value
          if (!(flags & SENDREPLY_FLAG_EXPAND)) {
            // STRING
            if (apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST) {
              // Multi
              v = RSValue_Trio_GetMiddle(v);
            } else {
              // Single
              v = RSValue_Trio_GetLeft(v);
            }
          } else {
            // EXPAND
            v = RSValue_Trio_GetRight(v);
          }
        }
        RedisModule_Reply_RSValue(reply, v, flags);
      });
    }
  }
  RedisModule_Reply_MapEnd(reply); // >result
}

static void startPipelineHybrid(HybridRequest *hreq, ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc) {
  CommonPipelineCtx ctx = {
    .timeoutPolicy = hreq->reqConfig.timeoutPolicy,
    .timeout = &hreq->sctx->time.timeout,
    .oomPolicy = hreq->reqConfig.oomPolicy,
    .skipTimeoutChecks = !HybridRequest_ShouldCheckTimeout(hreq),
  };
  startPipelineCommon(&ctx, rp, results, r, rc);
}

static void finishSendChunk_HREQ(HybridRequest *hreq, SearchResult **results, SearchResult *r, rs_wall_clock_ns_t duration, QueryError *err) {
  if (results) {
    destroyResults(results);
  } else {
    SearchResult_Destroy(r);
  }

  if (QueryError_IsOk(err) || hasTimeoutError(err)) {
    uint32_t reqflags = HREQ_RequestFlags(hreq);
    TotalGlobalStats_CountQuery(reqflags, duration);
  }

  // Reset the total results length
  QueryProcessingCtx *qctx = &hreq->tailPipeline->qctx;
  qctx->totalResults = 0;
  QueryError_ClearError(err);
}

static int HREQ_populateReplyWithResults(RedisModule_Reply *reply,
  SearchResult **results, HybridRequest *hreq, cachedVars *cv) {
    // populate the reply with an array containing the serialized results
    int len = array_len(results);
    array_foreach(results, res, {
      serializeResult_hybrid(hreq, reply, res, cv);
      SearchResult_Destroy(res);
      rm_free(res);
    });
    array_free(results);
    return len;
}

/**
 * Handles error/timeout checking and sends error reply if needed.
 * Returns true if an error was sent (caller should skip to cleanup).
 */
static bool handleSendChunkError_hybrid(HybridRequest *hreq, RedisModule_Reply *reply,
  QueryError *err, int rc) {
  if (ShouldReplyWithError(QueryError_GetCode(err), hreq->reqConfig.timeoutPolicy, IsProfile(hreq))) {
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(err), 1, COORD_ERR_WARN);
    RedisModule_Reply_Error(reply, QueryError_GetUserError(err));
    return true;
  } else if (ShouldReplyWithTimeoutError(rc, hreq->reqConfig.timeoutPolicy, IsProfile(hreq))) {
    QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
    ReplyWithTimeoutError(reply);
    return true;
  }
  return false;
}

/**
 * Prepares reply structure for hybrid format.
 * Opens the map and adds total_results.
 */
static void prepareSendChunkReply_hybrid(HybridRequest *hreq, RedisModule_Reply *reply,
  QueryProcessingCtx *qctx) {
  RedisModule_Reply_Map(reply);

  // <total_results>
  RedisModule_ReplyKV_LongLong(reply, "total_results", qctx->totalResults);

  RedisModule_ReplyKV_Array(reply, "results"); // >results
}

/**
 * Finishes reply structure for hybrid format.
 * Closes results array, adds warnings, execution_time, profile, and closes the map.
 */
static void finishSendChunkReply_hybrid(HybridRequest *hreq, RedisModule_Reply *reply,
  QueryProcessingCtx *qctx, int rc) {
  RedisModule_Reply_ArrayEnd(reply); // >results

  // warnings
  RedisModule_ReplyKV_Array(reply, "warnings"); // >warnings
  RedisSearchCtx *sctx = HREQ_SearchCtx(hreq);
  if (sctx->spec && sctx->spec->scan_failed_OOM) {
    RedisModule_Reply_SimpleString(reply, QUERY_WINDEXING_FAILURE);
  }
  if (QueryError_HasQueryOOMWarning(qctx->err)) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD, 1, COORD_ERR_WARN);
    // Cluster mode only: handled directly here instead of through handleAndReplyWarning()
    // because this warning is not related to subqueries or post-processing terminology
    RedisModule_Reply_SimpleString(reply, QUERY_WOOM_COORD);
  }
  if (QueryError_GetCode(qctx->err) == QUERY_ERROR_CODE_TIMED_OUT) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
    RedisModule_Reply_SimpleString(reply, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT));
  }

  replyWarningsWithSuffixes(reply, hreq, qctx, rc);

  RedisModule_Reply_ArrayEnd(reply); // >warnings

  // execution_time
  const rs_wall_clock_ns_t duration = rs_wall_clock_elapsed_ns(&hreq->profileClocks.initClock);
  double executionTime = rs_wall_clock_convert_ns_to_ms_d(duration);
  RedisModule_ReplyKV_Double(reply, "execution_time", executionTime);

  if (IsProfile(hreq)) {
    hreq->profile(reply, hreq);
  }

  RedisModule_Reply_MapEnd(reply);
}

/**
 * Serializes results and handles the main reply logic for hybrid.
 * Sets *results to NULL after consuming them, so finishSendChunk_HREQ won't double-free.
 * Returns true if reply was sent, false if error/timeout occurred before replying.
 */
static bool serializeAndReplyResults_hybrid(HybridRequest *hreq, RedisModule_Reply *reply,
  ResultProcessor *rp, QueryProcessingCtx *qctx, int rc, cachedVars *cv,
  SearchResult *r, SearchResult ***results, QueryError *err) {

  // If an error occurred, or a timeout in strict mode - return a simple error
  if (handleSendChunkError_hybrid(hreq, reply, err, rc)) {
    return false;
  }

  prepareSendChunkReply_hybrid(hreq, reply, qctx);

  if (*results != NULL) {
    HREQ_populateReplyWithResults(reply, *results, hreq, cv);
    *results = NULL;  // Results consumed and freed by HREQ_populateReplyWithResults
  } else {
    if (rp->parent->resultLimit && rc == RS_RESULT_OK) {
      serializeResult_hybrid(hreq, reply, r, cv);
    }

    SearchResult_Clear(r);
    if (rc == RS_RESULT_OK && rp->parent->resultLimit) {
      while (--rp->parent->resultLimit && (rc = rp->Next(rp, r)) == RS_RESULT_OK) {
        serializeResult_hybrid(hreq, reply, r, cv);
        SearchResult_Clear(r);
      }
    }
  }

  finishSendChunkReply_hybrid(hreq, reply, qctx, rc);
  return true;
}

#ifdef ENABLE_ASSERT
// Helper function to pause before/after store results for hybrid (for testing timeout during store)
static inline void debugPauseStoreResultsHybrid(HybridRequest *hreq, bool before) {
  // Only pause if we are using reply callback (otherwise we don't store results)
  if (!hreq->useReplyCallback) {
    return;
  }
  bool enabled = before ? StoreResultsDebugCtx_IsPauseBeforeEnabled()
                        : StoreResultsDebugCtx_IsPauseAfterEnabled();
  if (enabled) {
    StoreResultsDebugCtx_SetPause(true);
    while (StoreResultsDebugCtx_IsPaused()) {
      // Check if timed out - break to avoid deadlock with timeout callback
      if (HybridRequest_TimedOut(hreq)) {
        StoreResultsDebugCtx_SetPause(false);
        break;
      }
      usleep(1000);  // Spin-wait with 1ms sleep
    }
  }
}

// Helper function to pause before/after hybrid cursor storage ONLY (separate command)
static inline void debugPauseHybridStoreCursors(HybridRequest *hreq, bool before) {
  bool enabled = before ? HybridStoreCursorsDebugCtx_IsPauseBeforeEnabled()
                        : HybridStoreCursorsDebugCtx_IsPauseAfterEnabled();
  if (enabled) {
    HybridStoreCursorsDebugCtx_SetPause(true);
    while (HybridStoreCursorsDebugCtx_IsPaused()) {
      if (HybridRequest_TimedOut(hreq)) {
        HybridStoreCursorsDebugCtx_SetPause(false);
        break;
      }
      usleep(1000);
    }
  }
}
#else
static inline void debugPauseStoreResultsHybrid(HybridRequest *hreq, bool before) {
  UNUSED(hreq);
  UNUSED(before);
}
static inline void debugPauseHybridStoreCursors(HybridRequest *hreq, bool before) {
  UNUSED(hreq);
  UNUSED(before);
}
#endif

/**
 * Store pipeline results for reply_callback path (FAIL policy with workers).
 * Called after startPipelineHybrid when using reply_callback mode.
 * Stores results in hreq->storedReplyState so serializeStoredResults_hybrid can be called
 * from the reply_callback on the main thread.
 *
 * @param hreq The hybrid request
 * @param results Pipeline results (ownership transferred to storedReplyState)
 * @param rc Pipeline return code
 * @param cv Cached variables for result serialization
 */
void HREQ_StoreResults(HybridRequest *hreq, SearchResult **results, int rc, cachedVars cv) {
  // Store results in hreq for reply_callback to use
  hreq->storedReplyState.results = results;
  hreq->storedReplyState.rc = rc;
  hreq->storedReplyState.cv = cv;
  hreq->storedReplyState.hasStoredResults = true;
}

// Helper for error handling in coordinator HREQ execution.
// For FAIL policy (useReplyCallback=true): stores error for reply_callback to handle.
// For RETURN policy: replies with error directly.
void HREQ_ReplyOrStoreError(HybridRequest *hreq, RedisModuleCtx *ctx, QueryError *status) {
  if (hreq->useReplyCallback) {
    // Deep copy since QueryError contains heap-allocated strings.
    // reply_callback will clear the stored error after replying.
    QueryError_ClearError(&hreq->storedReplyState.err);
    QueryError_CloneFrom(status, &hreq->storedReplyState.err);
    // Clear the original to avoid leaking heap-allocated strings.
    QueryError_ClearError(status);
  } else {
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(status), 1, !IsInternal(hreq));
    QueryError_ReplyAndClear(ctx, status);
  }
}

/**
 * Activates the pipeline embedded in `hreq`, and serializes the appropriate
 * response to the client, according to the RESP protocol used (2/3).
 *
 * Note: Currently this is used only by the `FT.HYBRID` command, that does
 * not support cursors, thus this function does not handle
 * those cases. Support should be added as these features are added.
 *
 * Profile data is handled via the hreq->profile callback.
 *
 * @param hreq The hybrid request with built pipeline
 * @param reply Redis module reply object
 * @param limit Maximum number of results to return
 * @param cv Cached variables for result processing
 */
void sendChunk_hybrid(HybridRequest *hreq, RedisModule_Reply *reply, size_t limit, cachedVars cv) {
    SearchResult r = SearchResult_New();
    int rc = RS_RESULT_EOF;
    QueryProcessingCtx *qctx = &hreq->tailPipeline->qctx;
    ResultProcessor *rp = qctx->endProc;
    SearchResult **results = NULL;
    QueryError err = QueryError_Default();

    // Set the chunk size limit for the query
    rp->parent->resultLimit = limit;

    // Check if timed out before executing pipeline
    if (HybridRequest_TimedOut(hreq)) {
      // Timeout callback already replied - skip to cleanup without replying
      goto done_err;
    }

    startPipelineHybrid(hreq, rp, &results, &r, &rc);

    // Check if timed out during pipeline execution
    if (HybridRequest_TimedOut(hreq)) {
      // Timeout callback already replied - skip to cleanup without replying
      goto done_err;
    }

    if (hreq->useReplyCallback) {
      // Store results for reply_callback (includes cv)
      debugPauseStoreResultsHybrid(hreq, true);  // pause before
      HREQ_StoreResults(hreq, results, rc, cv);
      debugPauseStoreResultsHybrid(hreq, false); // pause after
      return;
    }

    // Get errors before replying (do not clear here; cleanup/teardown will handle it)
    HybridRequest_GetError(hreq, &err);

    serializeAndReplyResults_hybrid(hreq, reply, rp, qctx, rc, &cv, &r, &results, &err);

done_err:
    finishSendChunk_HREQ(hreq, results, &r, rs_wall_clock_elapsed_ns(&hreq->profileClocks.initClock), &err);
}

/**
 * Serialize results from stored state (reply_callback path for FAIL policy).
 * Called by DistHybridReplyCallback on the main thread after background thread stored results.
 */
void serializeStoredResults_hybrid(HybridRequest *hreq, RedisModule_Reply *reply) {
    QueryProcessingCtx *qctx = &hreq->tailPipeline->qctx;
    ResultProcessor *rp = qctx->endProc;
    ChunkReplyState *stored = &hreq->storedReplyState;

    // Create a stack-allocated SearchResult for finishSendChunk_HREQ cleanup
    SearchResult r = SearchResult_New();

    // Get error directly from hreq (no need to copy in HREQ_StoreResults)
    QueryError err = QueryError_Default();
    HybridRequest_GetError(hreq, &err);

    // Point qctx->err to the local error so finishSendChunkReply_hybrid/replyWarningsWithSuffixes
    // can access it. The original qctx->err pointed to a stack variable in RSExecDistHybrid
    // which is now gone (background thread returned). This local `err` remains valid until
    // we clear it at the end of this function.
    qctx->err = &err;

    // Get stored results and rc
    SearchResult **results = stored->results;
    int rc = stored->rc;

    serializeAndReplyResults_hybrid(hreq, reply, rp, qctx, rc, &stored->cv, &r, &results, &err);

    // Clear stored results pointer since ownership was transferred
    stored->results = NULL;
    stored->hasStoredResults = false;

    // finishSendChunk_HREQ handles cleanup and stats
    finishSendChunk_HREQ(hreq, results, &r, rs_wall_clock_elapsed_ns(&hreq->profileClocks.initClock), &err);

    // Clear the local error to avoid leak (QueryError may have allocated strings)
    QueryError_ClearError(&err);
}

// Simple version of sendChunk_hybrid that returns empty results for hybrid queries.
// Handles RESP3 protocol with map structure including total_results, results, warning, and execution_time.
// Includes OOM warning when QueryError has OOM status.
// Currently used during OOM conditions early bailout and return empty results instead of failing.
// Based on sendChunk_hybrid patterns.
void sendChunk_ReplyOnly_HybridEmptyResults(RedisModule_Reply *reply, QueryError *err) {
    RedisModule_Reply_Map(reply);

    // total_results
    RedisModule_ReplyKV_LongLong(reply, "total_results", 0);

    // results (empty array)
    RedisModule_ReplyKV_Array(reply, "results");
    RedisModule_Reply_ArrayEnd(reply);

    // warning
    RedisModule_ReplyKV_Array(reply, "warnings");
    if (QueryError_GetCode(err) == QUERY_ERROR_CODE_TIMED_OUT) {
        QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
        RedisModule_Reply_SimpleString(reply, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT));
    } else if (QueryError_HasQueryOOMWarning(err)) {
        QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD, 1, COORD_ERR_WARN);
        // This function is called by Coordinator or SA
        RedisModule_Reply_SimpleString(reply, QUERY_WOOM_COORD);
    }
    RedisModule_Reply_ArrayEnd(reply);

    // execution_time
    RedisModule_ReplyKV_Double(reply, "execution_time", 0.0);

    RedisModule_Reply_MapEnd(reply);
}

static inline void freeHybridParams(HybridPipelineParams *hybridParams) {
  if (hybridParams == NULL) {
    return;
  }
  if (hybridParams->scoringCtx) {
    HybridScoringContext_Free(hybridParams->scoringCtx);
    hybridParams->scoringCtx = NULL;
  }
  rm_free(hybridParams);
}

/**
 * Execute the hybrid search pipeline and send results to the client.
 * This function uses the hybrid-specific result serialization functions.
 * @param hreq The HybridRequest with built pipeline
 * @param ctx Redis module context for sending the reply
 * @param sctx Redis search context
 */
void HybridRequest_Execute(HybridRequest *hreq, RedisModuleCtx *ctx, RedisSearchCtx *sctx) {
    AGGPlan *plan = &hreq->tailPipeline->ap;
    cachedVars cv = {
        .lastLookup = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_LAST),
        .lastAstp = AGPLN_GetArrangeStep(plan)
    };

    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    sendChunk_hybrid(hreq, reply, UINT64_MAX, cv);
    RedisModule_EndReply(reply);
}

static void FreeHybridRequest(void *ptr) {
  HybridRequest_DecrRef((HybridRequest *)ptr);
}

int HybridRequest_StartSingleCursor(StrongRef hybrid_ref, RedisModule_Reply *reply, bool coord) {
    HybridRequest *req = StrongRef_Get(hybrid_ref);
    // We don't have depleters, we will create a single cursor just for the hybrid request
    // This is needed for client facing API, client expects a single cursor id to receive the merged result set
    AREQ *first = req->requests[0];
    Cursor *cursor = Cursors_Reserve(getCursorList(coord), first->sctx->spec->own_ref, first->cursorConfig.maxIdle, &req->tailPipelineError);
    if (!cursor) {
      return REDISMODULE_ERR;
    }
    cursor->hybrid_ref = hybrid_ref;
    RedisModule_Reply_LongLong(reply, cursor->id);;
    return REDISMODULE_OK;
}

static inline void replyWithCursors(RedisModuleCtx *replyCtx, arrayof(Cursor*) cursors,
                                     HybridRequest *hreq, bool timedOut) {
    RedisModule_Reply _reply = RedisModule_NewReply(replyCtx), *reply = &_reply;
    // Send map of cursor IDs as response
    RedisModule_Reply_Map(reply);
    for (size_t i = 0; i < array_len(cursors); i++) {
      Cursor *cursor = cursors[i];
      Cursor_Pause(cursor);
      AREQ *areq = cursor->execState;
      if (IsHybridSearchSubquery(areq)) {
        RedisModule_ReplyKV_LongLong(reply, "SEARCH", cursor->id);
      } else if (IsHybridVectorSubquery(areq)) {
        RedisModule_ReplyKV_LongLong(reply, "VSIM", cursor->id);
      } else {
        RS_ABORT_ALWAYS("Unknown subquery type");
      }
    }
    RedisModule_ReplyKV_Array(reply, "warnings"); // >warnings
    if (timedOut) {
      QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, 1, SHARD_ERR_WARN);
      RedisModule_Reply_SimpleString(reply, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT));
    }
    for (size_t i = 0; i < hreq->nrequests; i++) {
      QueryError *err = &hreq->errors[i];
      if (QueryError_HasReachedMaxPrefixExpansionsWarning(err)) {
        const char *suffix = (i == SEARCH_INDEX) ? SEARCH_SUFFIX : VSIM_SUFFIX;
        char buf[128];
        snprintf(buf, sizeof(buf), "%s %s", QUERY_WMAXPREFIXEXPANSIONS, suffix);
        QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_REACHED_MAX_PREFIX_EXPANSIONS, 1, SHARD_ERR_WARN);
        RedisModule_Reply_SimpleString(reply, buf);
      }
    }
    RedisModule_Reply_ArrayEnd(reply); // ~warnings

    RedisModule_Reply_MapEnd(reply);
    RedisModule_EndReply(reply);
}

int HybridRequest_StartCursors(StrongRef hybrid_ref, RedisModuleCtx *replyCtx, QueryError *status, bool backgroundDepletion) {
    HybridRequest *req = StrongRef_Get(hybrid_ref);
    if (req->nrequests == 0) {
      QueryError_SetError(&req->tailPipelineError, QUERY_ERROR_CODE_GENERIC, "No subqueries in hybrid request");
      return REDISMODULE_ERR;
    }
    // helper array to collect depleters so we can deplete them all at once
    // before returning the cursors
    arrayof(ResultProcessor*) depleters = array_new(ResultProcessor *, req->nrequests);

    // Pause before store cursors (hybrid cursors only)
    debugPauseHybridStoreCursors(req, true);

    // Lock cursor creation to synchronize with timeout callback.
    // This ensures that if timeout fires:
    // 1. Before we create cursors: we'll see timedOut flag and skip creation
    // 2. After we create cursors: timeout callback will free them properly
    HybridRequest_LockCursors(req);

    // Check if we timed out before creating cursors
    if (HybridRequest_TimedOut(req)) {
      HybridRequest_UnlockCursors(req);
      array_free(depleters);
      QueryError_SetError(status, QUERY_ERROR_CODE_TIMED_OUT, NULL);
      return REDISMODULE_ERR;
    }

    req->cursors = array_new(Cursor*, req->nrequests);
    ResultProcessorType expectedDepleterType = backgroundDepletion ? RP_SAFE_DEPLETER : RP_DEPLETER;
    for (size_t i = 0; i < req->nrequests; i++) {
      AREQ *areq = req->requests[i];
      ResultProcessor *depleter = areq->pipeline.qctx.endProc;
      if (IsProfile(req) && depleter->type == RP_PROFILE) {
        depleter = depleter->upstream;
      }
      if (depleter->type != expectedDepleterType) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_GENERIC,
          "Unexpected depleter type: expected %s, got %s",
          RPTypeToString(expectedDepleterType), RPTypeToString(depleter->type));
        break;
      }
      array_ensure_append_1(depleters, depleter);
      Cursor *cursor = Cursors_Reserve(getCursorList(false), areq->sctx->spec->own_ref, areq->cursorConfig.maxIdle, status);
      if (!cursor) {
        break;
      }
      // The cursor lifetime will determine the hybrid request lifetime
      cursor->execState = areq;
      cursor->hybrid_ref = StrongRef_Clone(hybrid_ref);
      areq->cursor_id = cursor->id;
      array_ensure_append_1(req->cursors, cursor);
    }

    if (array_len(req->cursors) != req->nrequests) {
      array_free_ex(req->cursors, Cursor_Free(*(Cursor**)ptr));
      req->cursors = NULL;
      HybridRequest_UnlockCursors(req);
      array_free(depleters);
      // verify error exists
      RS_ASSERT(QueryError_HasError(status));
      return REDISMODULE_ERR;
    }

    int rc;
    if (backgroundDepletion) {
      rc = RPSafeDepleter_DepleteAll(depleters, status);
    } else {
      // Foreground depletion for WORKERS == 0
      // Trigger synchronous depletion to read and buffer all results while the spec lock is held.
      rc = RPDepleter_DepleteAll(depleters);
    }

    array_free(depleters);

    bool depletionTimedOut = false;
    if (rc != RS_RESULT_OK) {
      if (rc == RS_RESULT_TIMEDOUT && req->reqConfig.timeoutPolicy != TimeoutPolicy_Fail) {
        // RETURN policy: keep cursors with partial results, emit warning in reply
        depletionTimedOut = true;
      } else {
        // Fatal error or FAIL policy — free everything
        array_free_ex(req->cursors, Cursor_Free(*(Cursor**)ptr));
        req->cursors = NULL;
        HybridRequest_UnlockCursors(req);
        if (!QueryError_HasError(status)) {
          if (rc == RS_RESULT_TIMEDOUT) {
            QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_TIMED_OUT, "Depleting timed out");
          } else {
            QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_GENERIC, "Failed to deplete set of results, rc=%d", rc);
          }
        }
        return REDISMODULE_ERR;
      }
    }

    HybridRequest_UnlockCursors(req);

    // Pause after store cursors (hybrid cursors only)
    debugPauseHybridStoreCursors(req, false);

    if (!req->useReplyCallback) {
      // If we are not using reply callback, we should reply with the cursors here
      replyWithCursors(replyCtx, req->cursors, req, depletionTimedOut);
      array_free(req->cursors);
      req->cursors = NULL;
    } // else the reply callback will reply with the cursors and free the array

    return REDISMODULE_OK;
}

/*
 * Internal function to build the pipeline and execute the hybrid request.
 * This function is used by both the foreground and background execution paths.
 * @param hreq The HybridRequest to build and execute
 * @param hybridParams The pipeline parameters for building the hybrid request - must be allocated with rm_calloc, freed by this function on success
 * @param ctx Redis module context for sending the reply
 * @param sctx Redis search context
 * @param status Output parameter for error reporting
 * @param internal Whether the request is internal (not exposed to the user)
 * @param depleteInBackground Whether the pipeline should be built for asynchronous depletion
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
*/
static int buildPipelineAndExecute(StrongRef hybrid_ref, HybridPipelineParams *hybridParams,
                                   RedisModuleCtx *ctx, RedisSearchCtx *sctx, QueryError *status,
                                   bool internal, bool depleteInBackground) {
  // Build the pipeline and execute
  HybridRequest *hreq = StrongRef_Get(hybrid_ref);
  hreq->reqflags = hybridParams->aggregationParams.common.reqflags;
  bool isCursor = hreq->reqflags & QEXEC_F_IS_CURSOR;

  // Start measuring pipeline build time if profiling is enabled
  rs_wall_clock pipelineClock;
  const bool isProfile = hreq->tailPipeline->qctx.isProfile;
  if (isProfile) {
    rs_wall_clock_init(&pipelineClock);
  }

  // Internal commands do not have a hybrid merger and only have a depletion pipeline
  if (internal) {
    RS_LOG_ASSERT(isCursor, "Internal hybrid command must be a cursor request from a coordinator");
    isCursor = true;
    if (HybridRequest_BuildDepletionPipeline(hreq, hybridParams, depleteInBackground) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  } else if (HybridRequest_BuildPipeline(hreq, hybridParams, depleteInBackground) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  // Record pipeline build time if profiling is enabled
  if (isProfile) {
    hreq->profileClocks.profilePipelineBuildTime = rs_wall_clock_elapsed_ns(&pipelineClock);
  }

  // Apply debug timeouts after pipeline is built (for _FT.DEBUG FT.HYBRID)
  if (hreq->debugParams && applyHybridDebugTimeout(hreq, hreq->debugParams) != REDISMODULE_OK) {
      QueryError_SetError(status, QUERY_ERROR_CODE_INVAL, "Failed to apply debug timeouts");
      return REDISMODULE_ERR;
  }

  if (!isCursor) {
    HybridRequest_Execute(hreq, ctx, sctx);
  } else if (HybridRequest_StartCursors(hybrid_ref, ctx, status, depleteInBackground) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  freeHybridParams(hybridParams);
  return REDISMODULE_OK;
}

// Timeout callback for HybridRequest execution in Run in Threads mode.
// Called on the main thread when the blocking client times out (FAIL policy only).
// Acquires cursorMutex to synchronize with HybridRequest_StartCursors:
// - If cursors were already created, we free them here
// - If cursors haven't been created yet, StartCursors will see timedOut and skip creation
static int HybridQueryTimeoutFailCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);

  BlockedQueryNode *node = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!node || !node->privdata) {
    // Shouldn't happen, but handle gracefully
    RedisModule_Log(ctx, "warning", "HybridQueryTimeoutFailCallback: no node or privdata");
    QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
    RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));
    return REDISMODULE_OK;
  }

  HybridRequest *hreq = (HybridRequest *)node->privdata;

  // Lock to synchronize with cursor creation in HybridRequest_StartCursors.
  // After setting timedOut, any subsequent cursor creation attempt will be skipped.
  // If cursors were already created, we free them here.
  HybridRequest_LockCursors(hreq);

  // Signal timeout to background thread
  HybridRequest_SetTimedOut(hreq);

  // Free cursors if they were already created
  if (hreq->cursors) {
    array_free_ex(hreq->cursors, Cursor_Free(*(Cursor**)ptr));
    hreq->cursors = NULL;
  }

  HybridRequest_UnlockCursors(hreq);

  // Reply with timeout error
  QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, !IsInternal(hreq->requests[0]));
  RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));

  return REDISMODULE_OK;
}

// Reply callback for AREQ execution in Run in Threads mode (FAIL policy).
// Called on the main thread when the background thread calls UnblockClient.
// For internal hybrid requests (cursor reply)
static int HybridQueryCursorReplyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);

  BlockedQueryNode *node = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!node || !node->privdata) {
    // Shouldn't happen, but handle gracefully
    RedisModule_Log(ctx, "warning", "HybridQueryReplyCallback: no node or privdata");
    RedisModule_ReplyWithError(ctx, "Internal error: no request context");
    return REDISMODULE_OK;
  }

  HybridRequest *req = (HybridRequest *)node->privdata;

  if (QueryError_HasError(&req->storedReplyState.err)) {
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(&req->storedReplyState.err), 1, SHARD_ERR_WARN);
    QueryError_ReplyAndClear(ctx, &req->storedReplyState.err);
    return REDISMODULE_OK;
  }

  // FAIL policy path — timeout would have been handled by HybridQueryTimeoutFailCallback
  replyWithCursors(ctx, req->cursors, req, false);
  array_free(req->cursors);
  req->cursors = NULL;
  return REDISMODULE_OK;
}

// Reply callback for AREQ execution in Run in Threads mode (FAIL policy).
// Called on the main thread when the background thread calls UnblockClient.
// For non-internal hybrid requests (STANDALONE)
static int HybridQueryReplyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);

  BlockedQueryNode *node = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!node || !node->privdata) {
    // Shouldn't happen, but handle gracefully
    RedisModule_Log(ctx, "warning", "HybridQueryReplyCallback: no node or privdata");
    RedisModule_ReplyWithError(ctx, "Internal error: no request context");
    return REDISMODULE_OK;
  }

  HybridRequest *req = (HybridRequest *)node->privdata;

  // Check if results were stored (background thread completed successfully)
  if (!req->storedReplyState.hasStoredResults) {
    // Background thread didn't store results - some early error occurred.
    if (QueryError_HasError(&req->storedReplyState.err)) {
      QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(&req->storedReplyState.err), 1, COORD_ERR_WARN);
      QueryError_ReplyAndClear(ctx, &req->storedReplyState.err);
    } else {
      RedisModule_ReplyWithError(ctx, "Internal error: no results stored");
    }
    return REDISMODULE_OK;
  }

  // Call serializeStoredResults_hybrid to build reply from stored results
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  serializeStoredResults_hybrid(req, reply);
  RedisModule_EndReply(reply);

  return REDISMODULE_OK;

}

// Wrapper for HybridRequest_DecrRef to match BlockedClientFreePrivDataCB signature
static void HybridRequest_DecrRefWrapper(void *privdata) {
  HybridRequest_DecrRef((HybridRequest *)privdata);
}

// Background execution functions implementation
static blockedClientHybridCtx *blockedClientHybridCtx_New(StrongRef hybrid_ref,
                                                   HybridPipelineParams *hybridParams,
                                                   RedisModuleBlockedClient *blockedClient,
                                                   StrongRef spec, bool internal) {
  blockedClientHybridCtx *ret = rm_new(blockedClientHybridCtx);
  ret->hybrid_ref = hybrid_ref;
  ret->blockedClient = blockedClient;
  ret->spec_ref = StrongRef_Demote(spec);
  ret->hybridParams = hybridParams;
  ret->internal = internal;
  return ret;
}

// Build the pipeline and execute
// if result is REDISMODULE_OK, the hreq and hybridParams are freed by the function thread
// otherwise, the caller is responsible for freeing them
static int HybridRequest_BuildPipelineAndExecute(StrongRef hybrid_ref, HybridPipelineParams *hybridParams, RedisModuleCtx *ctx,
                    RedisSearchCtx *sctx, QueryError* status, bool internal) {
  HybridRequest *hreq = StrongRef_Get(hybrid_ref);
  if (RunInThread(ctx)) {
    // Multi-threaded execution path
    StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);

    BlockClientCtx blockClientCtx = {0};

    blockClientCtx.ast = &hreq->requests[0]->ast;
    blockClientCtx.privdata = hreq;
    HybridRequest_IncrRef(hreq);
    blockClientCtx.freePrivData = HybridRequest_DecrRefWrapper;

    if (hreq->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
      blockClientCtx.timeoutCallback = HybridQueryTimeoutFailCallback;
      blockClientCtx.replyCallback = internal ? HybridQueryCursorReplyCallback : HybridQueryReplyCallback;
      blockClientCtx.timeoutMS = hreq->reqConfig.queryTimeoutMS;
      hreq->useReplyCallback = true;
    }

    RedisModuleBlockedClient* blockedClient = BlockQueryClientWithTimeout(ctx, spec_ref, &blockClientCtx);

    blockedClientHybridCtx *BCHCtx = blockedClientHybridCtx_New(StrongRef_Clone(hybrid_ref), hybridParams, blockedClient, spec_ref, internal);

    // Mark the hreq as running in the background
    hreq->reqflags |= QEXEC_F_RUN_IN_BACKGROUND;
    if (hreq->tailPipeline->qctx.isProfile){
      hreq->tailPipeline->qctx.queryGILTime += rs_wall_clock_elapsed_ns(&hreq->profileClocks.initClock);
    }
    // Mark the requests as thread safe, so that the pipeline will be built in a thread safe manner
    for (size_t i = 0; i < hreq->nrequests; i++) {
      AREQ_AddRequestFlags(hreq->requests[i], QEXEC_F_RUN_IN_BACKGROUND);
      AREQ_QueryProcessingCtx(hreq->requests[i])->queryGILTime = hreq->tailPipeline->qctx.queryGILTime;
    }

    const int rc = workersThreadPool_AddWork((redisearch_thpool_proc)HREQ_Execute_Callback, BCHCtx);
    RS_ASSERT(rc == 0);

    return REDISMODULE_OK;
  } else {
    // Single-threaded execution path
    return buildPipelineAndExecute(hybrid_ref, hybridParams, ctx, sctx, status, internal, false);
  }
}

static inline void DefaultCleanup(StrongRef hybrid_ref) {
  StrongRef_Release(hybrid_ref);
  CurrentThread_ClearIndexSpec();
}

// We only want to free the hybrid params in case an error happened
static inline int CleanupAndReplyStatus(RedisModuleCtx *ctx, StrongRef hybrid_ref, HybridPipelineParams *hybridParams, QueryError *status, bool internal) {
    freeHybridParams(hybridParams);
    DefaultCleanup(hybrid_ref);
    // Update global query errors, this path is only used for SA and internal
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(status), 1, !internal);
    return QueryError_ReplyAndClear(ctx, status);
}

void printHybridProfileCoordinator(RedisModule_Reply *reply, void *ctx) {
  // only print the coordinator if we are not internal
  HybridRequest *hreq = ctx;
  if ((hreq->reqflags & QEXEC_F_INTERNAL) != QEXEC_F_INTERNAL) {
    Profile_PrintHybrid(reply, ctx);
  } else {
    RedisModule_Reply_EmptyMap(reply);
  }
}

// output "SEARCH" and "VSIM" profiles grouped together for standalone mode
// Format: {SEARCH: profile, VSIM: profile} - single shard with both profiles
void printHybridProfileShards(RedisModule_Reply *reply, void *ctx) {
  HybridRequest *hreq = ctx;
  // For standalone mode, output as a single shard map containing both SEARCH
  // and VSIM
  RedisModule_Reply_Map(reply);  // Start shard map
  for (size_t i = 0; i < hreq->nrequests; i++) {
    AREQ *areq = hreq->requests[i];
    const char *subqueryType = "N/A";
    if (AREQ_RequestFlags(areq) & QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY) {
      subqueryType = "SEARCH";
    } else if (AREQ_RequestFlags(areq) & QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY) {
      subqueryType = "VSIM";
    }
    RedisModule_Reply_SimpleString(reply, subqueryType);
    Profile_Print(reply, areq);
  }
  RedisModule_Reply_MapEnd(reply);  // End shard map
}

void printHybridProfile(RedisModule_Reply *reply, void *ctx) {
  Profile_PrintInFormat(reply, printHybridProfileShards, ctx, printHybridProfileCoordinator, ctx);
}

// This function should only be called from the main thread (calling RunInThread() is not thread safe)
// HybridRequest execution flags are not set when this function is called currently
static bool shouldCheckInPipelineTimeoutHybrid(RedisModuleCtx* ctx, HybridRequest *hreq) {
  // We should check for timeout in pipeline only if timeout is > 0
  // and when the policy is RETURN or the policy is FAIL, without workers.
  return hreq->reqConfig.queryTimeoutMS > 0 &&
         (hreq->reqConfig.timeoutPolicy == TimeoutPolicy_Return || !RunInThread(ctx));

}

/**
 * Main command handler for FT.HYBRID command.
 *
 * Parses command arguments, builds hybrid request structure, constructs execution pipeline,
 * and prepares for hybrid search execution.
 *
 * This method does not take ownership of `debugParams`.
 */
int hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool internal,
                         ProfileOptions profileOptions, const HybridDebugParams *debugParams) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (SearchDisk_MarkUnsupportedCommandIfDiskEnabled(ctx, "FT.HYBRID")) {
    return REDISMODULE_OK;
  }
  QueryError status = QueryError_Default();

  // Memory guardrail
  if (QueryMemoryGuard(ctx)) {
    if (RSGlobalConfig.requestConfigParams.oomPolicy == OomPolicy_Fail) {
      QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_OUT_OF_MEMORY, 1, !internal);
      return QueryMemoryGuardFailure_WithReply(ctx);
    }
    // Assuming OOM policy is return since we didn't ignore the memory guardrail
    RS_ASSERT(RSGlobalConfig.requestConfigParams.oomPolicy == OomPolicy_Return);
    return common_hybrid_query_reply_empty(ctx, QUERY_ERROR_CODE_OUT_OF_MEMORY, internal,
                                           profileOptions & EXEC_WITH_PROFILE);
  }

  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError_SetWithUserDataFmt(&status, QUERY_ERROR_CODE_NO_INDEX, "Index not found", ": %s", indexname);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);
  CurrentThread_SetIndexSpec(spec_ref);

  HybridRequest *hybridRequest = MakeDefaultHybridRequest(sctx);
  hybridRequest->profile = printHybridProfile;
  hybridRequest->tailPipeline->qctx.isProfile = profileOptions & EXEC_WITH_PROFILE;
  if (debugParams) {
    hybridRequest->debugParams = rm_malloc(sizeof(*debugParams));
    *hybridRequest->debugParams = *debugParams;
  }
  StrongRef hybrid_ref = StrongRef_New(hybridRequest, &FreeHybridRequest);

  ParseHybridCommandCtx cmd = {0};
  cmd.search = hybridRequest->requests[SEARCH_INDEX];
  cmd.vector = hybridRequest->requests[VECTOR_INDEX];
  cmd.reqConfig = &hybridRequest->reqConfig;
  cmd.cursorConfig = &hybridRequest->cursorConfig;
  cmd.hybridParams = rm_calloc(1, sizeof(HybridPipelineParams));
  cmd.tailPlan = &hybridRequest->tailPipeline->ap;
  cmd.coordDispatchTime = &hybridRequest->profileClocks.coordDispatchTime;

  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hybridRequest, &ac, argv, argc);

  if (parseHybridCommand(ctx, &ac, sctx, &cmd, &status, internal, profileOptions) != REDISMODULE_OK) {
    return CleanupAndReplyStatus(ctx, hybrid_ref, cmd.hybridParams, &status, internal);
  }

  if (internal) {
    if (RequestConfig_ApplyCoordinatorElapsedTime(
            &hybridRequest->reqConfig, hybridRequest->profileClocks.coordDispatchTime)) {
      freeHybridParams(cmd.hybridParams);
      DefaultCleanup(hybrid_ref);
      return replyForHybridPreExecutionTimeout(ctx, internal, profileOptions);
    }
    // Propagate adjusted timeout to sub-queries
    for (size_t i = 0; i < hybridRequest->nrequests; i++) {
      hybridRequest->requests[i]->reqConfig.queryTimeoutMS = hybridRequest->reqConfig.queryTimeoutMS;
    }
  }

  // Check if we should check for timeout in pipeline
  HybridRequest_SetSkipTimeoutChecks(hybridRequest, !shouldCheckInPipelineTimeoutHybrid(ctx, hybridRequest));

  // Copy dispatch time to each subquery AREQ for profile printing
  for (size_t i = 0; i < hybridRequest->nrequests; i++) {
    hybridRequest->requests[i]->profileClocks.coordDispatchTime = hybridRequest->profileClocks.coordDispatchTime;
  }

  if (profileOptions != EXEC_NO_FLAGS) {
    hybridRequest->profileClocks.profileParseTime = rs_wall_clock_elapsed_ns(&hybridRequest->profileClocks.initClock);
  }

  // Initialize timeout for all subqueries BEFORE building pipelines
  for (int i = 0; i < hybridRequest->nrequests; i++) {
    AREQ *subquery = hybridRequest->requests[i];
    SearchCtx_UpdateTime(AREQ_SearchCtx(subquery), hybridRequest->reqConfig.queryTimeoutMS);
  }
  SearchCtx_UpdateTime(hybridRequest->sctx, hybridRequest->reqConfig.queryTimeoutMS);

  if (HybridRequest_BuildPipelineAndExecute(hybrid_ref, cmd.hybridParams, ctx, hybridRequest->sctx, &status, internal) != REDISMODULE_OK) {
    HybridRequest_GetError(hybridRequest, &status);
    HybridRequest_ClearErrors(hybridRequest);
    return CleanupAndReplyStatus(ctx, hybrid_ref, cmd.hybridParams, &status, internal);
  }

  // Update dialect statistics only after successful execution
  SET_DIALECT(sctx->spec->used_dialects, hybridRequest->reqConfig.dialectVersion);
  SET_DIALECT(RSGlobalStats.totalStats.used_dialects, hybridRequest->reqConfig.dialectVersion);

  DefaultCleanup(hybrid_ref);
  return REDISMODULE_OK;
}

/**
 * Destroy a blocked client hybrid context and clean up resources.
 *
 * @param BCHCtx The blocked client context to destroy
 */
static void blockedClientHybridCtx_destroy(blockedClientHybridCtx *BCHCtx) {
  StrongRef_Release(BCHCtx->hybrid_ref);
  freeHybridParams(BCHCtx->hybridParams);
  RedisModule_BlockedClientMeasureTimeEnd(BCHCtx->blockedClient);
  void *privdata = RedisModule_BlockClientGetPrivateData(BCHCtx->blockedClient);
  RedisModule_UnblockClient(BCHCtx->blockedClient, privdata);
  WeakRef_Release(BCHCtx->spec_ref);
  rm_free(BCHCtx);
}

/**
 * Background execution callback for hybrid requests.
 * This function is called by the worker thread to execute hybrid requests.
 *
 * @param BCHCtx The blocked client context containing the request
 */
static void HREQ_Execute_Callback(blockedClientHybridCtx *BCHCtx) {
  StrongRef hybrid_ref = BCHCtx->hybrid_ref;
  HybridRequest *hreq = StrongRef_Get(hybrid_ref);
  HybridPipelineParams *hybridParams = BCHCtx->hybridParams;
  RedisModuleCtx *outctx = RedisModule_GetThreadSafeContext(BCHCtx->blockedClient);
  QueryError status = QueryError_Default();

  StrongRef execution_ref = IndexSpecRef_Promote(BCHCtx->spec_ref);
  if (!StrongRef_Get(execution_ref)) {
    // The index was dropped while the query was in the job queue.
    // Notify the client that the query was aborted
    QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
    HREQ_ReplyOrStoreError(hreq, outctx, &status);
    RedisModule_FreeThreadSafeContext(outctx);
    blockedClientHybridCtx_destroy(BCHCtx);
    return;
  }

  RedisSearchCtx *sctx = HREQ_SearchCtx(hreq);
  if (!(hreq->reqflags & QEXEC_F_IS_CURSOR)) {
    // Update the main search context with the thread-safe context
    sctx->redisCtx = outctx;
  }

  // Acquire read lock before building pipeline (matching AREQ_Execute_Callback)
  RedisSearchCtx_LockSpecRead(sctx);

  if (buildPipelineAndExecute(hybrid_ref, hybridParams, outctx, sctx, &status, BCHCtx->internal, true) == REDISMODULE_OK) {
    // Set hybridParams to NULL so they won't be freed in destroy
    BCHCtx->hybridParams = NULL;
    RedisSearchCtx_UnlockSpec(sctx);
  } else {
    // buildPipelineAndExecute failed - release the lock if still held.
    // Note: If failure occurred after RPSafeDepleter_DepleteAll started, the lock
    // was already released in WaitForDepletionToStart. RedisSearchCtx_UnlockSpec
    // safely handles this case by checking sctx->flags before unlocking.
    RedisSearchCtx_UnlockSpec(sctx);
    if (!QueryError_HasError(&status)) {
      // There was an error but it was not set in status, get it from hreq
      HybridRequest_GetError(hreq, &status);
      HybridRequest_ClearErrors(hreq);
    }
    HREQ_ReplyOrStoreError(hreq, outctx, &status);
  }

  RedisModule_FreeThreadSafeContext(outctx);
  IndexSpecRef_Release(execution_ref);
  blockedClientHybridCtx_destroy(BCHCtx);
}
