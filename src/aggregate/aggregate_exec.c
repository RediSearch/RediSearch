/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "redismodule.h"
#include "redisearch.h"
#include "search_ctx.h"
#include "aggregate.h"
#include "aggregate_exec_common.h"
#include "cursor.h"
#include "rmutil/util.h"
#include "util/timeout.h"
#include "util/workers.h"
#include "score_explain.h"
#include "profile/profile.h"
#include "query_optimizer.h"
#include "resp3.h"
#include "query_error.h"
#include "info/global_stats.h"
#include "aggregate_debug.h"
#include "debug_commands.h"
#include "info/info_redis/block_client.h"
#include "info/info_redis/types/blocked_queries.h"
#include "info/info_redis/threads/current_thread.h"
#include "pipeline/pipeline.h"
#include "util/units.h"
#include "hybrid/hybrid_request.h"
#include "module.h"
#include "result_processor.h"
#include "profile/options.h"
#include "reply_empty.h"

// Multi threading data structure for background query execution.
// This context is created on the main thread and passed to the background worker.
// Ownership: The main thread transfers its AREQ reference (from AREQ_New) to this context.
typedef struct {
  AREQ *req;  // Owns transferred reference from main thread.
  RedisModuleBlockedClient *blockedClient;
  WeakRef spec_ref;
} blockedClientReqCtx;

static void runCursor(RedisModule_Reply *reply, Cursor *cursor, size_t num);
static int prepareExecutionPlan(AREQ *req, QueryError *status);
static int QueryReplyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/**
 * Get the sorting key of the result. This will be the sorting key of the last
 * RLookup registry. Returns NULL if there is no sorting key
 */
static const RSValue *getReplyKey(const RLookupKey *kk, const SearchResult *r) {
  const RSSortingVector* sv = RLookupRow_GetSortingVector(SearchResult_GetRowData(r));
  if ((RLookupKey_GetFlags(kk) & RLOOKUP_F_SVSRC) && (sv && RSSortingVector_Length(sv) > RLookupKey_GetSvIdx(kk))) {
    return RSSortingVector_Get(sv, RLookupKey_GetSvIdx(kk));
  } else {
    return RLookupRow_Get(kk, SearchResult_GetRowData(r));
  }
}



static void reeval_key(RedisModule_Reply *reply, const RSValue *key) {
  RedisModuleCtx *outctx = reply->ctx;
  RedisModuleString *rskey = NULL;
  if (!key) {
    RedisModule_Reply_Null(reply);
  }
  else {
    if (RSValue_IsReference(key)) {
      key = RSValue_Dereference(key);
    } else if (RSValue_IsTrio(key)) {
      key = RSValue_Trio_GetLeft(key);
    }

    switch (RSValue_Type(key)) {
      case RSValueType_Number:
        // Serialize double - by prepending "#" to the number, so the coordinator/client can
        // tell it's a double and not just a numeric string value
        rskey = RedisModule_CreateStringPrintf(outctx, "#%.17g", RSValue_Number_Get(key));
        break;
      case RSValueType_String:
        // Serialize string - by prepending "$" to it
        rskey = RedisModule_CreateStringPrintf(outctx, "$%s", RSValue_String_Get(key, NULL));
        break;
      case RSValueType_RedisString:
        rskey = RedisModule_CreateStringPrintf(outctx, "$%s",
          RedisModule_StringPtrLen(RSValue_RedisString_Get(key), NULL));
        break;
      case RSValueType_Null:
      case RSValueType_Undef:
      case RSValueType_Array:
      case RSValueType_Map:
      case RSValueType_Reference:
      case RSValueType_Trio:
        break;
    }

    if (rskey) {
      RedisModule_Reply_String(reply, rskey);
      RedisModule_FreeString(outctx, rskey);
    } else {
      RedisModule_Reply_Null(reply);
    }
  }
}

static size_t serializeResult(AREQ *req, RedisModule_Reply *reply, const SearchResult *r,
                              const cachedVars *cv) {
  const uint32_t options = AREQ_RequestFlags(req);
  const RSDocumentMetadata *dmd = SearchResult_GetDocumentMetadata(r);
  size_t count0 = RedisModule_Reply_LocalCount(reply);
  bool has_map = RedisModule_IsRESP3(reply);

  if (has_map) {
    RedisModule_Reply_Map(reply);
  }

  if (options & QEXEC_F_IS_SEARCH) {
    size_t n;
    RS_LOG_ASSERT(dmd, "Document metadata NULL in result serialization.");
    if (!dmd) {
      // Empty results should not be serialized!
      // We already crashed in development env. In production, log and continue
      RedisModule_Log(AREQ_SearchCtx(req)->redisCtx, "warning", "Document metadata NULL in result serialization.");
      return 0;
    }
    const char *s = DMD_KeyPtrLen(dmd, &n);
    if (has_map) {
      RedisModule_ReplyKV_StringBuffer(reply, "id", s, n);
    } else {
      RedisModule_Reply_StringBuffer(reply, s, n);
    }
  }

  if (options & QEXEC_F_SEND_SCORES) {
    if (has_map) {
      RedisModule_Reply_SimpleString(reply, "score");
    }
    if (!(options & QEXEC_F_SEND_SCOREEXPLAIN)) {
      RedisModule_Reply_Double(reply, SearchResult_GetScore(r));
    } else {
      RedisModule_Reply_Array(reply);
      RedisModule_Reply_Double(reply, SearchResult_GetScore(r));
      SEReply(reply, SearchResult_GetScoreExplain(r));
      RedisModule_Reply_ArrayEnd(reply);
    }
  }

  if (options & QEXEC_F_SENDRAWIDS) {
    if (has_map) {
      RedisModule_ReplyKV_LongLong(reply, "id", SearchResult_GetDocId(r));
    } else {
      RedisModule_Reply_LongLong(reply, SearchResult_GetDocId(r));
    }
  }

  if (options & QEXEC_F_SEND_PAYLOADS) {
    if (has_map) {
      RedisModule_Reply_SimpleString(reply, "payload");
    }
    if (dmd && hasPayload(dmd->flags)) {
      RedisModule_Reply_StringBuffer(reply, dmd->payload->data, dmd->payload->len);
    } else {
      RedisModule_Reply_Null(reply);
    }
  }

  // Coordinator only - sortkey will be sent on the required fields.
  // Non Coordinator modes will require this condition.
  if ((options & QEXEC_F_SEND_SORTKEYS)) {
    if (has_map) {
      RedisModule_Reply_SimpleString(reply, "sortkey");
    }
    const RSValue *sortkey = NULL;
    if (cv->lastAstp && cv->lastAstp->sortkeysLK) {
      const RLookupKey *kk = cv->lastAstp->sortkeysLK[0];
      sortkey = getReplyKey(kk, r);
    }
    reeval_key(reply, sortkey);
  }

  // Coordinator only - handle required fields for coordinator request
  if (options & QEXEC_F_REQUIRED_FIELDS) {

    // Sortkey is the first key to reply on the required fields, if we already replied it, continue to the next one.
    size_t currentField = options & QEXEC_F_SEND_SORTKEYS ? 1 : 0;
    size_t requiredFieldsCount = array_len(req->requiredFields);
    RSValue *rsv = NULL;
    bool need_map = has_map && currentField < requiredFieldsCount;
    if (need_map) {
      RedisModule_ReplyKV_Map(reply, "required_fields"); // >required_fields
    }
    for(; currentField < requiredFieldsCount; currentField++) {
      const RLookupKey *rlk = RLookup_GetKey_Read(cv->lastLookup, req->requiredFields[currentField], RLOOKUP_F_NOFLAGS);
      const RSValue *v = rlk ? getReplyKey(rlk, r) : NULL;
      if (RSValue_IsTrio(v)) {
        // For duo value, we use the left value here (not the right value)
        v = RSValue_Trio_GetLeft(v);
      }
      if (rlk && (RLookupKey_GetFlags(rlk) & RLOOKUP_F_NUMERIC) && v && !RSValue_IsNumber(v) && !RSValue_IsNull(v)) {
        double d;
        RSValue_ToNumber(v, &d);
        if (rsv == NULL) {
          rsv = RSValue_NewNumber(d);
        } else {
          RSValue_SetNumber(rsv, d);
        }
        v = rsv;
      }
      if (need_map) {
        RedisModule_Reply_CString(reply, req->requiredFields[currentField]); // key name
      }
      reeval_key(reply, v);
    }
    if (need_map) {
      RedisModule_Reply_MapEnd(reply); // >required_fields
    }
    if (rsv) {
      RSValue_DecrRef(rsv);
      rsv = NULL;
    }
  }

  if (!(options & QEXEC_F_SEND_NOFIELDS)) {
    const RLookup *lk = cv->lastLookup;
    if (has_map) {
      RedisModule_Reply_SimpleString(reply, "extra_attributes");
    }

    if (SearchResult_GetFlags(r) & Result_ExpiredDoc) {
      RedisModule_Reply_Null(reply);
    } else {
      // Get the number of fields in the reply.
      // Excludes hidden fields, fields not included in RETURN and, score and language fields.
      RedisSearchCtx *sctx = AREQ_SearchCtx(req);
      SchemaRule *rule = (sctx && sctx->spec) ? sctx->spec->rule : NULL;
      uint32_t excludeFlags = RLOOKUP_F_HIDDEN;
      uint32_t requiredFlags = (req->outFields.explicitReturn ? RLOOKUP_F_EXPLICITRETURN : 0);
      size_t skipFieldIndex_len = RLookup_GetRowLen(lk);
      bool skipFieldIndex[skipFieldIndex_len]; // After calling `RLookup_GetLength` will contain `false` for fields which we should skip below
      memset(skipFieldIndex, 0, skipFieldIndex_len * sizeof(*skipFieldIndex));
      size_t nfields = RLookup_GetLength(lk, SearchResult_GetRowData(r), skipFieldIndex, skipFieldIndex_len, requiredFlags, excludeFlags, rule);

      RedisModule_Reply_Map(reply);
      int i = 0;
      RLOOKUP_FOREACH(kk, lk, {
        if (!RLookupKey_GetName(kk) || !skipFieldIndex[i++]) {
          continue;
        }
        const RSValue *v = RLookupRow_Get(kk, SearchResult_GetRowData(r));
        RS_LOG_ASSERT(v, "v was found in RLookup_GetLength iteration")

        RedisModule_Reply_StringBuffer(reply, RLookupKey_GetName(kk), RLookupKey_GetNameLen(kk));

        QEFlags reqFlags = AREQ_RequestFlags(req);
        SendReplyFlags flags = (reqFlags & QEXEC_F_TYPED) ? SENDREPLY_FLAG_TYPED : 0;
        flags |= (reqFlags & QEXEC_FORMAT_EXPAND) ? SENDREPLY_FLAG_EXPAND : 0;

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
    RedisModule_Reply_MapEnd(reply);
  }
}

  if (has_map) {
    // placeholder for fields_values. (possible optimization)
    RedisModule_Reply_SimpleString(reply, "values");
    RedisModule_Reply_EmptyArray(reply);

    RedisModule_Reply_MapEnd(reply);
  }

  return RedisModule_Reply_LocalCount(reply) - count0;
}

static size_t getResultsFactor(AREQ *req) {
  size_t count = 0;
  QEFlags reqFlags = AREQ_RequestFlags(req);

  if (reqFlags & QEXEC_F_IS_SEARCH) {
    count++;
  }

  if (reqFlags & QEXEC_F_SEND_SCORES) {
    count++;
  }

  if (reqFlags & QEXEC_F_SENDRAWIDS) {
    count++;
  }

  if (reqFlags & QEXEC_F_SEND_PAYLOADS) {
    count++;
  }

  if (reqFlags & QEXEC_F_SEND_SORTKEYS) {
    count++;
  }

  if (reqFlags & QEXEC_F_REQUIRED_FIELDS) {
    count += array_len(req->requiredFields);
    if (reqFlags & QEXEC_F_SEND_SORTKEYS) {
      count--;
    }
  }

  if (!(reqFlags & QEXEC_F_SEND_NOFIELDS)) {
    count++;
  }
  return count;
}

static void startPipeline(AREQ *req, ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc) {
  CommonPipelineCtx ctx = {
    .timeoutPolicy = req->reqConfig.timeoutPolicy,
    .timeout = &req->sctx->time.timeout,
    .oomPolicy = req->reqConfig.oomPolicy,
    .skipTimeoutChecks = req->sctx->time.skipTimeoutChecks,
  };
  startPipelineCommon(&ctx, rp, results, r, rc);
}

#ifdef ENABLE_ASSERT
// Helper function to pause before/after store results (for testing timeout during store)
static inline void debugPauseStoreResults(AREQ *req, bool before) {
  bool enabled = before ? StoreResultsDebugCtx_IsPauseBeforeEnabled()
                        : StoreResultsDebugCtx_IsPauseAfterEnabled();
  if (enabled) {
    StoreResultsDebugCtx_SetPause(true);
    while (StoreResultsDebugCtx_IsPaused()) {
      // Check if timed out - break to avoid deadlock with timeout callback
      if (AREQ_TimedOut(req)) {
        StoreResultsDebugCtx_SetPause(false);
        break;
      }
      usleep(1000);  // Spin-wait with 1ms sleep
    }
  }
}
#else
// Compiler eliminates the function completely in release builds - zero overhead
static inline void debugPauseStoreResults(AREQ *req, bool before) {
  UNUSED(req);
  UNUSED(before);
}
#endif

/**
 * Store pipeline results for reply_callback path.
 * Called after startPipeline when using reply_callback mode (FAIL policy with workers).
 * Stores results in req->storedReplyState so serializeAndReplyResults can be called
 * from the reply_callback on the main thread.
 *
 * @param req The aggregate request
 * @param results Pipeline results (ownership transferred to storedReplyState)
 * @param rc Pipeline return code
 * @param cv Cached variables for result serialization
 */
static void AREQ_StoreResults(AREQ *req, SearchResult **results, int rc, cachedVars cv) {
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);

  // Store results in AREQ for reply_callback to use
  req->storedReplyState.results = results;
  req->storedReplyState.rc = rc;
  req->storedReplyState.cv = cv;
  req->storedReplyState.hasStoredResults = true;

  // Deep copy error state since qctx->err points to a local variable in the caller
  // which will go out of scope. QueryError contains heap-allocated strings.
  QueryError_ClearError(&req->storedReplyState.err);
  QueryError_CloneFrom(qctx->err, &req->storedReplyState.err);
}

static int populateReplyWithResults(RedisModule_Reply *reply,
  SearchResult **results, AREQ *req, cachedVars *cv) {
    // populate the reply with an array containing the serialized results
    int len = array_len(results);
    array_foreach(results, res, {
      serializeResult(req, reply, res, cv);
      SearchResult_Destroy(res);
      rm_free(res);
    });
    array_free(results);
    return len;
}

long calc_results_len(AREQ *req, size_t limit) {
  long resultsLen;
  PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(AREQ_AGGPlan(req));
  size_t reqLimit = arng && arng->isLimited ? arng->limit : DEFAULT_LIMIT;
  size_t reqOffset = arng && arng->isLimited ? arng->offset : 0;
  size_t resultFactor = getResultsFactor(req);

  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
  size_t expected_res = ((reqLimit + reqOffset) <= req->maxSearchResults) ? qctx->totalResults : MIN(req->maxSearchResults, qctx->totalResults);
  size_t reqResults = expected_res > reqOffset ? expected_res - reqOffset : 0;

  return 1 + MIN(limit, MIN(reqLimit, reqResults)) * resultFactor;
}

static void finishSendChunk(AREQ *req, SearchResult **results, SearchResult *r, bool cursor_done) {
  if (results) {
    destroyResults(results);
  } else {
    SearchResult_Destroy(r);
  }

  if (cursor_done) {
    req->stateflags |= QEXEC_S_ITERDONE;
  }

  rs_wall_clock_ns_t duration = rs_wall_clock_elapsed_ns(&req->profileClocks.initClock);
  // Accumulate profile time for intermediate cursor reads (final read is added in Profile_Print)
  if (IsProfile(req) && !cursor_done && (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR)) {
    req->profileClocks.profileTotalTime += duration;
  }

  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
  if (QueryError_IsOk(qctx->err) || hasTimeoutError(qctx->err)) {
    TotalGlobalStats_CountQuery(AREQ_RequestFlags(req), duration);
  }

  // Reset the total results length:
  qctx->totalResults = 0;
  QueryError_ClearError(qctx->err);
}

/**
 * State for chunk serialization, shared by RESP2 and RESP3 implementations.
 */
typedef struct {
  SearchResult **results;   // Aggregated results (for ON_TIMEOUT FAIL policy)
  SearchResult *r;          // Current result being processed
  long nelem;               // Number of elements sent (RESP2 only)
  long resultsLen;          // Expected results length for assertion (RESP2 only)
  bool cursor_done;         // Whether the cursor is done
} ChunkSerializeState;

/**
 * Handles error/timeout checking and sends error reply if needed.
 * Returns true if an error was sent (caller should skip to cleanup).
 * Shared by both RESP2 and RESP3 implementations.
 */
static bool handleSendChunkError(AREQ *req, RedisModule_Reply *reply,
  QueryProcessingCtx *qctx, int rc) {
  if (ShouldReplyWithError(QueryError_GetCode(qctx->err), req->reqConfig.timeoutPolicy, IsProfile(req))) {
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(qctx->err), 1, !IsInternal(req));
    RedisModule_Reply_Error(reply, QueryError_GetUserError(qctx->err));
    return true;
  } else if (ShouldReplyWithTimeoutError(rc, req->reqConfig.timeoutPolicy, IsProfile(req))) {
    QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, !IsInternal(req));
    ReplyWithTimeoutError(reply);
    return true;
  }
  return false;
}

/**
 * Sets up resultsLen, updates optimizer, and prepares reply arrays.
 * Returns the calculated resultsLen value.
 */
static long prepareSendChunkReply_Resp2(AREQ *req, RedisModule_Reply *reply,
  QueryProcessingCtx *qctx, int rc, size_t limit) {
  long resultsLen = REDISMODULE_POSTPONED_ARRAY_LEN;

  if (rc == RS_RESULT_ERROR) {
    resultsLen = 2;
  } else if (AREQ_RequestFlags(req) & QEXEC_F_IS_SEARCH && rc != RS_RESULT_TIMEDOUT &&
             req->optimizer->type != Q_OPT_NO_SORTER) {
    resultsLen = calc_results_len(req, limit);
  }

  if (IsOptimized(req)) {
    QOptimizer_UpdateTotalResults(req);
  }

  // Upon `FT.PROFILE` commands, embed the response inside another map
  if (IsProfile(req)) {
    Profile_PrepareMapForReply(reply);
  } else if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
    RedisModule_Reply_Array(reply);
  }

  RedisModule_Reply_Array(reply);
  RedisModule_Reply_LongLong(reply, qctx->totalResults);

  return resultsLen;
}

/**
 * Tracks warnings in global statistics and profile context.
 */
static void trackWarnings_Resp2(AREQ *req, QueryProcessingCtx *qctx, int rc) {
  bool has_timedout = (rc == RS_RESULT_TIMEDOUT) || hasTimeoutError(qctx->err);
  if (has_timedout) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, 1, !IsInternal(req));
    ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_TIMEOUT);
  }
  if (QueryError_HasQueryOOMWarning(qctx->err)) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD, 1, !IsInternal(req));
    ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_QUERY_OOM);
  }
  if (QueryError_HasReachedMaxPrefixExpansionsWarning(qctx->err)) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_REACHED_MAX_PREFIX_EXPANSIONS, 1, !IsInternal(req));
    ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_MAX_PREFIX_EXPANSIONS);
  }
  if (req->stateflags & QEXEC_S_ASM_TRIMMING_DELAY_TIMEOUT) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_ASM_INACCURATE_RESULTS, 1, !IsInternal(req));
    ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_ASM_INACCURATE_RESULTS);
  }

  RedisSearchCtx *sctx = AREQ_SearchCtx(req);
  if (sctx->spec && sctx->spec->scan_failed_OOM) {
    ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_BG_SCAN_OOM);
  }
}

/**
 * Finishes chunk reply by handling cursor ID and profile info.
 */
static void finishSendChunkReply_Resp2(AREQ *req, RedisModule_Reply *reply, bool cursor_done) {
  if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
    if (cursor_done) {
      RedisModule_Reply_LongLong(reply, 0);
      if (IsProfile(req)) {
        req->profile(reply, req);
      }
    } else {
      RedisModule_Reply_LongLong(reply, req->cursor_id);
      if (IsProfile(req)) {
        // If the cursor is still alive, don't print profile info to save bandwidth
        RedisModule_Reply_Null(reply);
      }
    }
    RedisModule_Reply_ArrayEnd(reply);
  } else if (IsProfile(req)) {
    req->profile(reply, req);
    RedisModule_Reply_ArrayEnd(reply);
  }
}

/**
 * Serializes results and handles the main reply logic for RESP2.
 * Returns the final rc value and updates state accordingly.
 */
static int serializeAndReplyResults_Resp2(AREQ *req, RedisModule_Reply *reply, ResultProcessor *rp,
  QueryProcessingCtx *qctx, int rc, size_t limit, cachedVars *cv, ChunkSerializeState *state) {

    // If an error occurred, or a timeout in strict mode - return a simple error
    if (handleSendChunkError(req, reply, qctx, rc)) {
      state->cursor_done = true;
      return rc;
    }

    state->resultsLen = prepareSendChunkReply_Resp2(req, reply, qctx, rc, limit);
    state->nelem++;

    // Once we get here, we want to return the results we got from the pipeline (with no error)
    if (AREQ_RequestFlags(req) & QEXEC_F_NOROWS || (rc != RS_RESULT_OK && rc != RS_RESULT_EOF)) {
      goto done_2;
    }

    // If the policy is `ON_TIMEOUT FAIL`, we already aggregated the results
    if (state->results != NULL) {
      state->nelem += populateReplyWithResults(reply, state->results, req, cv);
      state->results = NULL;
      goto done_2;
    }

    if (rp->parent->resultLimit && rc == RS_RESULT_OK) {
      state->nelem += serializeResult(req, reply, state->r, cv);
      SearchResult_Clear(state->r);
    } else {
      goto done_2;
    }

    while (--rp->parent->resultLimit && (rc = rp->Next(rp, state->r)) == RS_RESULT_OK) {
      state->nelem += serializeResult(req, reply, state->r, cv);
      SearchResult_Clear(state->r);
    }

done_2:
    RedisModule_Reply_ArrayEnd(reply);    // </results>

    // Assert that timeout only occurs when skipTimeoutChecks is false (if not in debug)
    RS_ASSERT(!(rc == RS_RESULT_TIMEDOUT) || !req->skipTimeoutChecks || IsDebug(req));

    state->cursor_done = (rc != RS_RESULT_OK
                          && !(rc == RS_RESULT_TIMEDOUT
                               && req->reqConfig.timeoutPolicy == TimeoutPolicy_Return));

    trackWarnings_Resp2(req, qctx, rc);
    finishSendChunkReply_Resp2(req, reply, state->cursor_done);

    return rc;
}

/**
 * Sends a chunk of <n> rows in the resp2 format
 */
static void sendChunk_Resp2(AREQ *req, RedisModule_Reply *reply, size_t limit,
  cachedVars cv) {
    SearchResult r = SearchResult_New();
    int rc = RS_RESULT_EOF;
    QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
    ResultProcessor *rp = qctx->endProc;

    ChunkSerializeState state = {
      .results = NULL,
      .r = &r,
      .nelem = 0,
      .resultsLen = REDISMODULE_POSTPONED_ARRAY_LEN,
      .cursor_done = false
    };

    startPipeline(req, rp, &state.results, &r, &rc);

    if (req->useReplyCallback) {
      // Store results for reply_callback (includes cv)
      debugPauseStoreResults(req, true);  // pause before
      AREQ_StoreResults(req, state.results, rc, cv);
      debugPauseStoreResults(req, false); // pause after
      return;
    }

    rc = serializeAndReplyResults_Resp2(req, reply, rp, qctx, rc, limit, &cv, &state);

    finishSendChunk(req, state.results, &r, state.cursor_done);

    if (state.resultsLen != REDISMODULE_POSTPONED_ARRAY_LEN && rc == RS_RESULT_OK && state.resultsLen != state.nelem) {
      RS_LOG_ASSERT_FMT(false, "Failed to predict the number of replied results. Prediction=%ld, actual_number=%ld.", state.resultsLen, state.nelem);
    }
}

static void _replyWarnings(AREQ *req, RedisModule_Reply *reply, int rc) {
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
  RedisSearchCtx *sctx = AREQ_SearchCtx(req);
  ProfilePrinterCtx *profileCtx = &req->profileCtx;
  RedisModule_ReplyKV_Array(reply, "warning"); // >warnings
  // qctx->bgScanOOM for coordinator, sctx->spec->scan_failed_OOM for shards
  if ((qctx->bgScanOOM)||(sctx->spec && sctx->spec->scan_failed_OOM)) {
    RedisModule_Reply_SimpleString(reply, QUERY_WINDEXING_FAILURE);
    ProfileWarnings_Add(&profileCtx->warnings, PROFILE_WARNING_TYPE_BG_SCAN_OOM);
  }
  if (QueryError_HasQueryOOMWarning(qctx->err)) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD, 1, !IsInternal(req));
    // We use the cluster warning since shard level warning sent via empty reply bailout
    RedisModule_Reply_SimpleString(reply, QUERY_WOOM_COORD);
    ProfileWarnings_Add(&profileCtx->warnings, PROFILE_WARNING_TYPE_QUERY_OOM);
  }
  if (rc == RS_RESULT_TIMEDOUT) {
    // Track warnings in global statistics
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, 1, !IsInternal(req));
    RedisModule_Reply_SimpleString(reply, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT));
    ProfileWarnings_Add(&profileCtx->warnings, PROFILE_WARNING_TYPE_TIMEOUT);
  } else if (rc == RS_RESULT_ERROR) {
    // Non-fatal error
    RedisModule_Reply_SimpleString(reply, QueryError_GetUserError(qctx->err));
  }
  if (QueryError_HasReachedMaxPrefixExpansionsWarning(qctx->err)) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_REACHED_MAX_PREFIX_EXPANSIONS, 1, !IsInternal(req));
    RedisModule_Reply_SimpleString(reply, QUERY_WMAXPREFIXEXPANSIONS);
    ProfileWarnings_Add(&profileCtx->warnings, PROFILE_WARNING_TYPE_MAX_PREFIX_EXPANSIONS);
  }
  if (req->stateflags & QEXEC_S_ASM_TRIMMING_DELAY_TIMEOUT) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_ASM_INACCURATE_RESULTS, 1, !IsInternal(req));
    RedisModule_Reply_SimpleString(reply, QUERY_ASM_INACCURATE_RESULTS);
    ProfileWarnings_Add(&profileCtx->warnings, PROFILE_WARNING_TYPE_ASM_INACCURATE_RESULTS);
  }
  RedisModule_Reply_ArrayEnd(reply); // >warnings
}

/**
 * Prepares reply structure for RESP3 format.
 */
static void prepareSendChunkReply_Resp3(AREQ *req, RedisModule_Reply *reply) {
  if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
    RedisModule_Reply_Array(reply);
  }

  RedisModule_Reply_Map(reply);

  if (IsProfile(req)) {
    Profile_PrepareMapForReply(reply);
  }

  if (IsOptimized(req)) {
    QOptimizer_UpdateTotalResults(req);
  }

  // <attributes>
  RedisModule_ReplyKV_Array(reply, "attributes");
  RedisModule_Reply_ArrayEnd(reply);

  // <format>
  if (AREQ_RequestFlags(req) & QEXEC_FORMAT_EXPAND) {
    RedisModule_ReplyKV_SimpleString(reply, "format", "EXPAND");
  } else {
    RedisModule_ReplyKV_SimpleString(reply, "format", "STRING");
  }

  // <results>
  RedisModule_ReplyKV_Array(reply, "results");
}

/**
 * Finishes chunk reply by handling cursor ID and profile info for RESP3.
 */
static void finishSendChunkReply_Resp3(AREQ *req, RedisModule_Reply *reply,
  QueryProcessingCtx *qctx, int rc, bool cursor_done) {
  RedisModule_Reply_ArrayEnd(reply); // >results

  // <total_results>
  RedisModule_ReplyKV_LongLong(reply, "total_results", qctx->totalResults);

  // <error>
  _replyWarnings(req, reply, rc);

  bool has_timedout = (rc == RS_RESULT_TIMEDOUT) || hasTimeoutError(qctx->err);

  if (IsProfile(req)) {
    if (has_timedout) {
      ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_TIMEOUT);
    }
    RedisModule_Reply_MapEnd(reply); // >Results
    if (!(AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) || cursor_done) {
      req->profile(reply, req);
    }
  }

  RedisModule_Reply_MapEnd(reply);

  if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
    if (cursor_done) {
      RedisModule_Reply_LongLong(reply, 0);
    } else {
      RedisModule_Reply_LongLong(reply, req->cursor_id);
    }
    RedisModule_Reply_ArrayEnd(reply);
  }
}

/**
 * Serializes results and handles the main reply logic for RESP3.
 * Returns the final rc value and updates state accordingly.
 */
static int serializeAndReplyResults_Resp3(AREQ *req, RedisModule_Reply *reply, ResultProcessor *rp,
  QueryProcessingCtx *qctx, int rc, cachedVars *cv, ChunkSerializeState *state) {

    // If an error occurred, or a timeout in strict mode - return a simple error
    if (handleSendChunkError(req, reply, qctx, rc)) {
      state->cursor_done = true;
      return rc;
    }

    prepareSendChunkReply_Resp3(req, reply);

    if (AREQ_RequestFlags(req) & QEXEC_F_NOROWS || (rc != RS_RESULT_OK && rc != RS_RESULT_EOF)) {
      goto done_3;
    }

    if (state->results != NULL) {
      populateReplyWithResults(reply, state->results, req, cv);
      state->results = NULL;
    } else {
      if (rp->parent->resultLimit && rc == RS_RESULT_OK) {
        serializeResult(req, reply, state->r, cv);
      }

      SearchResult_Clear(state->r);
      if (rc != RS_RESULT_OK || !rp->parent->resultLimit) {
        goto done_3;
      }

      while (--rp->parent->resultLimit && (rc = rp->Next(rp, state->r)) == RS_RESULT_OK) {
        serializeResult(req, reply, state->r, cv);
        SearchResult_Clear(state->r);
      }
    }

done_3:
    // Assert that timeout only occurs when skipTimeoutChecks is false (if not in debug)
    RS_ASSERT(!(rc == RS_RESULT_TIMEDOUT) || !req->skipTimeoutChecks || IsDebug(req));

    state->cursor_done = (rc != RS_RESULT_OK
                          && !(rc == RS_RESULT_TIMEDOUT
                               && req->reqConfig.timeoutPolicy == TimeoutPolicy_Return));

    finishSendChunkReply_Resp3(req, reply, qctx, rc, state->cursor_done);

    return rc;
}

/**
 * Sends a chunk of <n> rows in the resp3 format
 */
static void sendChunk_Resp3(AREQ *req, RedisModule_Reply *reply, size_t limit,
  cachedVars cv) {
    SearchResult r = SearchResult_New();
    int rc = RS_RESULT_EOF;
    QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
    ResultProcessor *rp = qctx->endProc;

    ChunkSerializeState state = {
      .results = NULL,
      .r = &r,
      .nelem = 0,              // Unused in RESP3
      .resultsLen = 0,         // Unused in RESP3
    };

    startPipeline(req, rp, &state.results, &r, &rc);

    if (req->useReplyCallback) {
      // Store results for reply_callback (includes cv)
      debugPauseStoreResults(req, true);  // pause before
      AREQ_StoreResults(req, state.results, rc, cv);
      debugPauseStoreResults(req, false); // pause after
      return;
    }

    rc = serializeAndReplyResults_Resp3(req, reply, rp, qctx, rc, &cv, &state);

    finishSendChunk(req, state.results, &r, state.cursor_done);
}

/**
 * Sends a chunk of <n> rows, optionally also sending the preamble
 */
void sendChunk(AREQ *req, RedisModule_Reply *reply, size_t limit) {
  QEFlags reqFlags = AREQ_RequestFlags(req);
  if (!(reqFlags & QEXEC_F_IS_CURSOR) && !(reqFlags & QEXEC_F_IS_SEARCH)) {
    limit = req->maxAggregateResults;
  }
  RedisSearchCtx *sctx = AREQ_SearchCtx(req);
  if (sctx->spec) {
    IndexSpec_IncrActiveQueries(sctx->spec);
  }

  AGGPlan *plan = AREQ_AGGPlan(req);
  cachedVars cv = {
    .lastLookup = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_LAST),
    .lastAstp = AGPLN_GetArrangeStep(plan)
  };

  // Set the chunk size limit for the query
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
  qctx->resultLimit = limit;

  if (reply->resp3) {
    sendChunk_Resp3(req, reply, limit, cv);
  } else {
    sendChunk_Resp2(req, reply, limit, cv);
  }

  if (sctx->spec) {
    IndexSpec_DecrActiveQueries(sctx->spec);
  }
}

// Simple version of sendChunk that returns empty results for aggregate queries.
// Handles both RESP2 and RESP3 protocols with cursor support.
// Includes OOM warning when QueryError has OOM status.
// Currently used during OOM conditions to return empty results instead of failing.
// Based on sendChunk_Resp2/3 patterns.
 void sendChunk_ReplyOnly_EmptyResults(RedisModuleCtx *ctx, AREQ *req) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  if (reply->resp3) {

    if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
      RedisModule_Reply_Array(reply);
    }
    // RESP3 format - use map structure
    RedisModule_Reply_Map(reply);

    if (IsProfile(req)) {
      Profile_PrepareMapForReply(reply);
    }

    // attributes (field names)
    RedisModule_Reply_SimpleString(reply, "attributes");
    RedisModule_Reply_EmptyArray(reply);

    // <format>
    if (AREQ_RequestFlags(req) & QEXEC_FORMAT_EXPAND) {
      RedisModule_ReplyKV_SimpleString(reply, "format", "EXPAND"); // >format
    } else {
      RedisModule_ReplyKV_SimpleString(reply, "format", "STRING"); // >format
    }

    // results (empty array)
    RedisModule_ReplyKV_Array(reply, "results");
    RedisModule_Reply_ArrayEnd(reply);

    // total_results
    RedisModule_ReplyKV_LongLong(reply, "total_results", 0);

    // warning
    RedisModule_ReplyKV_Array(reply, "warning"); // >warnings
    if (QueryError_HasQueryOOMWarning(AREQ_QueryProcessingCtx(req)->err)) {
      QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_SHARD, 1, !IsInternal(req));
      // Shards should use SHARD warning
      // SA and Coordinator should use COORD warning
      const char *warning = !IsInternal(req) ? QUERY_WOOM_COORD : QUERY_WOOM_SHARD;
      RedisModule_Reply_SimpleString(reply, warning);
      ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_QUERY_OOM);
    }

    if (req->stateflags & QEXEC_S_ASM_TRIMMING_DELAY_TIMEOUT) {
      QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_ASM_INACCURATE_RESULTS, 1, !IsInternal(req));
      ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_ASM_INACCURATE_RESULTS);
      RedisModule_Reply_SimpleString(reply, QUERY_ASM_INACCURATE_RESULTS);
    }
    RedisModule_Reply_ArrayEnd(reply);

    // Add BG_SCAN_OOM warning to profile context if applicable
    RedisSearchCtx *sctx = AREQ_SearchCtx(req);
    if (sctx && sctx->spec && sctx->spec->scan_failed_OOM) {
      ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_BG_SCAN_OOM);
    }

    if (IsProfile(req)) {
      RedisModule_Reply_MapEnd(reply);  // >Results
      req->profile(reply, req);
    }

    RedisModule_Reply_MapEnd(reply);

    if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
      RedisModule_Reply_LongLong(reply, 0);
      RedisModule_Reply_ArrayEnd(reply);
    }
  } else {

    // Upon `FT.PROFILE` commands, embed the response inside another map
    if (IsProfile(req)) {
      Profile_PrepareMapForReply(reply);
    } else if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
      RedisModule_Reply_Array(reply);
    }

    // RESP2 format - use array structure
    RedisModule_Reply_Array(reply);

    // First element is always the total count (0 for empty results)
    RedisModule_Reply_LongLong(reply, 0);

    // No individual results to add for empty results

    RedisModule_Reply_ArrayEnd(reply);

    if (QueryError_HasQueryOOMWarning(AREQ_QueryProcessingCtx(req)->err)) {
      QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_SHARD, 1, !IsInternal(req));
      ProfileWarnings_Add(&AREQ_ProfilePrinterCtx(req)->warnings, PROFILE_WARNING_TYPE_QUERY_OOM);
    }

    // Add BG_SCAN_OOM warning to profile context if applicable
    RedisSearchCtx *sctx = AREQ_SearchCtx(req);
    if (sctx && sctx->spec && sctx->spec->scan_failed_OOM) {
      ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_BG_SCAN_OOM);
    }

    if (req->stateflags & QEXEC_S_ASM_TRIMMING_DELAY_TIMEOUT) {
      QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_ASM_INACCURATE_RESULTS, 1, !IsInternal(req));
      ProfileWarnings_Add(&req->profileCtx.warnings, PROFILE_WARNING_TYPE_ASM_INACCURATE_RESULTS);
    }

    if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
      // Cursor done
      RedisModule_Reply_LongLong(reply, 0);
      if (IsProfile(req)) {
        req->profile(reply, req);
      }
      // Cursor end array
      RedisModule_Reply_ArrayEnd(reply);
    } else if (IsProfile(req)) {
      req->profile(reply, req);
      RedisModule_Reply_ArrayEnd(reply);
    }
  }
  RedisModule_EndReply(reply);
}

void AREQ_Execute(AREQ *req, RedisModuleCtx *ctx) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  sendChunk(req, reply, UINT64_MAX);
  RedisModule_EndReply(reply);
  AREQ_DecrRef(req);
}

// Creates a new blockedClientReqCtx, taking ownership of the AREQ reference from the main thread.
// Note: No AREQ_IncrRef here - ownership is transferred, not shared.
static blockedClientReqCtx *blockedClientReqCtx_New(AREQ *req,
                                                    RedisModuleBlockedClient *blockedClient, StrongRef spec) {
  blockedClientReqCtx *ret = rm_new(blockedClientReqCtx);
  ret->req = req;
  ret->blockedClient = blockedClient;
  ret->spec_ref = StrongRef_Demote(spec);
  return ret;
}

static AREQ *blockedClientReqCtx_getRequest(const blockedClientReqCtx *BCRctx) {
  return BCRctx->req;
}

static void blockedClientReqCtx_setRequest(blockedClientReqCtx *BCRctx, AREQ *req) {
  BCRctx->req = req;
}

static void blockedClientReqCtx_destroy(blockedClientReqCtx *BCRctx) {
  RedisModule_BlockedClientMeasureTimeEnd(BCRctx->blockedClient);
  void *privdata = RedisModule_BlockClientGetPrivateData(BCRctx->blockedClient);
  RedisModule_UnblockClient(BCRctx->blockedClient, privdata);

  // Release the owned AREQ reference if it has not already been released.
  // On the normal success path, AREQ_Execute() releases the reference and
  // the owner clears it via blockedClientReqCtx_setRequest(BCRctx, NULL),
  // so this conditional avoids a double-decr while still handling error paths
  // where AREQ_Execute() is never called.
  if (BCRctx->req) {
    AREQ_DecrRef(BCRctx->req);
    BCRctx->req = NULL;
  }

  WeakRef_Release(BCRctx->spec_ref);
  rm_free(BCRctx);
}

// Helper for error handling in AREQ_Execute_Callback.
// For FAIL policy (useReplyCallback=true): stores error for QueryReplyCallback to handle.
// For RETURN policy: replies with error directly.
static void AREQ_ReplyOrStoreError(AREQ *req, RedisModuleCtx *ctx, QueryError *status) {
  if (req->useReplyCallback) {
    // Deep copy since QueryError contains heap-allocated strings.
    // QueryReplyCallback will clear the stored error after replying.
    QueryError_CloneFrom(status, &req->storedReplyState.err);
    // Clear the original to avoid leaking heap-allocated strings.
    QueryError_ClearError(status);
  } else {
    QueryError_ReplyAndClear(ctx, status);
  }
}

void AREQ_Execute_Callback(blockedClientReqCtx *BCRctx) {
  AREQ *req = blockedClientReqCtx_getRequest(BCRctx);

  // Check if timed out while in the job queue.
  if (AREQ_TimedOut(req)) {
    // Timeout callback already replied.
    // blockedClientReqCtx_destroy will release the AREQ ref.
    blockedClientReqCtx_destroy(BCRctx);
    return;
  }

  if (IsProfile(req)) {
    req->profileClocks.profileQueueTime = rs_wall_clock_elapsed_ns(&req->profileClocks.initClock);
  }

  RedisModuleCtx *outctx = RedisModule_GetThreadSafeContext(BCRctx->blockedClient);
  QueryError status = QueryError_Default();

  StrongRef execution_ref = IndexSpecRef_Promote(BCRctx->spec_ref);
  if (!StrongRef_Get(execution_ref)) {
    // The index was dropped while the query was in the job queue.
    QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
    AREQ_ReplyOrStoreError(req, outctx, &status);
    RedisModule_FreeThreadSafeContext(outctx);
    blockedClientReqCtx_destroy(BCRctx);
    return;
  }

  // Cursors are created with a thread-safe context, so we don't want to replace it
  RedisSearchCtx *sctx = AREQ_SearchCtx(req);
  if (!(AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR)) {
    sctx->redisCtx = outctx;
  }

  // lock spec
  RedisSearchCtx_LockSpecRead(sctx);
  if (prepareExecutionPlan(req, &status) != REDISMODULE_OK) {
    RedisSearchCtx_UnlockSpec(sctx);
    goto error;
  }

  if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
    RedisModule_Reply _reply = RedisModule_NewReply(outctx), *reply = &_reply;
    int rc = AREQ_StartCursor(req, reply, execution_ref, &status, false);
    RedisModule_EndReply(reply);
    if (rc != REDISMODULE_OK) {
      goto error;
    }
  } else {
    AREQ_Execute(req, outctx);
  }

  // If the execution was successful, we either:
  // 1. Freed the request (if it was a regular query)
  // 2. Kept it as the cursor's state (if it was a cursor query)
  // Either way, we don't want to free `req` here. we set it to NULL so that it won't be freed with the context.
  blockedClientReqCtx_setRequest(BCRctx, NULL);
  goto cleanup;

error:
  AREQ_ReplyOrStoreError(req, outctx, &status);

cleanup:
  // No need to unlock spec as it was unlocked by `AREQ_Execute` or will be unlocked by `blockedClientReqCtx_destroy`
  RedisModule_FreeThreadSafeContext(outctx);
  IndexSpecRef_Release(execution_ref);
  blockedClientReqCtx_destroy(BCRctx);
}

// Assumes the spec is guarded (by its own lock for read or by the global lock)
int prepareExecutionPlan(AREQ *req, QueryError *status) {
  int rc = REDISMODULE_ERR;
  RedisSearchCtx *sctx = AREQ_SearchCtx(req);
  RSSearchOptions *opts = &req->searchopts;
  QueryAST *ast = &req->ast;

  // Set timeout for the query execution
  // TODO: this should be done in `AREQ_execute`, but some of the iterators needs the timeout's
  // value and some of the execution begins in `QAST_Iterate`.
  // Setting the timeout context should be done in the same thread that executes the query.
  SearchCtx_UpdateTime(sctx, req->reqConfig.queryTimeoutMS);

  req->rootiter = QAST_Iterate(ast, opts, sctx, AREQ_RequestFlags(req), status);

  // check possible optimization after creation of QueryIterator tree
  if (IsOptimized(req)) {
    QOptimizer_Iterators(req, req->optimizer);
  }

  if (AREQ_ShouldCheckTimeout(req) && req->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
    TimedOut_WithStatus(&sctx->time.timeout, status);
  }

  if (QueryError_HasError(status)) {
    return REDISMODULE_ERR;
  }

  if (IsProfile(req)) {
    // Add a Profile iterators before every iterator in the tree
    Profile_AddIters(&req->rootiter);
  }

  rs_wall_clock parseClock;
  bool is_profile = IsProfile(req);
  if (is_profile) {
    rs_wall_clock_init(&parseClock);
    // Calculate the time elapsed for profileParseTime by using the initialized parseClock
    // Subtract queue time since initClock includes time spent waiting in the queue
    req->profileClocks.profileParseTime = rs_wall_clock_diff_ns(&req->profileClocks.initClock, &parseClock) - req->profileClocks.profileQueueTime;
  }

  rc = AREQ_BuildPipeline(req, status);

  if (is_profile) {
    req->profileClocks.profilePipelineBuildTime = rs_wall_clock_elapsed_ns(&parseClock);
  }

  if (IsDebug(req)) {
    rc = parseAndCompileDebug((AREQ_Debug *)req, status);
    if (rc != REDISMODULE_OK) {
      return rc;
    }
  }

  return rc;
}

static int buildRequest(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int type,
                        QueryError *status, AREQ **r) {
  int rc = REDISMODULE_ERR;
  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NULL;
  RedisModuleCtx *thctx = NULL;

  if (type == COMMAND_SEARCH) {
    AREQ_AddRequestFlags(*r, QEXEC_F_IS_SEARCH);
  }
  else if (type == COMMAND_AGGREGATE) {
    AREQ_AddRequestFlags(*r, QEXEC_F_IS_AGGREGATE);
  }

  AREQ_AddRequestFlags(*r, QEXEC_FORMAT_DEFAULT);

  if (AREQ_Compile(*r, argv + 2, argc - 2, status) != REDISMODULE_OK) {
    RS_LOG_ASSERT(QueryError_HasError(status), "Query has error");
    goto done;
  }

  (*r)->protocol = is_resp3(ctx) ? 3 : 2;

  // Prepare the query.. this is where the context is applied.
  if (AREQ_RequestFlags(*r) & QEXEC_F_IS_CURSOR) {
    RedisModuleCtx *newctx = RedisModule_GetDetachedThreadSafeContext(ctx);
    RedisModule_SelectDb(newctx, RedisModule_GetSelectedDb(ctx));
    ctx = thctx = newctx;  // In case of error!
  }

  sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_NO_INDEX, "Index not found", ": %s", indexname);
    goto done;
  }

  CurrentThread_SetIndexSpec(sctx->spec->own_ref);

  rc = AREQ_ApplyContext(*r, sctx, status);
  thctx = NULL;
  // ctx is always assigned after ApplyContext
  if (rc != REDISMODULE_OK) {
    CurrentThread_ClearIndexSpec();
    RS_LOG_ASSERT(QueryError_HasError(status), "Query has error");
  }

done:
  if (rc != REDISMODULE_OK && *r) {
    AREQ_DecrRef(*r);
    *r = NULL;
    if (thctx) {
      RedisModule_FreeThreadSafeContext(thctx);
    }
  }
  return rc;
}

static int prepareRequest(AREQ **r_ptr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc, CommandType type, ProfileOptions profileOptions, QueryError *status) {
  AREQ *r = *r_ptr;
  // If we got here, we know `argv[0]` is a valid registered command name.
  // If it starts with an underscore, it is an internal command.
  if (RedisModule_StringPtrLen(argv[0], NULL)[0] == '_') {
    AREQ_AddRequestFlags(r, QEXEC_F_INTERNAL);
  }

  ApplyProfileOptions(AREQ_QueryProcessingCtx(r), &r->reqflags, profileOptions);

  if (!IsInternal(r) || IsProfile(r)) {
    // We currently don't need to measure the time for internal and non-profile commands
    rs_wall_clock_init(&r->profileClocks.initClock);
    rs_wall_clock_init(&AREQ_QueryProcessingCtx(r)->initTime);
  }

  // This function also builds the RedisSearchCtx
  // It will search for the spec according to the name given in the argv array,
  // and ensure the spec is valid.
  if (buildRequest(ctx, argv, argc, type, status, r_ptr) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  SET_DIALECT(AREQ_SearchCtx(r)->spec->used_dialects, r->reqConfig.dialectVersion);
  SET_DIALECT(RSGlobalStats.totalStats.used_dialects, r->reqConfig.dialectVersion);

  return REDISMODULE_OK;
}

// Timeout callback for AREQ execution in Run in Threads mode.
// Called on the main thread when the blocking client times out (FAIL policy only).
// Simply sets the timeout flag and replies with error - no synchronization needed
// because AREQ uses reply_callback pattern (background thread does not reply directly).
static int QueryTimeoutFailCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);

  BlockedQueryNode *node = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!node || !node->privdata) {
    // Shouldn't happen, but handle gracefully
    RedisModule_Log(ctx, "warning", "QueryTimeoutFailCallback: no node or privdata");
    QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, true);
    RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));
    return REDISMODULE_OK;
  }

  AREQ *req = (AREQ *)node->privdata;
  // Signal timeout to background thread (will notice and skip storing results)
  AREQ_SetTimedOut(req);

  // Reply with timeout error
  QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, !IsInternal(req));
  RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));

  return REDISMODULE_OK;
}

// Reply callback for AREQ execution in Run in Threads mode (FAIL policy).
// Called on the main thread when the background thread calls UnblockClient.
// The background thread stored results in req->storedReplyState, which we use to build the reply.
// Note: This callback is NOT called if timeout fired first (bc->client becomes NULL).
// Reference counting: BlockedQueryNode holds a reference released via FreeQueryNode after this callback.
static int QueryReplyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);

  BlockedQueryNode *node = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!node || !node->privdata) {
    // Shouldn't happen, but handle gracefully
    RedisModule_Log(ctx, "warning", "QueryReplyCallback: no node or privdata");
    RedisModule_ReplyWithError(ctx, "ERR Internal error: no request context");
    return REDISMODULE_OK;
  }

  AREQ *req = (AREQ *)node->privdata;

  // Check if results were stored (background thread completed successfully)
  if (!req->storedReplyState.hasStoredResults) {
    // Background thread didn't store results - some early error occurred.
    // Use the stored error if available, otherwise generic error.
    if (QueryError_HasError(&req->storedReplyState.err)) {
      QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(&req->storedReplyState.err), 1, !IsInternal(req));
      QueryError_ReplyAndClear(ctx, &req->storedReplyState.err);
    } else {
      RedisModule_ReplyWithError(ctx, "ERR Internal error: no results stored");
    }
    return REDISMODULE_OK;
  }

  // Use stored state directly - no need to recompute cv, it was stored by AREQ_StoreResults
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
  ResultProcessor *rp = qctx->endProc;
  ChunkReplyState *stored = &req->storedReplyState;

  // Point qctx->err to the stored error so serializeAndReplyResults/finishSendChunk can access it.
  // This is the end of the request lifecycle, so no need to restore.
  qctx->err = &stored->err;

  // Create a stack-allocated SearchResult for finishSendChunk cleanup
  SearchResult r = SearchResult_New();

  // Build ChunkSerializeState from stored results
  ChunkSerializeState state = {
    .results = stored->results,
    .r = &r,
    .nelem = 0,
    .resultsLen = REDISMODULE_POSTPONED_ARRAY_LEN,
    .cursor_done = false
  };
  int rc = stored->rc;

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  // Call serializeAndReplyResults like the normal sendChunk path
  if (reply->resp3) {
    rc = serializeAndReplyResults_Resp3(req, reply, rp, qctx, rc, &stored->cv, &state);
  } else {
    rc = serializeAndReplyResults_Resp2(req, reply, rp, qctx, rc, qctx->resultLimit, &stored->cv, &state);
  }

  RedisModule_EndReply(reply);

  // Clear stored results pointer since ownership was transferred to state
  stored->results = NULL;
  stored->hasStoredResults = false;

  // finishSendChunk handles cleanup and stats, and sets QEXEC_S_ITERDONE if cursor is done
  finishSendChunk(req, state.results, &r, state.cursor_done);

  // Handle cursor lifecycle now that QEXEC_S_ITERDONE has been set by finishSendChunk.
  // runCursor stored the cursor handle here instead of pausing/freeing it immediately,
  // because finishSendChunk (which sets QEXEC_S_ITERDONE) runs in the reply_callback.
  if (stored->cursor) {
    if (req->stateflags & QEXEC_S_ITERDONE) {
      Cursor_Free(stored->cursor);
    } else {
      Cursor_Pause(stored->cursor);
    }
    stored->cursor = NULL;
  }

  // No AREQ_DecrRef here - BlockedQueryNode holds the reference, released via FreeQueryNode.
  return REDISMODULE_OK;
}

static int buildPipelineAndExecute(AREQ *r, RedisModuleCtx *ctx, QueryError *status) {
  RedisSearchCtx *sctx = AREQ_SearchCtx(r);
  if (RunInThread()) {
    StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);

    BlockedClientTimeoutCB timeoutCB = NULL;
    BlockedClientReplyCB replyCB = NULL;
    int blockedClientTimeoutMS = 0;
    // Determine timeout and reply callbacks based on policy.
    if (r->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
      timeoutCB = QueryTimeoutFailCallback;
      replyCB = QueryReplyCallback;
      blockedClientTimeoutMS = r->reqConfig.queryTimeoutMS;
      r->useReplyCallback = true;
    }

    RedisModuleBlockedClient* blockedClient = BlockQueryClientWithTimeout(ctx, spec_ref, r, blockedClientTimeoutMS, replyCB, timeoutCB);
    blockedClientReqCtx *BCRctx = blockedClientReqCtx_New(r, blockedClient, spec_ref);
    // Mark the request as thread safe, so that the pipeline will be built in a thread safe manner
    AREQ_AddRequestFlags(r, QEXEC_F_RUN_IN_BACKGROUND);
    if (AREQ_QueryProcessingCtx(r)->isProfile ){
      // Add 1ns as epsilon value so we can verify that the GIL time is greater than 0.
      AREQ_QueryProcessingCtx(r)->queryGILTime += rs_wall_clock_elapsed_ns(&(AREQ_QueryProcessingCtx(r)->initTime)) + 1;
    }
    const int rc = workersThreadPool_AddWork((redisearch_thpool_proc)AREQ_Execute_Callback, BCRctx);
    RS_ASSERT(rc == 0);
  } else {
    // Take a read lock on the spec (to avoid conflicts with the GC).
    // This is released in AREQ_Free or while executing the query.
    RedisSearchCtx_LockSpecRead(sctx);

    if (prepareExecutionPlan(r, status) != REDISMODULE_OK) {
      CurrentThread_ClearIndexSpec();
      return REDISMODULE_ERR;
    }
    if (AREQ_RequestFlags(r) & QEXEC_F_IS_CURSOR) {
      // Since we are still in the main thread, and we already validated the
      // spec'c existence, it is safe to directly get the strong reference from the spec
      // found in buildRequest
      StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);
      RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
      int rc = AREQ_StartCursor(r, reply, spec_ref, status, false);
      RedisModule_EndReply(reply);
      if (rc != REDISMODULE_OK) {
        CurrentThread_ClearIndexSpec();
        return REDISMODULE_ERR;
      }
    } else {
      AREQ_Execute(r, ctx);
    }
  }

  CurrentThread_ClearIndexSpec();
  return REDISMODULE_OK;
}

/**
 * @param profileOptions is a bitmask of EXEC_* flags defined in ProfileOptions enum.
 */
int execCommandCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                             CommandType type, ProfileOptions profileOptions) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  QueryError status = QueryError_Default();

  // Memory guardrail
  if (QueryMemoryGuard(ctx)) {
    if (RSGlobalConfig.requestConfigParams.oomPolicy == OomPolicy_Fail) {
      QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_OUT_OF_MEMORY, 1, GetNumShards_UnSafe() == 1);
      return QueryMemoryGuardFailure_WithReply(ctx);
    }
    // Assuming OOM policy is return since we didn't ignore the memory guardrail
    RS_ASSERT(RSGlobalConfig.requestConfigParams.oomPolicy == OomPolicy_Return);
    return single_shard_common_query_reply_empty(ctx, argv, argc, profileOptions, QUERY_ERROR_CODE_OUT_OF_MEMORY);
  }

  AREQ *r = AREQ_New();

  if (prepareRequest(&r, ctx, argv, argc, type, profileOptions, &status) != REDISMODULE_OK) {
    goto error;
  }

  if (buildPipelineAndExecute(r, ctx, &status) != REDISMODULE_OK) {
    goto error;
  }

  return REDISMODULE_OK;

error:
  // Update global query errors statistics
  // If num shards == 1 we are in SA, and we count it as a coord error
  QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(&status), 1, GetNumShards_UnSafe() == 1);

  if (r) {
    AREQ_DecrRef(r);
  }

  return QueryError_ReplyAndClear(ctx, &status);
}

int RSExecuteAggregateOrSearch(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, CommandType type, ProfileOptions profileOptions) {
  return execCommandCommon(ctx, argv, argc, type, profileOptions);
}

char *RS_GetExplainOutput(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          QueryError *status) {
  AREQ *r = AREQ_New();
  if (buildRequest(ctx, argv, argc, COMMAND_EXPLAIN, status, &r) != REDISMODULE_OK) {
    return NULL;
  }
  if (prepareExecutionPlan(r, status) != REDISMODULE_OK) {
    AREQ_DecrRef(r);
    CurrentThread_ClearIndexSpec();
    return NULL;
  }
  char *ret = QAST_DumpExplain(&r->ast, AREQ_SearchCtx(r)->spec);
  AREQ_DecrRef(r);
  CurrentThread_ClearIndexSpec();
  return ret;
}

// Assumes that the cursor has a strong ref to the relevant spec and that it is already locked.
int AREQ_StartCursor(AREQ *r, RedisModule_Reply *reply, StrongRef spec_ref, QueryError *err, bool coord) {
  Cursor *cursor = Cursors_Reserve(getCursorList(coord), spec_ref, r->cursorConfig.maxIdle, err);
  if (cursor == NULL) {
    return REDISMODULE_ERR;
  }
  cursor->execState = r;
  r->cursor_id = cursor->id;
  runCursor(reply, cursor, 0);
  return REDISMODULE_OK;
}

// Assumes that the cursor has a strong ref to the relevant spec and that it is already locked.
static void runCursor(RedisModule_Reply *reply, Cursor *cursor, size_t num) {
  AREQ *req = cursor->execState;
  AREQ_ProfilePrinterCtx(req)->cursor_reads++;
  // update timeout for current cursor read
  SearchCtx_UpdateTime(AREQ_SearchCtx(req), req->reqConfig.queryTimeoutMS);

  if (!num) {
    num = req->cursorConfig.chunkSize;
    if (!num) {
      num = RSGlobalConfig.cursorReadSize;
    }
  }
  req->cursorConfig.chunkSize = num;

  sendChunk(req, reply, num);
  RedisSearchCtx_UnlockSpec(AREQ_SearchCtx(req)); // Verify that we release the spec lock

  if (req->useReplyCallback) {
    // In reply_callback path, sendChunk returns early after storing results.
    // QEXEC_S_ITERDONE is not set yet (it's set by finishSendChunk in the reply_callback).
    // Store the cursor handle so the reply_callback can pause/free it after finishSendChunk.
    req->storedReplyState.cursor = cursor;
    return;
  }

  if (req->stateflags & QEXEC_S_ITERDONE) {
    Cursor_Free(cursor);
  } else {
    // Update the idle timeout
    Cursor_Pause(cursor);
  }
}

static QueryProcessingCtx *prepareForCursorRead(Cursor *cursor, bool *hasLoader, bool *initClock, QEFlags *reqFlags, QueryError *status) {
  AREQ *req = cursor->execState;
  QueryProcessingCtx *qctx = NULL;
  if (req) {
    qctx = AREQ_QueryProcessingCtx(cursor->execState);
    AREQ_RemoveRequestFlags(req, QEXEC_F_IS_AGGREGATE); // Second read was not triggered by FT.AGGREGATE
    *reqFlags = AREQ_RequestFlags(req);
    *hasLoader = HasLoader(req);
    *initClock = IsProfile(req) || !IsInternal(req);
  } else {
    HybridRequest *hreq = StrongRef_Get(cursor->hybrid_ref);
    *reqFlags = hreq->reqflags;
    qctx = &hreq->tailPipeline->qctx;
    // If we don't have an AREQ then this is a coordinator cursor going directly to the client
    // We can't have a loader in the coordinator
    *hasLoader = false;
  }
  qctx->err = status;
  return qctx;
}

static void cursorRead(RedisModuleCtx *ctx, Cursor *cursor, size_t count, bool bg) {

  QueryError status = QueryError_Default();

  QEFlags reqFlags = 0;
  bool hasLoader = false;
  bool initClock = false;
  AREQ *req = cursor->execState;
  QueryProcessingCtx *qctx = prepareForCursorRead(cursor, &hasLoader, &initClock, &reqFlags, &status);
  StrongRef execution_ref;
  bool has_spec = cursor_HasSpecWeakRef(cursor);
  // If the cursor is associated with a spec, e.g a coordinator ctx.
  if (has_spec) {
    execution_ref = IndexSpecRef_Promote(cursor->spec_ref);
    if (!StrongRef_Get(execution_ref)) {
      // The index was dropped while the cursor was idle.
      // Notify the client that the query was aborted.
      Cursor_Free(cursor);
      RedisModule_ReplyWithError(ctx, "The index was dropped while the cursor was idle");
      return;
    }

    if (hasLoader) { // Quick check if the cursor has loaders.
      bool isSetForBackground = reqFlags & QEXEC_F_RUN_IN_BACKGROUND;
      if (bg && !isSetForBackground) {
        // Reset loaders to run in background
        SetLoadersForBG(AREQ_QueryProcessingCtx(req));
        // Mark the request as set to run in background
        AREQ_AddRequestFlags(req, QEXEC_F_RUN_IN_BACKGROUND);
      } else if (!bg && isSetForBackground) {
        // Reset loaders to run in main thread
        SetLoadersForMainThread(AREQ_QueryProcessingCtx(req));
        // Mark the request as set to run in main thread
        AREQ_RemoveRequestFlags(req, QEXEC_F_RUN_IN_BACKGROUND);
      }
    }
  }

  if (initClock) {
    rs_wall_clock_init(&req->profileClocks.initClock); // Reset the clock for the current cursor read
  }

  if (req) {
    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    runCursor(reply, cursor, count);
    RedisModule_EndReply(reply);
  } else {
    // TODO: run hybrid cursor - this needs to be implemented for the coordinator
  }
  if (has_spec) {
    IndexSpecRef_Release(execution_ref);
  }
}

typedef struct {
  RedisModuleBlockedClient *bc;
  Cursor *cursor;
  size_t count;
} CursorReadCtx;

static void cursorRead_ctx(CursorReadCtx *cr_ctx) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(cr_ctx->bc);
  cursorRead(ctx, cr_ctx->cursor, cr_ctx->count, true);
  RedisModule_FreeThreadSafeContext(ctx);
  RedisModule_BlockedClientMeasureTimeEnd(cr_ctx->bc);
  void *privdata = RedisModule_BlockClientGetPrivateData(cr_ctx->bc);
  RedisModule_UnblockClient(cr_ctx->bc, privdata);
  rm_free(cr_ctx);
}

/**
 * FT.CURSOR READ {index} {CID} {COUNT} [MAXIDLE]
 */
int RSCursorReadCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }

  long long cid;
  if (RedisModule_StringToLongLong(argv[3], &cid) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Bad cursor ID");
  }

  long long count = 0;
  if (argc > 5) {
    // e.g. 'COUNT <timeout>'
    // Verify that the 4'th argument is `COUNT`.
    const char *count_str = RedisModule_StringPtrLen(argv[4], NULL);
    if (strcasecmp(count_str, "count") != 0) {
      return RedisModule_ReplyWithErrorFormat(ctx, "Unknown argument `%s`", count_str);
    }

    if (RedisModule_StringToLongLong(argv[5], &count) != REDISMODULE_OK) {
      return RedisModule_ReplyWithErrorFormat(ctx, "Bad value for COUNT: `%s`", RedisModule_StringPtrLen(argv[5], NULL));
    }
  }

  Cursor *cursor = Cursors_TakeForExecution(GetGlobalCursor(cid), cid);
  if (cursor == NULL) {
    return RedisModule_ReplyWithErrorFormat(ctx, "Cursor not found, id: %d", cid);
  }

  // We have to check that we are not blocked yet from elsewhere (e.g. coordinator)
  if (RunInThread() && !RedisModule_GetBlockedClientHandle(ctx)) {
    CursorReadCtx *cr_ctx = rm_new(CursorReadCtx);
    cr_ctx->bc = BlockCursorClient(ctx, cursor, count, 0);
    cr_ctx->cursor = cursor;
    cr_ctx->count = count;
    workersThreadPool_AddWork((redisearch_thpool_proc)cursorRead_ctx, cr_ctx);
  } else {
    cursorRead(ctx, cursor, count, false);
  }

  return REDISMODULE_OK;
}

/**
 * FT.CURSOR PROFILE {index} {CID}
 */
int RSCursorProfileCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }

  long long cid;
  if (RedisModule_StringToLongLong(argv[3], &cid) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Bad cursor ID");
  }

  // Return profile
  Cursor *cursor = Cursors_TakeForExecution(GetGlobalCursor(cid), cid);
  if (cursor == NULL) {
    return RedisModule_ReplyWithErrorFormat(ctx, "Cursor not found, id: %d", cid);
  }

  AREQ *req = cursor->execState;
  if (!IsProfile(req)) {
    Cursor_Pause(cursor); // Pause the cursor again since we are not going to use it, but it's still valid.
    return RedisModule_ReplyWithErrorFormat(ctx, "cursor request is not profile, id: %d", cid);
  }
  // We get here only if it's internal (coord) cursor because cursor is not supported with profile,
  // and we already checked that the cmd is not for profiling.
  // Since it's an internal cursor, it must be associated with a spec.
  RS_ASSERT(cursor_HasSpecWeakRef(cursor));
  // Check if the spec is still valid
  StrongRef execution_ref = IndexSpecRef_Promote(cursor->spec_ref);
  if (!StrongRef_Get(execution_ref)) {
    // The index was dropped while the cursor was idle.
    // Notify the client that the query was aborted.
    RedisModule_ReplyWithError(ctx, "The index was dropped while the cursor was idle");
  } else {
    QueryError status = QueryError_Default();
    AREQ_QueryProcessingCtx(req)->err = &status;
    sendChunk_ReplyOnly_EmptyResults(ctx, req);
    IndexSpecRef_Release(execution_ref);
  }

  // Free the cursor
  Cursor_Free(cursor);

  return REDISMODULE_OK;
}

/**
 * FT.CURSOR DEL {index} {CID}
 */
int RSCursorDelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }

  long long cid;
  if (RedisModule_StringToLongLong(argv[3], &cid) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Bad cursor ID");
  }
  int rc = Cursors_Purge(GetGlobalCursor(cid), cid);
  if (rc != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Cursor does not exist");
  } else {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

/**
 * FT.CURSOR GC {index}
 */
int RSCursorGCCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  // Collect idle cursors from both local and coord lists
  int ret_local = Cursors_CollectIdle(&g_CursorsList);
  int ret_coord = Cursors_CollectIdle(&g_CursorsListCoord);

  // `Cursors_CollectIdle` returns -1 if no cursors were expired (quick check),
  // otherwise it returns the number of collected cursors (which can be 0, if there were no idle+expired cursors).
  // We want to return -1 only if both lists returned -1, otherwise we sum the non-negative results.
  int ret;
  if (ret_local < 0 && ret_coord < 0) {
    ret = -1;
  } else if (ret_local < 0) {
    ret = ret_coord;
  } else if (ret_coord < 0) {
    ret = ret_local;
  } else {
    ret = ret_local + ret_coord;
  }

  return RedisModule_ReplyWithLongLong(ctx, ret);
}

/* ======================= DEBUG ONLY ======================= */

// FT.DEBUG FT.AGGREGATE idx * <DEBUG_TYPE> <DEBUG_TYPE_ARGS> <DEBUG_TYPE> <DEBUG_TYPE_ARGS> ... DEBUG_PARAMS_COUNT 2
// Example:
// FT.AGGREGATE idx * TIMEOUT_AFTER_N 3 DEBUG_PARAMS_COUNT 2
int DEBUG_execCommandCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                             CommandType type, ProfileOptions profileOptions) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  AREQ *r = NULL;
  AREQ_Debug_params debug_params = {0};
  int debug_argv_count = 0;
  // debug_req and &debug_req->r are allocated in the same memory block, so it will be freed
  // when AREQ_Free is called
  QueryError status = QueryError_Default();
  AREQ_Debug *debug_req = AREQ_Debug_New(argv, argc, &status);
  if (!debug_req) {
    goto error;
  }
  r = &debug_req->r;
  debug_params = debug_req->debug_params;

  debug_argv_count = debug_params.debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>` strings
  // Parse the query, not including debug params

  if (prepareRequest(&r, ctx, argv, argc - debug_argv_count, type, profileOptions, &status) != REDISMODULE_OK) {
    goto error;
  }

  if (buildPipelineAndExecute(r, ctx, &status) != REDISMODULE_OK) {
    goto error;
  }

  return REDISMODULE_OK;

error:
  if (r) {
    AREQ_DecrRef(r);
  }
  return QueryError_ReplyAndClear(ctx, &status);
}

/**DEBUG COMMANDS - not for production! */
int DEBUG_RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  ProfileOptions profileOptions = EXEC_DEBUG;
  return DEBUG_execCommandCommon(ctx, argv, argc, COMMAND_AGGREGATE, profileOptions);
}

int DEBUG_RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  ProfileOptions profileOptions = EXEC_DEBUG;
  return DEBUG_execCommandCommon(ctx, argv, argc, COMMAND_SEARCH, profileOptions);
}
