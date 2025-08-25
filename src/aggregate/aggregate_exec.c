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
#include "profile.h"
#include "query_optimizer.h"
#include "resp3.h"
#include "query_error.h"
#include "info/global_stats.h"
#include "aggregate_debug.h"
#include "info/info_redis/block_client.h"
#include "info/info_redis/threads/current_thread.h"
#include "pipeline/pipeline.h"
#include "util/units.h"

typedef enum {
  EXEC_NO_FLAGS = 0x00,
  EXEC_WITH_PROFILE = 0x01,
  EXEC_WITH_PROFILE_LIMITED = 0x02,
  EXEC_DEBUG = 0x04,
} ExecOptions;

// Multi threading data structure
typedef struct {
  AREQ *req;
  RedisModuleBlockedClient *blockedClient;
  WeakRef spec_ref;
} blockedClientReqCtx;

static void runCursor(RedisModule_Reply *reply, Cursor *cursor, size_t num);

/**
 * Get the sorting key of the result. This will be the sorting key of the last
 * RLookup registry. Returns NULL if there is no sorting key
 */
static const RSValue *getReplyKey(const RLookupKey *kk, const SearchResult *r) {
  if ((kk->flags & RLOOKUP_F_SVSRC) && (r->rowdata.sv && RSSortingVector_Length(r->rowdata.sv) > kk->svidx)) {
    return RSSortingVector_Get(r->rowdata.sv, kk->svidx);
  } else {
    return RLookup_GetItem(kk, &r->rowdata);
  }
}



static void reeval_key(RedisModule_Reply *reply, const RSValue *key) {
  RedisModuleCtx *outctx = reply->ctx;
  RedisModuleString *rskey = NULL;
  if (!key) {
    RedisModule_Reply_Null(reply);
  }
  else {
    if (key->t == RSValue_Reference) {
      key = RSValue_Dereference(key);
    } else if (key->t == RSValue_Duo) {
      key = RS_DUOVAL_VAL(*key);
    }
    switch (key->t) {
      case RSValue_Number:
        // Serialize double - by prepending "#" to the number, so the coordinator/client can
        // tell it's a double and not just a numeric string value
        rskey = RedisModule_CreateStringPrintf(outctx, "#%.17g", key->numval);
        break;
      case RSValue_String:
        // Serialize string - by prepending "$" to it
        rskey = RedisModule_CreateStringPrintf(outctx, "$%s", key->strval.str);
        break;
      case RSValue_RedisString:
      case RSValue_OwnRstring:
        rskey = RedisModule_CreateStringPrintf(outctx, "$%s",
                                               RedisModule_StringPtrLen(key->rstrval, NULL));
        break;
      case RSValue_Null:
      case RSValue_Undef:
      case RSValue_Array:
      case RSValue_Map:
      case RSValue_Reference:
      case RSValue_Duo:
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
  const RSDocumentMetadata *dmd = r->dmd;
  size_t count0 = RedisModule_Reply_LocalCount(reply);
  bool has_map = RedisModule_HasMap(reply);

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
      RedisModule_Reply_Double(reply, r->score);
    } else {
      RedisModule_Reply_Array(reply);
      RedisModule_Reply_Double(reply, r->score);
      SEReply(reply, r->scoreExplain);
      RedisModule_Reply_ArrayEnd(reply);
    }
  }

  if (options & QEXEC_F_SENDRAWIDS) {
    if (has_map) {
      RedisModule_ReplyKV_LongLong(reply, "id", r->docId);
    } else {
      RedisModule_Reply_LongLong(reply, r->docId);
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
    bool need_map = has_map && currentField < requiredFieldsCount;
    if (need_map) {
      RedisModule_ReplyKV_Map(reply, "required_fields"); // >required_fields
    }
    for(; currentField < requiredFieldsCount; currentField++) {
      const RLookupKey *rlk = RLookup_GetKey_Read(cv->lastLk, req->requiredFields[currentField], RLOOKUP_F_NOFLAGS);
      const RSValue *v = rlk ? getReplyKey(rlk, r) : NULL;
      if (v && v->t == RSValue_Duo) {
        // For duo value, we use the value here (not the other value)
        v = RS_DUOVAL_VAL(*v);
      }
      RSValue rsv;
      if (rlk && (rlk->flags & RLOOKUP_T_NUMERIC) && v && v->t != RSVALTYPE_DOUBLE && !RSValue_IsNull(v)) {
        double d;
        RSValue_ToNumber(v, &d);
        RSValue_SetNumber(&rsv, d);
        v = &rsv;
      }
      if (need_map) {
        RedisModule_Reply_CString(reply, req->requiredFields[currentField]); // key name
      }
      reeval_key(reply, v);
    }
    if (need_map) {
      RedisModule_Reply_MapEnd(reply); // >required_fields
    }
  }

  if (!(options & QEXEC_F_SEND_NOFIELDS)) {
    const RLookup *lk = cv->lastLk;
    if (has_map) {
      RedisModule_Reply_SimpleString(reply, "extra_attributes");
    }

    if (r->flags & Result_ExpiredDoc) {
      RedisModule_Reply_Null(reply);
    } else {
      // Get the number of fields in the reply.
      // Excludes hidden fields, fields not included in RETURN and, score and language fields.
      RedisSearchCtx *sctx = AREQ_SearchCtx(req);
      SchemaRule *rule = (sctx && sctx->spec) ? sctx->spec->rule : NULL;
      int excludeFlags = RLOOKUP_F_HIDDEN;
      int requiredFlags = (req->outFields.explicitReturn ? RLOOKUP_F_EXPLICITRETURN : 0);
      int skipFieldIndex[lk->rowlen]; // Array has `0` for fields which will be skipped
      memset(skipFieldIndex, 0, lk->rowlen * sizeof(*skipFieldIndex));
      size_t nfields = RLookup_GetLength(lk, &r->rowdata, skipFieldIndex, requiredFlags, excludeFlags, rule);

      RedisModule_Reply_Map(reply);
        int i = 0;
        for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
          if (!kk->name || !skipFieldIndex[i++]) {
            continue;
          }
          const RSValue *v = RLookup_GetItem(kk, &r->rowdata);
          RS_LOG_ASSERT(v, "v was found in RLookup_GetLength iteration")

          RedisModule_Reply_StringBuffer(reply, kk->name, kk->name_len);

          QEFlags reqFlags = AREQ_RequestFlags(req);
          SendReplyFlags flags = (reqFlags & QEXEC_F_TYPED) ? SENDREPLY_FLAG_TYPED : 0;
          flags |= (reqFlags & QEXEC_FORMAT_EXPAND) ? SENDREPLY_FLAG_EXPAND : 0;

          unsigned int apiVersion = sctx->apiVersion;
          if (v && v->t == RSValue_Duo) {
            // Which value to use for duo value
            if (!(flags & SENDREPLY_FLAG_EXPAND)) {
              // STRING
              if (apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST) {
                // Multi
                v = RS_DUOVAL_OTHERVAL(*v);
              } else {
                // Single
                v = RS_DUOVAL_VAL(*v);
              }
            } else {
              // EXPAND
              v = RS_DUOVAL_OTHER2VAL(*v);
            }
          }
          RSValue_SendReply(reply, v, flags);
        }
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
  startPipelineCommon(req->reqConfig.timeoutPolicy, &req->sctx->time.timeout,
                      rp, results, r, rc);
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

static void finishSendChunk(AREQ *req, SearchResult **results, SearchResult *r, bool cursor_done, clock_t duration) {
  if (results) {
    destroyResults(results);
  } else {
    SearchResult_Destroy(r);
  }

  if (cursor_done) {
    req->stateflags |= QEXEC_S_ITERDONE;
  }

  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
  if (QueryError_GetCode(qctx->err) == QUERY_OK || hasTimeoutError(qctx->err)) {
    TotalGlobalStats_CountQuery(AREQ_RequestFlags(req), duration);
  }

  // Reset the total results length:
  qctx->totalResults = 0;
  QueryError_ClearError(qctx->err);
}

/**
 * Sends a chunk of <n> rows in the resp2 format
*/
static void sendChunk_Resp2(AREQ *req, RedisModule_Reply *reply, size_t limit,
  cachedVars cv) {
    SearchResult r = {0};
    int rc = RS_RESULT_EOF;
    QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
    ResultProcessor *rp = qctx->endProc;
    SearchResult **results = NULL;
    long nelem = 0, resultsLen = REDISMODULE_POSTPONED_ARRAY_LEN;
    bool cursor_done = false;

    startPipeline(req, rp, &results, &r, &rc);

    // If an error occurred, or a timeout in strict mode - return a simple error
    if (ShouldReplyWithError(rp->parent->err, req->reqConfig.timeoutPolicy, IsProfile(req))) {
      RedisModule_Reply_Error(reply, QueryError_GetUserError(qctx->err));
      cursor_done = true;
      goto done_2_err;
    } else if (ShouldReplyWithTimeoutError(rc, req->reqConfig.timeoutPolicy, IsProfile(req))) {
      ReplyWithTimeoutError(reply);
      cursor_done = true;
      goto done_2_err;
    }

    // Set `resultsLen` to be the expected number of results in the response.
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
    nelem++;

    // Once we get here, we want to return the results we got from the pipeline (with no error)
    if (AREQ_RequestFlags(req) & QEXEC_F_NOROWS || (rc != RS_RESULT_OK && rc != RS_RESULT_EOF)) {
      goto done_2;
    }

    // If the policy is `ON_TIMEOUT FAIL`, we already aggregated the results
    if (results != NULL) {
      nelem += populateReplyWithResults(reply, results, req, &cv);
      results = NULL;
      goto done_2;
    }

    if (rp->parent->resultLimit && rc == RS_RESULT_OK) {
      nelem += serializeResult(req, reply, &r, &cv);
      SearchResult_Clear(&r);
    } else {
      goto done_2;
    }

    while (--rp->parent->resultLimit && (rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
      nelem += serializeResult(req, reply, &r, &cv);
      SearchResult_Clear(&r);
    }

done_2:
    RedisModule_Reply_ArrayEnd(reply);    // </results>

    cursor_done = (rc != RS_RESULT_OK
                   && !(rc == RS_RESULT_TIMEDOUT
                        && req->reqConfig.timeoutPolicy == TimeoutPolicy_Return));

    bool has_timedout = (rc == RS_RESULT_TIMEDOUT) || hasTimeoutError(qctx->err);

    // Prepare profile printer context
    RedisSearchCtx *sctx = AREQ_SearchCtx(req);
    ProfilePrinterCtx profileCtx = {
      .req = req,
      .timedout = has_timedout,
      .reachedMaxPrefixExpansions = qctx->err->reachedMaxPrefixExpansions,
      .bgScanOOM = sctx->spec && sctx->spec->scan_failed_OOM,
    };

    if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
      if (cursor_done) {
        RedisModule_Reply_LongLong(reply, 0);
        if (IsProfile(req)) {
          req->profile(reply, &profileCtx);
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
      req->profile(reply, &profileCtx);
      RedisModule_Reply_ArrayEnd(reply);
    }

done_2_err:
    finishSendChunk(req, results, &r, cursor_done, clock() - req->initClock);

    if (resultsLen != REDISMODULE_POSTPONED_ARRAY_LEN && rc == RS_RESULT_OK && resultsLen != nelem) {
      RS_LOG_ASSERT_FMT(false, "Failed to predict the number of replied results. Prediction=%ld, actual_number=%ld.", resultsLen, nelem);
    }
}

/**
 * Sends a chunk of <n> rows in the resp3 format
**/
static void sendChunk_Resp3(AREQ *req, RedisModule_Reply *reply, size_t limit,
  cachedVars cv) {
    SearchResult r = {0};
    int rc = RS_RESULT_EOF;
    QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
    RedisSearchCtx *sctx = AREQ_SearchCtx(req);
    ResultProcessor *rp = qctx->endProc;
    SearchResult **results = NULL;
    bool cursor_done = false;

    startPipeline(req, rp, &results, &r, &rc);

    if (ShouldReplyWithError(rp->parent->err, req->reqConfig.timeoutPolicy, IsProfile(req))) {
      RedisModule_Reply_Error(reply, QueryError_GetUserError(qctx->err));
      cursor_done = true;
      goto done_3_err;
    } else if (ShouldReplyWithTimeoutError(rc, req->reqConfig.timeoutPolicy, IsProfile(req))) {
      ReplyWithTimeoutError(reply);
      cursor_done = true;
      goto done_3_err;
    }

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
      RedisModule_ReplyKV_SimpleString(reply, "format", "EXPAND"); // >format
    } else {
      RedisModule_ReplyKV_SimpleString(reply, "format", "STRING"); // >format
    }

    // <results>
    RedisModule_ReplyKV_Array(reply, "results"); // >results

    if (AREQ_RequestFlags(req) & QEXEC_F_NOROWS || (rc != RS_RESULT_OK && rc != RS_RESULT_EOF)) {
      goto done_3;
    }

    if (results != NULL) {
      populateReplyWithResults(reply, results, req, &cv);
      results = NULL;
    } else {
      if (rp->parent->resultLimit && rc == RS_RESULT_OK) {
        serializeResult(req, reply, &r, &cv);
      }

      SearchResult_Clear(&r);
      if (rc != RS_RESULT_OK || !rp->parent->resultLimit) {
        goto done_3;
      }

      while (--rp->parent->resultLimit && (rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
        serializeResult(req, reply, &r, &cv);
        // Serialize it as a search result
        SearchResult_Clear(&r);
      }
    }

done_3:
    RedisModule_Reply_ArrayEnd(reply); // >results

    // <total_results>
    RedisModule_ReplyKV_LongLong(reply, "total_results", qctx->totalResults);

    // <error>
    RedisModule_ReplyKV_Array(reply, "warning"); // >warnings
    if (sctx->spec && sctx->spec->scan_failed_OOM) {
      RedisModule_Reply_SimpleString(reply, QUERY_WINDEXING_FAILURE);
    }
    if (rc == RS_RESULT_TIMEDOUT) {
      RedisModule_Reply_SimpleString(reply, QueryError_Strerror(QUERY_ETIMEDOUT));
    } else if (rc == RS_RESULT_ERROR) {
      // Non-fatal error
      RedisModule_Reply_SimpleString(reply, QueryError_GetUserError(qctx->err));
    } else if (qctx->err->reachedMaxPrefixExpansions) {
      RedisModule_Reply_SimpleString(reply, QUERY_WMAXPREFIXEXPANSIONS);
    }
    RedisModule_Reply_ArrayEnd(reply); // >warnings

    cursor_done = (rc != RS_RESULT_OK
                   && !(rc == RS_RESULT_TIMEDOUT
                        && req->reqConfig.timeoutPolicy == TimeoutPolicy_Return));

    bool has_timedout = (rc == RS_RESULT_TIMEDOUT) || hasTimeoutError(qctx->err);

    // Prepare profile printer context
    ProfilePrinterCtx profileCtx = {
      .req = req,
      .timedout = has_timedout,
      .reachedMaxPrefixExpansions = qctx->err->reachedMaxPrefixExpansions,
      .bgScanOOM = sctx->spec && sctx->spec->scan_failed_OOM,
    };

    if (IsProfile(req)) {
      RedisModule_Reply_MapEnd(reply); // >Results
      if (!(AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) || cursor_done) {
        req->profile(reply, &profileCtx);
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

done_3_err:
    finishSendChunk(req, results, &r, cursor_done, clock() - req->initClock);
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
    .lastLk = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_LAST),
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

void AREQ_Execute(AREQ *req, RedisModuleCtx *ctx) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  sendChunk(req, reply, UINT64_MAX);
  RedisModule_EndReply(reply);
  AREQ_Free(req);
}

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
  if (BCRctx->req) {
    AREQ_Free(BCRctx->req);
  }
  RedisModule_BlockedClientMeasureTimeEnd(BCRctx->blockedClient);
  void *privdata = RedisModule_BlockClientGetPrivateData(BCRctx->blockedClient);
  RedisModule_UnblockClient(BCRctx->blockedClient, privdata);
  WeakRef_Release(BCRctx->spec_ref);
  rm_free(BCRctx);
}

void AREQ_Execute_Callback(blockedClientReqCtx *BCRctx) {
  AREQ *req = blockedClientReqCtx_getRequest(BCRctx);
  RedisModuleCtx *outctx = RedisModule_GetThreadSafeContext(BCRctx->blockedClient);
  QueryError status = {0};

  StrongRef execution_ref = IndexSpecRef_Promote(BCRctx->spec_ref);
  if (!StrongRef_Get(execution_ref)) {
    // The index was dropped while the query was in the job queue.
    // Notify the client that the query was aborted
    QueryError_SetCode(&status, QUERY_EDROPPEDBACKGROUND);
    QueryError_ReplyAndClear(outctx, &status);
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
  QueryError_ReplyAndClear(outctx, &status);

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

  ConcurrentSearchCtx_Init(sctx->redisCtx, &req->conc);
  req->rootiter = QAST_Iterate(ast, opts, sctx, &req->conc, AREQ_RequestFlags(req), status);

  // check possible optimization after creation of IndexIterator tree
  if (IsOptimized(req)) {
    QOptimizer_Iterators(req, req->optimizer);
  }

  if (req->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
    TimedOut_WithStatus(&sctx->time.timeout, status);
  }

  if (QueryError_HasError(status)) {
    return REDISMODULE_ERR;
  }

  if (IsProfile(req)) {
    // Add a Profile iterators before every iterator in the tree
    Profile_AddIters(&req->rootiter);
  }

  clock_t parseClock;
  bool is_profile = IsProfile(req);
  if (is_profile) {
    parseClock = clock();
    req->parseTime = parseClock - req->initClock;
  }

  rc = AREQ_BuildPipeline(req, status);

  if (is_profile) {
    req->pipelineBuildTime = clock() - parseClock;
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
    QueryError_SetWithUserDataFmt(status, QUERY_ENOINDEX, "No such index", " %s", indexname);
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
    AREQ_Free(*r);
    *r = NULL;
    if (thctx) {
      RedisModule_FreeThreadSafeContext(thctx);
    }
  }
  return rc;
}

static void parseProfile(AREQ *r, int execOptions) {
  if (execOptions & EXEC_WITH_PROFILE) {
    AREQ_QueryProcessingCtx(r)->isProfile = true;
    AREQ_AddRequestFlags(r, QEXEC_F_PROFILE);
    if (execOptions & EXEC_WITH_PROFILE_LIMITED) {
      AREQ_AddRequestFlags(r, QEXEC_F_PROFILE_LIMITED);
    }
  } else {
    AREQ_QueryProcessingCtx(r)->isProfile = false;
  }
}

int prepareRequest(AREQ **r_ptr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc, CommandType type, int execOptions, QueryError *status) {
  AREQ *r = *r_ptr;
  // If we got here, we know `argv[0]` is a valid registered command name.
  // If it starts with an underscore, it is an internal command.
  if (RedisModule_StringPtrLen(argv[0], NULL)[0] == '_') {
    AREQ_AddRequestFlags(r, QEXEC_F_INTERNAL);
  }

  parseProfile(r, execOptions);

  if (!IsInternal(r) || IsProfile(r)) {
    // We currently don't need to measure the time for internal and non-profile commands
    r->initClock = clock();
  }

  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
  if (qctx->isProfile) {
    clock_gettime(CLOCK_MONOTONIC, &qctx->initTime);
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

static int buildPipelineAndExecute(AREQ *r, RedisModuleCtx *ctx, QueryError *status) {
  RedisSearchCtx *sctx = AREQ_SearchCtx(r);
  if (RunInThread()) {
    StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);
    RedisModuleBlockedClient* blockedClient = BlockQueryClient(ctx, spec_ref, r, 0);
    blockedClientReqCtx *BCRctx = blockedClientReqCtx_New(r, blockedClient, spec_ref);
    // Mark the request as thread safe, so that the pipeline will be built in a thread safe manner
    AREQ_AddRequestFlags(r, QEXEC_F_RUN_IN_BACKGROUND);
    QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
    if (qctx->isProfile) {
      struct timespec time;
      clock_gettime(CLOCK_MONOTONIC, &time);
      rs_timersub(&time, &qctx->initTime, &time);
      rs_timeradd(&time, &qctx->GILTime, &qctx->GILTime);
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
 * @param execOptions is a bitmask of EXEC_* flags defined in ExecOptions enum.
 */
static int execCommandCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                             CommandType type, int execOptions) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  AREQ *r = AREQ_New();
  QueryError status = {0};

  if (prepareRequest(&r, ctx, argv, argc, type, execOptions, &status) != REDISMODULE_OK) {
    goto error;
  }

  if (buildPipelineAndExecute(r, ctx, &status) != REDISMODULE_OK) {
    goto error;
  }

  return REDISMODULE_OK;

error:
  if (r) {
    AREQ_Free(r);
  }
  return QueryError_ReplyAndClear(ctx, &status);
}

int RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommandCommon(ctx, argv, argc, COMMAND_AGGREGATE, EXEC_NO_FLAGS);
}

int RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommandCommon(ctx, argv, argc, COMMAND_SEARCH, EXEC_NO_FLAGS);
}

#define PROFILE_1ST_PARAM 2

RedisModuleString **_profileArgsDup(RedisModuleString **argv, int argc, int params) {
  RedisModuleString **newArgv = rm_malloc(sizeof(*newArgv) * (argc- params));
  // copy cmd & index
  memcpy(newArgv, argv, PROFILE_1ST_PARAM * sizeof(*newArgv));
  // copy non-profile commands
  memcpy(newArgv + PROFILE_1ST_PARAM, argv + PROFILE_1ST_PARAM + params,
          (argc - PROFILE_1ST_PARAM - params) * sizeof(*newArgv));
  return newArgv;
}

int RSProfileCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }

  CommandType cmdType;
  int curArg = PROFILE_1ST_PARAM;
  int withProfile = EXEC_WITH_PROFILE;

  // Check the command type
  const char *cmd = RedisModule_StringPtrLen(argv[curArg++], NULL);
  if (strcasecmp(cmd, "SEARCH") == 0) {
    cmdType = COMMAND_SEARCH;
  } else if (strcasecmp(cmd, "AGGREGATE") == 0) {
    cmdType = COMMAND_AGGREGATE;
  } else {
    RedisModule_ReplyWithError(ctx, "No `SEARCH` or `AGGREGATE` provided");
    return REDISMODULE_OK;
  }

  cmd = RedisModule_StringPtrLen(argv[curArg++], NULL);
  if (strcasecmp(cmd, "LIMITED") == 0) {
    withProfile |= EXEC_WITH_PROFILE_LIMITED;
    cmd = RedisModule_StringPtrLen(argv[curArg++], NULL);
  }

  if (strcasecmp(cmd, "QUERY") != 0) {
    RedisModule_ReplyWithError(ctx, "The QUERY keyword is expected");
    return REDISMODULE_OK;
  }

  int newArgc = argc - curArg + PROFILE_1ST_PARAM;
  RedisModuleString **newArgv = _profileArgsDup(argv, argc, curArg - PROFILE_1ST_PARAM);
  execCommandCommon(ctx, newArgv, newArgc, cmdType, withProfile);
  rm_free(newArgv);
  return REDISMODULE_OK;
}

char *RS_GetExplainOutput(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          QueryError *status) {
  AREQ *r = AREQ_New();
  if (buildRequest(ctx, argv, argc, COMMAND_EXPLAIN, status, &r) != REDISMODULE_OK) {
    return NULL;
  }
  if (prepareExecutionPlan(r, status) != REDISMODULE_OK) {
    AREQ_Free(r);
    CurrentThread_ClearIndexSpec();
    return NULL;
  }
  char *ret = QAST_DumpExplain(&r->ast, AREQ_SearchCtx(r)->spec);
  AREQ_Free(r);
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
  bool has_map = RedisModule_HasMap(reply);

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

  if (req->stateflags & QEXEC_S_ITERDONE) {
    Cursor_Free(cursor);
  } else {
    // Update the idle timeout
    Cursor_Pause(cursor);
  }
}

static void cursorRead(RedisModule_Reply *reply, Cursor *cursor, size_t count, bool bg) {

  QueryError status = {0};
  AREQ *req = cursor->execState;
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
  qctx->err = &status;
  AREQ_RemoveRequestFlags(req, QEXEC_F_IS_AGGREGATE); // Second read was not triggered by FT.AGGREGATE

  StrongRef execution_ref;
  bool has_spec = cursor_HasSpecWeakRef(cursor);
  // If the cursor is associated with a spec, e.g a coordinator ctx.
  if (has_spec) {
    execution_ref = IndexSpecRef_Promote(cursor->spec_ref);
    if (!StrongRef_Get(execution_ref)) {
      // The index was dropped while the cursor was idle.
      // Notify the client that the query was aborted.
      RedisModule_Reply_Error(reply, "The index was dropped while the cursor was idle");
      return;
    }

    if (HasLoader(req)) { // Quick check if the cursor has loaders.
      QEFlags reqFlags = AREQ_RequestFlags(req);
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

  if (IsProfile(req) || !IsInternal(req)) {
    req->initClock = clock(); // Reset the clock for the current cursor read
  }

  runCursor(reply, cursor, count);
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
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  cursorRead(reply, cr_ctx->cursor, cr_ctx->count, true);
  RedisModule_EndReply(reply);
  RedisModule_FreeThreadSafeContext(ctx);
  RedisModule_BlockedClientMeasureTimeEnd(cr_ctx->bc);
  void *privdata = RedisModule_BlockClientGetPrivateData(cr_ctx->bc);
  RedisModule_UnblockClient(cr_ctx->bc, privdata);
  rm_free(cr_ctx);
}

/**
 * FT.CURSOR READ {index} {CID} {COUNT} [MAXIDLE]
 * FT.CURSOR DEL {index} {CID}
 * FT.CURSOR GC {index}
 */
int RSCursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }

  const char *cmd = RedisModule_StringPtrLen(argv[1], NULL);
  long long cid = 0;
  // argv[0] - FT.CURSOR
  // argv[1] - subcommand
  // argv[2] - index
  // argv[3] - cursor ID

  if (RedisModule_StringToLongLong(argv[3], &cid) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "Bad cursor ID");
    return REDISMODULE_OK;
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  if (strcasecmp(cmd, "READ") == 0) {
    long long count = 0;
    if (argc > 5) {
      // e.g. 'COUNT <timeout>'
      // Verify that the 4'th argument is `COUNT`.
      const char *count_str = RedisModule_StringPtrLen(argv[4], NULL);
      if (strcasecmp(count_str, "count") != 0) {
        RedisModule_ReplyWithErrorFormat(ctx, "Unknown argument `%s`", count_str);
        RedisModule_EndReply(reply);
        return REDISMODULE_OK;
      }

      if (RedisModule_StringToLongLong(argv[5], &count) != REDISMODULE_OK) {
        RedisModule_ReplyWithErrorFormat(ctx, "Bad value for COUNT: `%s`", RedisModule_StringPtrLen(argv[5], NULL));
        RedisModule_EndReply(reply);
        return REDISMODULE_OK;
      }
    }

    Cursor *cursor = Cursors_TakeForExecution(GetGlobalCursor(cid), cid);
    if (cursor == NULL) {
      RedisModule_ReplyWithErrorFormat(ctx, "Cursor not found, id: %d", cid);
      RedisModule_EndReply(reply);
      return REDISMODULE_OK;
    }

    // We have to check that we are not blocked yet from elsewhere (e.g. coordinator)
    if (RunInThread() && !RedisModule_GetBlockedClientHandle(ctx)) {
      CursorReadCtx *cr_ctx = rm_new(CursorReadCtx);
      cr_ctx->bc = BlockCursorClient(ctx, cursor, count, 0);
      cr_ctx->cursor = cursor;
      cr_ctx->count = count;
      workersThreadPool_AddWork((redisearch_thpool_proc)cursorRead_ctx, cr_ctx);
    } else {
      cursorRead(reply, cursor, count, false);
    }
  } else if (strcasecmp(cmd, "DEL") == 0) {
    int rc = Cursors_Purge(GetGlobalCursor(cid), cid);
    if (rc != REDISMODULE_OK) {
      RedisModule_Reply_Error(reply, "Cursor does not exist");
    } else {
      RedisModule_Reply_SimpleString(reply, "OK");
    }
  } else if (strcasecmp(cmd, "GC") == 0) {
    int rc = Cursors_CollectIdle(&g_CursorsList);
    rc += Cursors_CollectIdle(&g_CursorsListCoord);
    RedisModule_Reply_LongLong(reply, rc);
  } else {
    RedisModule_Reply_Error(reply, "Unknown subcommand");
  }
  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}

/* ======================= DEBUG ONLY ======================= */

// FT.DEBUG FT.AGGREGATE idx * <DEBUG_TYPE> <DEBUG_TYPE_ARGS> <DEBUG_TYPE> <DEBUG_TYPE_ARGS> ... DEBUG_PARAMS_COUNT 2
// Example:
// FT.AGGREGATE idx * TIMEOUT_AFTER_N 3 DEBUG_PARAMS_COUNT 2
static int DEBUG_execCommandCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                             CommandType type, int execOptions) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  AREQ *r = NULL;
  // debug_req and &debug_req->r are allocated in the same memory block, so it will be freed
  // when AREQ_Free is called
  QueryError status = {0};
  AREQ_Debug *debug_req = AREQ_Debug_New(argv, argc, &status);
  if (!debug_req) {
    goto error;
  }
  r = &debug_req->r;
  AREQ_Debug_params debug_params = debug_req->debug_params;

  int debug_argv_count = debug_params.debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>` strings
  // Parse the query, not including debug params

  if (prepareRequest(&r, ctx, argv, argc - debug_argv_count, type, execOptions, &status) != REDISMODULE_OK) {
    goto error;
  }

  if (buildPipelineAndExecute(r, ctx, &status) != REDISMODULE_OK) {
    goto error;
  }

  return REDISMODULE_OK;

error:
  if (r) {
    AREQ_Free(r);
  }
  return QueryError_ReplyAndClear(ctx, &status);
}

/**DEBUG COMMANDS - not for production! */
int DEBUG_RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return DEBUG_execCommandCommon(ctx, argv, argc, COMMAND_AGGREGATE, EXEC_DEBUG);
}

int DEBUG_RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return DEBUG_execCommandCommon(ctx, argv, argc, COMMAND_SEARCH, EXEC_DEBUG);
}
