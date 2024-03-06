/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redismodule.h"
#include "redisearch.h"
#include "search_ctx.h"
#include "aggregate.h"
#include "cursor.h"
#include "rmutil/util.h"
#include "util/timeout.h"
#include "util/workers.h"
#include "score_explain.h"
#include "commands.h"
#include "profile.h"
#include "query_optimizer.h"
#include "resp3.h"

typedef enum { COMMAND_AGGREGATE, COMMAND_SEARCH, COMMAND_EXPLAIN } CommandType;

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
  if ((kk->flags & RLOOKUP_F_SVSRC) && (r->rowdata.sv && r->rowdata.sv->len > kk->svidx)) {
    return r->rowdata.sv->values[kk->svidx];
  } else {
    return RLookup_GetItem(kk, &r->rowdata);
  }
}

/** Cached variables to avoid serializeResult retrieving these each time */
typedef struct {
  RLookup *lastLk;
  const PLN_ArrangeStep *lastAstp;
} cachedVars;

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
  const uint32_t options = req->reqflags;
  const RSDocumentMetadata *dmd = r->dmd;
  size_t count0 = RedisModule_Reply_LocalCount(reply);
  bool has_map = RedisModule_HasMap(reply);

  if (has_map) {
    RedisModule_Reply_Map(reply);
  }

  if (dmd && (options & QEXEC_F_IS_SEARCH)) {
    size_t n;
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
      const RLookupKey *rlk = RLookup_GetKey(cv->lastLk, req->requiredFields[currentField], RLOOKUP_M_READ, RLOOKUP_F_NOFLAGS);
      RSValue *v = rlk ? (RSValue*)getReplyKey(rlk, r) : NULL;
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
        RedisModule_Reply_SimpleString(reply, req->requiredFields[currentField]); // key name
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

    // Get the number of fields in the reply.
    // Excludes hidden fields, fields not included in RETURN and, score and language fields.
    SchemaRule *rule = (req->sctx && req->sctx->spec) ? req->sctx->spec->rule : NULL;
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

        SendReplyFlags flags = (req->reqflags & QEXEC_F_TYPED) ? SENDREPLY_FLAG_TYPED : 0;
        flags |= (req->reqflags & QEXEC_FORMAT_EXPAND) ? SENDREPLY_FLAG_EXPAND : 0;

        unsigned int apiVersion = req->sctx->apiVersion;
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

_out:
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

  if (req->reqflags & QEXEC_F_IS_SEARCH) {
    count++;
  }

  if (req->reqflags & QEXEC_F_SEND_SCORES) {
    count++;
  }

  if (req->reqflags & QEXEC_F_SENDRAWIDS) {
    count++;
  }

  if (req->reqflags & QEXEC_F_SEND_PAYLOADS) {
    count++;
  }

  if (req->reqflags & QEXEC_F_SEND_SORTKEYS) {
    count++;
  }

  if (req->reqflags & QEXEC_F_REQUIRED_FIELDS) {
    count += array_len(req->requiredFields);
    if (req->reqflags & QEXEC_F_SEND_SORTKEYS) {
      count--;
    }
  }

  if (!(req->reqflags & QEXEC_F_SEND_NOFIELDS)) {
    count++;
  }
  return count;
}

/**
 * Sends a chunk of <n> rows, optionally also sending the preamble
 */
void sendChunk(AREQ *req, RedisModule_Reply *reply, size_t limit) {
  size_t nelem = 0;
  SearchResult r = {0};
  int rc = RS_RESULT_EOF;
  ResultProcessor *rp = req->qiter.endProc;
  bool has_map = RedisModule_HasMap(reply);

  if (!(req->reqflags & QEXEC_F_IS_CURSOR) && !(req->reqflags & QEXEC_F_IS_SEARCH)) {
    limit = req->maxAggregateResults;
  }

  cachedVars cv = {0};
  cv.lastLk = AGPLN_GetLookup(&req->ap, NULL, AGPLN_GETLOOKUP_LAST);
  cv.lastAstp = AGPLN_GetArrangeStep(&req->ap);

  // Set the chunk size limit for the query
  rp->parent->resultLimit = limit;

  //-------------------------------------------------------------------------------------------
  if (has_map)  // RESP3 variant
  {
    RedisModule_ReplyKV_Array(reply, "attributes");
    RedisModule_Reply_ArrayEnd(reply);

    rc = rp->Next(rp, &r);
    long resultsLen = REDISMODULE_POSTPONED_ARRAY_LEN;
    if (rc == RS_RESULT_TIMEDOUT && !(req->reqflags & QEXEC_F_IS_CURSOR) && !IsProfile(req) &&
        req->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
      resultsLen = 0;
    } else if (rc == RS_RESULT_ERROR) {
      resultsLen = 0;
    } else if (req->reqflags & QEXEC_F_IS_SEARCH && rc != RS_RESULT_TIMEDOUT &&
               req->optimizer->type != Q_OPT_NO_SORTER) {
      PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(&req->ap);
      size_t reqLimit = arng && arng->isLimited? arng->limit : DEFAULT_LIMIT;
      size_t reqOffset = arng && arng->isLimited? arng->offset : 0;
      size_t resultFactor = getResultsFactor(req);

      size_t expected_res = reqLimit + reqOffset <= req->maxSearchResults ? req->qiter.totalResults : MIN(req->maxSearchResults, req->qiter.totalResults);
      size_t reqResults = expected_res > reqOffset ? expected_res - reqOffset : 0;

      resultsLen = MIN(limit, MIN(reqLimit, reqResults));
    }

    if (IsOptimized(req)) {
      QOptimizer_UpdateTotalResults(req);
    }

    RedisModule_ReplyKV_Array(reply, "error"); // >errors
      if (rc == RS_RESULT_TIMEDOUT) {
        RedisModule_Reply_SimpleString(reply, "Timeout limit was reached");
      } else if (rc == RS_RESULT_ERROR) {
        RedisModule_Reply_Error(reply, QueryError_GetError(req->qiter.err));
        QueryError_ClearError(req->qiter.err);
      }
    RedisModule_Reply_ArrayEnd(reply); // >errors

    if (rc == RS_RESULT_TIMEDOUT) {
      if (!(req->reqflags & QEXEC_F_IS_CURSOR) && !IsProfile(req) &&
          req->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
        RedisModule_ReplyKV_LongLong(reply, "total_results", 0);
      } else {
        rc = RS_RESULT_OK;
        RedisModule_ReplyKV_LongLong(reply, "total_results", req->qiter.totalResults);
      }
    } else if (rc == RS_RESULT_ERROR) {
      RedisModule_ReplyKV_LongLong(reply, "total_results", req->qiter.totalResults);
      nelem++;
    } else {
      RedisModule_ReplyKV_LongLong(reply, "total_results", req->qiter.totalResults);
    }
    nelem++;

    if (req->reqflags & QEXEC_FORMAT_EXPAND) {
      RedisModule_ReplyKV_SimpleString(reply, "format", "EXPAND"); // >format
    } else {
      RedisModule_ReplyKV_SimpleString(reply, "format", "STRING"); // >format
    }

    RedisModule_ReplyKV_Array(reply, "results"); // >results
    nelem = 0;

    if (rc == RS_RESULT_OK && rp->parent->resultLimit && !(req->reqflags & QEXEC_F_NOROWS)) {
      serializeResult(req, reply, &r, &cv);
      nelem++;
    }

    SearchResult_Clear(&r);
    if (rc != RS_RESULT_OK || !rp->parent->resultLimit) {
      goto done_3;
    }

    while (--rp->parent->resultLimit && (rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
      if (!(req->reqflags & QEXEC_F_NOROWS)) {
        serializeResult(req, reply, &r, &cv);
        nelem++;
      }
      // Serialize it as a search result
      SearchResult_Clear(&r);
    }

done_3:
    SearchResult_Destroy(&r);
    if (rc != RS_RESULT_OK) {
      req->stateflags |= QEXEC_S_ITERDONE;
    }

    // Reset the total results length:
    req->qiter.totalResults = 0;
    RedisModule_Reply_ArrayEnd(reply); // >results
  }
  //-------------------------------------------------------------------------------------------
  else // ! has_map (RESP2 variant)
  {
    rc = rp->Next(rp, &r);
    long resultsLen = REDISMODULE_POSTPONED_ARRAY_LEN;
    if (rc == RS_RESULT_TIMEDOUT && !(req->reqflags & QEXEC_F_IS_CURSOR) && !IsProfile(req) &&
        req->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
      resultsLen = 1;
    } else if (rc == RS_RESULT_ERROR) {
      resultsLen = 2;
    } else if (req->reqflags & QEXEC_F_IS_SEARCH && rc != RS_RESULT_TIMEDOUT &&
               req->optimizer->type != Q_OPT_NO_SORTER) {
      PLN_ArrangeStep *arng = AGPLN_GetArrangeStep(&req->ap);
      size_t reqLimit = arng && arng->isLimited? arng->limit : DEFAULT_LIMIT;
      size_t reqOffset = arng && arng->isLimited? arng->offset : 0;
      size_t resultFactor = getResultsFactor(req);

      size_t expected_res = reqLimit + reqOffset <= req->maxSearchResults ? req->qiter.totalResults : MIN(req->maxSearchResults, req->qiter.totalResults);
      size_t reqResults = expected_res > reqOffset ? expected_res - reqOffset : 0;

      resultsLen = 1 + MIN(limit, MIN(reqLimit, reqResults)) * resultFactor;
    }

    RedisModule_Reply_Array(reply); // results @@

    if (IsOptimized(req)) {
      QOptimizer_UpdateTotalResults(req);
    }

    if (rc == RS_RESULT_TIMEDOUT) {
      if (!(req->reqflags & QEXEC_F_IS_CURSOR) && !IsProfile(req) &&
         req->reqConfig.timeoutPolicy == TimeoutPolicy_Fail) {
        RedisModule_Reply_SimpleString(reply, "Timeout limit was reached");
      } else {
        rc = RS_RESULT_OK;
        RedisModule_Reply_LongLong(reply, req->qiter.totalResults);
      }
    } else if (rc == RS_RESULT_ERROR) {
      RedisModule_Reply_LongLong(reply, req->qiter.totalResults);
      RedisModule_Reply_Array(reply);
        // QueryError_ReplyAndClear(reply->ctx, req->qiter.err);
        RedisModule_Reply_Error(reply, QueryError_GetError(req->qiter.err));
        QueryError_ClearError(req->qiter.err);
      RedisModule_Reply_ArrayEnd(reply);
      nelem++;
    } else {
      RedisModule_Reply_LongLong(reply, req->qiter.totalResults);
    }
    nelem++;

    if (rc == RS_RESULT_OK && rp->parent->resultLimit && !(req->reqflags & QEXEC_F_NOROWS)) {
      nelem += serializeResult(req, reply, &r, &cv);
    }

    SearchResult_Clear(&r);
    if (rc != RS_RESULT_OK || !rp->parent->resultLimit) {
      goto done_2;
    }

    while (--rp->parent->resultLimit && (rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
      if (!(req->reqflags & QEXEC_F_NOROWS)) {
        nelem += serializeResult(req, reply, &r, &cv);
      }
      // Serialize it as a search result
      SearchResult_Clear(&r);
    }

  done_2:
    SearchResult_Destroy(&r);
    if (rc != RS_RESULT_OK) {
      req->stateflags |= QEXEC_S_ITERDONE;
    }

    RedisModule_Reply_ArrayEnd(reply); // results
    // Reset the total results length:
    req->qiter.totalResults = 0;
    if (resultsLen == REDISMODULE_POSTPONED_ARRAY_LEN || rc != RS_RESULT_OK) {
      return;
    }
    if (resultsLen != nelem) {
      RedisModule_Log(RSDummyContext, "warning", "Failed predict number of replied, prediction=%ld, actual_number=%ld.", resultsLen, nelem);
      RS_LOG_ASSERT(0, "Precalculated number of replies must be equal to actual number");
    }
  }
  //-------------------------------------------------------------------------------------------
}

void AREQ_Execute(AREQ *req, RedisModuleCtx *ctx) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  if (reply->resp3 || IsProfile(req)) {
    RedisModule_Reply_Map(reply);
  }
    sendChunk(req, reply, -1);
    if (IsProfile(req)) {
      Profile_Print(reply, req);
    }
  if (reply->resp3 || IsProfile(req)) {
    RedisModule_Reply_MapEnd(reply);
  }
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
  RedisModule_UnblockClient(BCRctx->blockedClient, NULL);
  WeakRef_Release(BCRctx->spec_ref);
  rm_free(BCRctx);
}

void AREQ_Execute_Callback(blockedClientReqCtx *BCRctx) {
  AREQ *req = blockedClientReqCtx_getRequest(BCRctx);
  RedisModuleCtx *outctx = RedisModule_GetThreadSafeContext(BCRctx->blockedClient);
  QueryError status = {0}, detailed_status = {0};

  StrongRef execution_ref = WeakRef_Promote(BCRctx->spec_ref);
  if (!StrongRef_Get(execution_ref)) {
    // The index was dropped while the query was in the job queue.
    // Notify the client that the query was aborted
    QueryError_SetError(&status, QUERY_ENOINDEX, "The index was dropped before the query could be executed");
    QueryError_ReplyAndClear(outctx, &status);
    RedisModule_FreeThreadSafeContext(outctx);
    blockedClientReqCtx_destroy(BCRctx);
    return;
  }
  // Cursors are created with a thread-safe context, so we don't want to replace it
  if (!(req->reqflags & QEXEC_F_IS_CURSOR)) {
    req->sctx->redisCtx = outctx;
  }

  // lock spec
  RedisSearchCtx_LockSpecRead(req->sctx);
  if (prepareExecutionPlan(req, &status) != REDISMODULE_OK) {
    goto error;
  }

  if (req->reqflags & QEXEC_F_IS_CURSOR) {
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
  // Enrich the error message that was caught to include the fact that the query ran
  // in a background thread.
  QueryError_SetErrorFmt(&detailed_status, QueryError_GetCode(&status),
                          "The following error was caught upon running the query asynchronously: %s", QueryError_GetError(&status));
  QueryError_ClearError(&status);
  QueryError_ReplyAndClear(outctx, &detailed_status);

cleanup:
  // No need to unlock spec as it was unlocked by `AREQ_Execute` or will be unlocked by `blockedClientReqCtx_destroy`
  RedisModule_FreeThreadSafeContext(outctx);
  StrongRef_Release(execution_ref);
  blockedClientReqCtx_destroy(BCRctx);
}

// Assumes the spec is guarded (by its own lock for read or by the global lock)
int prepareExecutionPlan(AREQ *req, QueryError *status) {
  int rc = REDISMODULE_ERR;
  RedisSearchCtx *sctx = req->sctx;
  RSSearchOptions *opts = &req->searchopts;
  QueryAST *ast = &req->ast;

  // Set timeout for the query execution
  // TODO: this should be done in `AREQ_execute`, but some of the iterators needs the timeout's
  // value and some of the execution begins in `QAST_Iterate`.
  // Setting the timeout context should be done in the same thread that executes the query.
  updateTimeout(&req->timeoutTime, req->reqConfig.queryTimeoutMS);
  sctx->timeout = req->timeoutTime;

  ConcurrentSearchCtx_Init(sctx->redisCtx, &req->conc);
  req->rootiter = QAST_Iterate(ast, opts, sctx, &req->conc, req->reqflags, status);

  // check possible optimization after creation of IndexIterator tree
  if (IsOptimized(req)) {
    QOptimizer_Iterators(req, req->optimizer);
  }

  TimedOut_WithStatus(&req->timeoutTime, status);

  if (QueryError_HasError(status)) {
    return REDISMODULE_ERR;
  }

  if (IsProfile(req)) {
    // Add a Profile iterators before every iterator in the tree
    Profile_AddIters(&req->rootiter);
  }

  hires_clock_t parseClock;
  bool is_profile = IsProfile(req);
  if (is_profile) {
    hires_clock_get(&parseClock);
    req->parseTime += hires_clock_diff_msec(&parseClock, &req->initClock);
  }

  rc = AREQ_BuildPipeline(req, status);

  if (is_profile) {
    req->pipelineBuildTime = hires_clock_since_msec(&parseClock);
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
    (*r)->reqflags |= QEXEC_F_IS_SEARCH;
  }
  else if (type == COMMAND_AGGREGATE) {
    (*r)->reqflags |= QEXEC_F_IS_EXTENDED;
  }

  (*r)->reqflags |= QEXEC_FORMAT_DEFAULT;

  if (AREQ_Compile(*r, argv + 2, argc - 2, status) != REDISMODULE_OK) {
    RS_LOG_ASSERT(QueryError_HasError(status), "Query has error");
    goto done;
  }

  (*r)->protocol = is_resp3(ctx) ? 3 : 2;

  // Prepare the query.. this is where the context is applied.
  if ((*r)->reqflags & QEXEC_F_IS_CURSOR) {
    RedisModuleCtx *newctx = RedisModule_GetDetachedThreadSafeContext(ctx);
    RedisModule_SelectDb(newctx, RedisModule_GetSelectedDb(ctx));
    ctx = thctx = newctx;  // In case of error!
  }

  sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError_SetErrorFmt(status, QUERY_ENOINDEX, "%s: no such index", indexname);
    goto done;
  }

  rc = AREQ_ApplyContext(*r, sctx, status);
  thctx = NULL;
  // ctx is always assigned after ApplyContext
  if (rc != REDISMODULE_OK) {
    RS_LOG_ASSERT(QueryError_HasError(status), "Query has error");
    goto done;
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

#define NO_PROFILE 0
#define PROFILE_FULL 1
#define PROFILE_LIMITED 2

static int parseProfile(AREQ *r, int withProfile, RedisModuleString **argv, int argc, QueryError *status) {
  if (withProfile != NO_PROFILE) {

    // WithCursor is disabled on the shards for external use but is available internally to the coordinator
    #ifndef RS_COORDINATOR
    if (RMUtil_ArgExists("WITHCURSOR", argv, argc, 3)) {
      QueryError_SetError(status, QUERY_EGENERIC, "FT.PROFILE does not support cursor");
      return REDISMODULE_ERR;
    }
    #endif

    r->reqflags |= QEXEC_F_PROFILE;
    if (withProfile == PROFILE_LIMITED) {
      r->reqflags |= QEXEC_F_PROFILE_LIMITED;
    }
    hires_clock_get(&r->initClock);
  }
  return REDISMODULE_OK;
}

static int execCommandCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                             CommandType type, int withProfile) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  AREQ *r = AREQ_New();
  QueryError status = {0};
  if (parseProfile(r, withProfile, argv, argc, &status) != REDISMODULE_OK) {
    goto error;
  }

  // This function also builds the RedisSearchCtx.
  // It will search for the spec according the the name given in the argv array,
  // and ensure the spec is valid.
  if (buildRequest(ctx, argv, argc, type, &status, &r) != REDISMODULE_OK) {
    goto error;
  }

  SET_DIALECT(r->sctx->spec->used_dialects, r->reqConfig.dialectVersion);
  SET_DIALECT(RSGlobalConfig.used_dialects, r->reqConfig.dialectVersion);

#ifdef MT_BUILD
  if (RunInThread()) {
    // Prepare context for the worker thread
    // Since we are still in the main thread, and we already validated the
    // spec'c existence, it is safe to directly get the strong reference from the spec
    // found in buildRequest.
    StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(r->sctx->spec);
    RedisModuleBlockedClient *blockedClient = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    // report block client start time
    RedisModule_BlockedClientMeasureTimeStart(blockedClient);
    blockedClientReqCtx *BCRctx = blockedClientReqCtx_New(r, blockedClient, spec_ref);
    // Mark the request as thread safe, so that the pipeline will be built in a thread safe manner
    r->reqflags |= QEXEC_F_RUN_IN_BACKGROUND;

    workersThreadPool_AddWork((redisearch_thpool_proc)AREQ_Execute_Callback, BCRctx);
  } else
#endif // MT_BUILD
  {
    // Take a read lock on the spec (to avoid conflicts with the GC).
    // This is released in AREQ_Free or while executing the query.
    RedisSearchCtx_LockSpecRead(r->sctx);

    if (prepareExecutionPlan(r, &status) != REDISMODULE_OK) {
      goto error;
    }
    if (r->reqflags & QEXEC_F_IS_CURSOR) {
      // Since we are still in the main thread, and we already validated the
      // spec'c existence, it is safe to directly get the strong reference from the spec
      // found in buildRequest
      StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(r->sctx->spec);
      RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
      int rc = AREQ_StartCursor(r, reply, spec_ref, &status, false);
      RedisModule_EndReply(reply);
      if (rc != REDISMODULE_OK) {
        goto error;
      }
    } else {
      AREQ_Execute(r, ctx);
    }
  }

  return REDISMODULE_OK;

error:
  if (r) {
    AREQ_Free(r);
  }
  return QueryError_ReplyAndClear(ctx, &status);
}

int RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommandCommon(ctx, argv, argc, COMMAND_AGGREGATE, NO_PROFILE);
}

int RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return execCommandCommon(ctx, argv, argc, COMMAND_SEARCH, NO_PROFILE);
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
  int withProfile = PROFILE_FULL;

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
    withProfile = PROFILE_LIMITED;
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
    return NULL;
  }
  char *ret = QAST_DumpExplain(&r->ast, r->sctx->spec);
  AREQ_Free(r);
  return ret;
}

// Assumes that the cursor has a strong ref to the relevant spec and that it is already locked.
int AREQ_StartCursor(AREQ *r, RedisModule_Reply *reply, StrongRef spec_ref, QueryError *err, bool coord) {
  Cursor *cursor = Cursors_Reserve(getCursorList(coord), spec_ref, r->cursorMaxIdle, err);
  if (cursor == NULL) {
    return REDISMODULE_ERR;
  }
  cursor->execState = r;
  runCursor(reply, cursor, 0);
  return REDISMODULE_OK;
}

// Assumes that the cursor has a strong ref to the relevant spec and that it is already locked.
static void runCursor(RedisModule_Reply *reply, Cursor *cursor, size_t num) {
  AREQ *req = cursor->execState;
  bool has_map = RedisModule_HasMap(reply);

  // reset profile clock for cursor reads except for 1st
  if (IsProfile(req) && req->totalTime != 0) {
    hires_clock_get(&req->initClock);
  }

  // update timeout for current cursor read
  if (req->qiter.rootProc->type != RP_NETWORK) {
    updateTimeout(&req->timeoutTime, req->reqConfig.queryTimeoutMS);
    updateRPIndexTimeout(req->qiter.rootProc, req->timeoutTime);
  }
  if (!num) {
    num = req->cursorChunkSize;
    if (!num) {
      num = RSGlobalConfig.cursorReadSize;
    }
  }
  req->cursorChunkSize = num;

  if (has_map) // RESP3
  {
    RedisModule_Reply_Array(reply);
    RedisModule_Reply_Map(reply);
    sendChunk(req, reply, num);
    RedisSearchCtx_UnlockSpec(req->sctx); // Verify that we release the spec lock
    bool cursor_done = !!(req->stateflags & QEXEC_S_ITERDONE);

    // If the cursor is still alive, don't print profile info to save bandwidth
    if (IsProfile(req) && cursor_done) {
      Profile_Print(reply, req);
    }
    RedisModule_Reply_MapEnd(reply);

    RedisModule_Reply_LongLong(reply, cursor_done ? 0 : cursor->id);
    RedisModule_Reply_ArrayEnd(reply);
  }
  else // RESP2
  {
    // return array of [results, cursorID]. (the typical result reply is in the first reply)
    // for profile, we return array of [results, cursorID, profile]
    RedisModule_Reply_Array(reply);
    sendChunk(req, reply, num);
    RedisSearchCtx_UnlockSpec(req->sctx); // Verify that we release the spec lock
    bool cursor_done = !!(req->stateflags & QEXEC_S_ITERDONE);

    if (cursor_done) {
      // Write the count!
      RedisModule_Reply_LongLong(reply, 0);
      if (IsProfile(req)) {
        Profile_Print(reply, req);
      }
    } else {
      RedisModule_Reply_LongLong(reply, cursor->id);
      if (IsProfile(req)) {
        // If the cursor is still alive, don't print profile info to save bandwidth
        RedisModule_Reply_Null(reply);
      }
    }
    RedisModule_Reply_ArrayEnd(reply);
  }

  if (req->stateflags & QEXEC_S_ITERDONE) {
    AREQ_Free(req);
    cursor->execState = NULL;
    Cursor_Free(cursor);
  } else {
    // Update the idle timeout
    Cursor_Pause(cursor);
  }
}

static void cursorRead(RedisModule_Reply *reply, uint64_t cid, size_t count) {
  Cursor *cursor = Cursors_TakeForExecution(GetGlobalCursor(cid), cid);

  if (cursor == NULL) {
    RedisModule_Reply_Error(reply, "Cursor not found");
    return;
  }
  QueryError status = {0};
  StrongRef execution_ref;
  bool has_spec = cursor_HasSpecWeakRef(cursor);
  // If the cursor is associated with a spec, e.g a coordinator ctx.
  if (has_spec) {
    execution_ref = WeakRef_Promote(cursor->spec_ref);
    if (!StrongRef_Get(execution_ref)) {
      // The index was dropped while the cursor was idle.
      // Notify the client that the query was aborted.
      QueryError_SetError(&status, QUERY_ENOINDEX, "The index was dropped while the cursor was idle");
      // QueryError_ReplyAndClear(reply->ctx, &status);
      RedisModule_Reply_Error(reply, QueryError_GetError(&status));
      QueryError_ClearError(&status);
      return;
    }
  }

  AREQ *req = cursor->execState;
  req->qiter.err = &status;

  runCursor(reply, cursor, count);
  if (has_spec) {
    StrongRef_Release(execution_ref);
  }
}

typedef struct {
  RedisModuleBlockedClient *bc;
  uint64_t cid;
  size_t count;
} CursorReadCtx;

static void cursorRead_ctx(CursorReadCtx *cr_ctx) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(cr_ctx->bc);
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  cursorRead(reply, cr_ctx->cid, cr_ctx->count);
  RedisModule_EndReply(reply);
  RedisModule_FreeThreadSafeContext(ctx);
  RedisModule_BlockedClientMeasureTimeEnd(cr_ctx->bc);
  RedisModule_UnblockClient(cr_ctx->bc, NULL);
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

  char cmdc = toupper(*cmd);

  if (cmdc == 'R') {
    long long count = 0;
    if (argc > 5) {
      // e.g. 'COUNT <timeout>'
      if (RedisModule_StringToLongLong(argv[5], &count) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "Bad value for COUNT");
        return REDISMODULE_OK;
      }
    }
#ifdef MT_BUILD
    // We have to check that we are not blocked yet from elsewhere (e.g. coordinator)
    if (RunInThread() && !RedisModule_GetBlockedClientHandle(ctx)) {
      CursorReadCtx *cr_ctx = rm_new(CursorReadCtx);
      cr_ctx->bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
      cr_ctx->cid = cid;
      cr_ctx->count = count;
      RedisModule_BlockedClientMeasureTimeStart(cr_ctx->bc);
      workersThreadPool_AddWork((redisearch_thpool_proc)cursorRead_ctx, cr_ctx);
    } else
#endif
    {
      cursorRead(reply, cid, count);
    }

  } else if (cmdc == 'D') {
    int rc = Cursors_Purge(GetGlobalCursor(cid), cid);
    if (rc != REDISMODULE_OK) {
      RedisModule_Reply_Error(reply, "Cursor does not exist");
    } else {
      RedisModule_Reply_SimpleString(reply, "OK");
    }

  } else if (cmdc == 'G') {
    int rc = Cursors_CollectIdle(&g_CursorsList);
    rc += Cursors_CollectIdle(&g_CursorsListCoord);
    RedisModule_Reply_LongLong(reply, rc);
  } else {
    RedisModule_Reply_Error(reply, "Unknown subcommand");
  }
  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}

void Cursor_FreeExecState(void *p) {
  AREQ *r = p;
  AREQ_Free(p);
}
