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
#include "pipeline/pipeline.h"
#include "util/units.h"

#include <time.h>

// Serializes a result for the `FT.HYBRID` command.
// The format is consistent, i.e., does not change according to the values of
// the reply, or the RESP protocol used.
static void serializeResult_hybrid(HybridRequest *hreq, RedisModule_Reply *reply, const SearchResult *r,
                              const cachedVars *cv) {
  const uint32_t options = HREQ_RequestFlags(hreq);
  const RSDocumentMetadata *dmd = r->dmd;

  RedisModule_Reply_Map(reply); // >result

  // Reply should have the same structure of an FT.AGGREGATE reply

  if (options & QEXEC_F_SEND_SCORES) {
    RedisModule_Reply_SimpleString(reply, "score");
    if (!(options & QEXEC_F_SEND_SCOREEXPLAIN)) {
      // This will become a string in RESP2
      RedisModule_Reply_Double(reply, r->score);
    } else {
      RedisModule_Reply_Array(reply);
      RedisModule_Reply_Double(reply, r->score);
      SEReply(reply, r->scoreExplain);
      RedisModule_Reply_ArrayEnd(reply);
    }
  }

  if (!(options & QEXEC_F_SEND_NOFIELDS)) {
    const RLookup *lk = cv->lastLk;

    RedisModule_ReplyKV_Map(reply, "attributes"); // >attributes

    if (r->flags & Result_ExpiredDoc) {
      RedisModule_Reply_Null(reply);
    } else {
      RedisSearchCtx *sctx = HREQ_SearchCtx(hreq);
      // Get the number of fields in the reply.
      // Excludes hidden fields, fields not included in RETURN and, score and language fields.
      SchemaRule *rule = (sctx && sctx->spec) ? sctx->spec->rule : NULL;
      int excludeFlags = RLOOKUP_F_HIDDEN;
      int requiredFlags = RLOOKUP_F_NOFLAGS;  //Hybrid does not use RETURN fields; it uses LOAD fields instead
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

        SendReplyFlags flags = (options & QEXEC_F_TYPED) ? SENDREPLY_FLAG_TYPED : 0;
        flags |= (options & QEXEC_FORMAT_EXPAND) ? SENDREPLY_FLAG_EXPAND : 0;

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
    RedisModule_Reply_MapEnd(reply); // >attributes
  }
  RedisModule_Reply_MapEnd(reply); // >result
}

static void startPipelineHybrid(HybridRequest *hreq, ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc) {
  startPipelineCommon(hreq->reqConfig.timeoutPolicy,
          &hreq->hybridParams->aggregationParams.common.sctx->time.timeout,
          rp, results, r, rc);
}

static void finishSendChunk_HREQ(HybridRequest *hreq, SearchResult **results, SearchResult *r, clock_t duration) {
  if (results) {
    destroyResults(results);
  } else {
    SearchResult_Destroy(r);
  }

  // TODO: take to error using HREQ_GetError
  QueryProcessingCtx *qctx = &hreq->tailPipeline->qctx;
  if (QueryError_GetCode(qctx->err) == QUERY_OK || hasTimeoutError(qctx->err)) {
    uint32_t reqflags = HREQ_RequestFlags(hreq);
    TotalGlobalStats_CountQuery(reqflags, duration);
  }

  // Reset the total results length
  qctx->totalResults = 0;
  QueryError_ClearError(qctx->err);
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

static int HREQ_BuildPipelineAndExecute(HybridRequest *hreq, RedisModuleCtx *ctx,
                    RedisSearchCtx *sctx) {
  RedisSearchCtx *sctx1 = hreq->requests[0]->sctx;
  RedisSearchCtx *sctx2 = hreq->requests[1]->sctx;

  if (RunInThread()) {
    // Multi-threaded execution path
    StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);

    // TODO: Dump the entire hreq when explain is implemented
    // Create a dummy AREQ for BlockQueryClient (it expects an AREQ but we'll use the first one)
    AREQ *dummy_req = hreq->requests[0];
    RedisModuleBlockedClient* blockedClient = BlockQueryClient(ctx, spec_ref, dummy_req, 0);

    blockedClientHybridCtx *BCHCtx = blockedClientHybridCtx_New(hreq, blockedClient, spec_ref);

    // TODO mark the hreq as running in the background
    // Mark the requests as thread safe, so that the pipeline will be built in a thread safe manner
    for (size_t i = 0; i < hreq->nrequests; i++) {
      AREQ_AddRequestFlags(hreq->requests[i], QEXEC_F_RUN_IN_BACKGROUND);
    }

    const int rc = workersThreadPool_AddWork((redisearch_thpool_proc)HREQ_Execute_Callback, BCHCtx);
    RS_ASSERT(rc == 0);

    return REDISMODULE_OK;
    } else {
    // Single-threaded execution path

    // Build the pipeline and execute
    if (HybridRequest_BuildPipeline(hreq, hreq->hybridParams) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    } else {
      HREQ_Execute(hreq, ctx, sctx);
      return REDISMODULE_OK;
    }
  }
}

/**
 * Activates the pipeline embedded in `hreq`, and serializes the appropriate
 * response to the client, according to the RESP protocol used (2/3).
 *
 * Note: Currently this is used only by the `FT.HYBRID` command, that does
 * not support cursors and profiling, thus this function does not handle
 * those cases. Support should be added as these features are added.
 */
void sendChunk_hybrid(HybridRequest *hreq, RedisModule_Reply *reply, size_t limit, cachedVars cv) {
    SearchResult r = {0};
    int rc = RS_RESULT_EOF;
    QueryProcessingCtx *qctx = &hreq->tailPipeline->qctx;
    ResultProcessor *rp = qctx->endProc;
    SearchResult **results = NULL;

    // Set the chunk size limit for the query
    rp->parent->resultLimit = limit;

    startPipelineHybrid(hreq, rp, &results, &r, &rc);

    // If an error occurred, or a timeout in strict mode - return a simple error
    if (ShouldReplyWithError(rp->parent->err, hreq->reqConfig.timeoutPolicy, false)) {  // hybrid doesn't support profiling yet
      // TODO: take to error using HREQ_GetError
      RedisModule_Reply_Error(reply, QueryError_GetUserError(qctx->err));
      goto done_err;
    } else if (ShouldReplyWithTimeoutError(rc, hreq->reqConfig.timeoutPolicy, false)) {
      ReplyWithTimeoutError(reply);
      goto done_err;
    }

    RedisModule_Reply_Map(reply);

    // <format>
    QEFlags reqFlags = HREQ_RequestFlags(hreq);
    if (reqFlags & QEXEC_FORMAT_EXPAND) {
      RedisModule_ReplyKV_SimpleString(reply, "format", "EXPAND"); // >format
    } else {
      RedisModule_ReplyKV_SimpleString(reply, "format", "STRING"); // >format
    }

    RedisModule_ReplyKV_Array(reply, "results"); // >results

    if (results != NULL) {
      HREQ_populateReplyWithResults(reply, results, hreq, &cv);
      results = NULL;
    } else {
      if (rp->parent->resultLimit && rc == RS_RESULT_OK) {
        serializeResult_hybrid(hreq, reply, &r, &cv);
      }

      SearchResult_Clear(&r);
      if (rc != RS_RESULT_OK || !rp->parent->resultLimit) {
        goto done;
      }

      while (--rp->parent->resultLimit && (rc = rp->Next(rp, &r)) == RS_RESULT_OK) {
        serializeResult_hybrid(hreq, reply, &r, &cv);
        // Serialize it as a search result
        SearchResult_Clear(&r);
      }
    }

done:
    RedisModule_Reply_ArrayEnd(reply); // >results

    // <total_results>
    RedisModule_ReplyKV_LongLong(reply, "total_results", qctx->totalResults);

    // warnings
    RedisModule_ReplyKV_Array(reply, "warning"); // >warnings
    RedisSearchCtx *sctx = HREQ_SearchCtx(hreq);
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

    // execution_time
    clock_t duration = clock() - hreq->initClock;
    double executionTime = (double)duration / CLOCKS_PER_MILLISEC;
    RedisModule_ReplyKV_Double(reply, "execution_time", executionTime);

    RedisModule_Reply_MapEnd(reply);

done_err:
    finishSendChunk_HREQ(hreq, results, &r, clock() - hreq->initClock);
}

/**
 * Main command handler for FT.HYBRID command.
 *
 * Parses command arguments, builds hybrid request structure, constructs execution pipeline,
 * and prepares for hybrid search execution.
 */
int hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError status = {0};
    QueryError_SetWithUserDataFmt(&status, QUERY_ENOINDEX, "No such index", " %s", indexname);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);
  CurrentThread_SetIndexSpec(spec_ref);

  QueryError status = {0};

  HybridRequest *hybridRequest = parseHybridCommand(ctx, argv, argc, sctx, indexname, &status);
  if (!hybridRequest) {
    goto error;
  }

  if (HREQ_BuildPipelineAndExecute(hybridRequest, ctx, sctx) != REDISMODULE_OK) {
    HREQ_GetError(hybridRequest, &status);
    goto error;
  }

  CurrentThread_ClearIndexSpec();
  return REDISMODULE_OK;

error:
  if (hybridRequest) {
    HybridRequest_Free(hybridRequest);
  }

  CurrentThread_ClearIndexSpec();
  return QueryError_ReplyAndClear(ctx, &status);
}

/**
 * Execute the hybrid search pipeline and send results to the client.
 * This function uses the hybrid-specific result serialization functions.
 *
 * @param hreq The HybridRequest with built pipeline
 * @param ctx Redis module context for sending the reply
 * @param sctx Redis search context
 */
void HREQ_Execute(HybridRequest *hreq, RedisModuleCtx *ctx, RedisSearchCtx *sctx) {
    AGGPlan *plan = &hreq->tailPipeline->ap;
    cachedVars cv = {
        .lastLk = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_LAST),
        .lastAstp = AGPLN_GetArrangeStep(plan)
    };

    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    sendChunk_hybrid(hreq, reply, UINT64_MAX, cv);
    RedisModule_EndReply(reply);
    HybridRequest_Free(hreq);
}
// Background execution functions implementation
blockedClientHybridCtx *blockedClientHybridCtx_New(HybridRequest *hreq,
                                                   RedisModuleBlockedClient *blockedClient,
                                                   StrongRef spec) {
  blockedClientHybridCtx *ret = rm_new(blockedClientHybridCtx);
  ret->hreq = hreq;
  ret->blockedClient = blockedClient;
  ret->spec_ref = StrongRef_Demote(spec);
  return ret;
}

void blockedClientHybridCtx_destroy(blockedClientHybridCtx *BCHCtx) {
  if (BCHCtx->hreq) {
    HybridRequest_Free(BCHCtx->hreq);
  }
  RedisModule_BlockedClientMeasureTimeEnd(BCHCtx->blockedClient);
  void *privdata = RedisModule_BlockClientGetPrivateData(BCHCtx->blockedClient);
  RedisModule_UnblockClient(BCHCtx->blockedClient, privdata);
  WeakRef_Release(BCHCtx->spec_ref);
  rm_free(BCHCtx);
}

void HREQ_Execute_Callback(blockedClientHybridCtx *BCHCtx) {
  HybridRequest *hreq = BCHCtx->hreq;
  RedisModuleCtx *outctx = RedisModule_GetThreadSafeContext(BCHCtx->blockedClient);
  QueryError status = {0};

  StrongRef execution_ref = IndexSpecRef_Promote(BCHCtx->spec_ref);
  if (!StrongRef_Get(execution_ref)) {
    // The index was dropped while the query was in the job queue.
    // Notify the client that the query was aborted
    QueryError_SetCode(&status, QUERY_EDROPPEDBACKGROUND);
    QueryError_ReplyAndClear(outctx, &status);
    RedisModule_FreeThreadSafeContext(outctx);
    blockedClientHybridCtx_destroy(BCHCtx);
    return;
  }

  // Update the main search context with the thread-safe context
  RedisSearchCtx *sctx = hreq->hybridParams->aggregationParams.common.sctx;
  sctx->redisCtx = outctx;

  // Build the pipeline and execute
  if (HybridRequest_BuildPipeline(hreq, hreq->hybridParams) != REDISMODULE_OK) {
    HREQ_GetError(hreq, &status);
    QueryError_ReplyAndClear(outctx, &status);
    // hreq will be freed by blockedClientHybridCtx_destroy since execution failed
  } else {
    // Hybrid query doesn't support cursors.
    HREQ_Execute(hreq, outctx, sctx);
    // Set hreq to NULL so it won't be freed in destroy (it was freed by HREQ_Execute)
    BCHCtx->hreq = NULL;
  }

  RedisModule_FreeThreadSafeContext(outctx);
  IndexSpecRef_Release(execution_ref);
  blockedClientHybridCtx_destroy(BCHCtx);
}
