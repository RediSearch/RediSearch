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
#include "value.h"
#include "module.h"
#include "aggregate/reply_empty.h"

#include <time.h>

#define SEARCH_SUFFIX "(SEARCH)"
#define VSIM_SUFFIX "(VSIM)"
#define POST_PROCESSING_SUFFIX "(POST PROCESSING)"

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
    ReplyWarning(reply, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT), suffix);
    timeoutOccurred = true;
  } else if (returnCode == RS_RESULT_ERROR) {
    // Non-fatal error
    ReplyWarning(reply, QueryError_GetUserError(err), suffix);
  } else if (QueryError_HasReachedMaxPrefixExpansionsWarning(err)) {
    ReplyWarning(reply, QUERY_WMAXPREFIXEXPANSIONS, suffix);
  }

  return timeoutOccurred;
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
      int excludeFlags = RLOOKUP_F_HIDDEN;
      int requiredFlags = RLOOKUP_F_NOFLAGS;  //Hybrid does not use RETURN fields; it uses LOAD fields instead
      int skipFieldIndex[lk->rowlen]; // Array has `0` for fields which will be skipped
      memset(skipFieldIndex, 0, lk->rowlen * sizeof(*skipFieldIndex));
      size_t nfields = RLookup_GetLength(lk, SearchResult_GetRowData(r), skipFieldIndex, requiredFlags, excludeFlags, rule);

      int i = 0;
      for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
        if (!kk->name || !skipFieldIndex[i++]) {
          continue;
        }
        const RSValue *v = RLookup_GetItem(kk, SearchResult_GetRowData(r));
        RS_LOG_ASSERT(v, "v was found in RLookup_GetLength iteration")

        RedisModule_Reply_StringBuffer(reply, kk->name, kk->name_len);

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
      }
    }
  }
  RedisModule_Reply_MapEnd(reply); // >result
}

static void startPipelineHybrid(HybridRequest *hreq, ResultProcessor *rp, SearchResult ***results, SearchResult *r, int *rc) {
  CommonPipelineCtx ctx = {
    .timeoutPolicy = hreq->reqConfig.timeoutPolicy,
    .timeout = &hreq->sctx->time.timeout,
    .oomPolicy = hreq->reqConfig.oomPolicy,
  };
  startPipelineCommon(&ctx, rp, results, r, rc);
}

static void finishSendChunk_HREQ(HybridRequest *hreq, SearchResult **results, SearchResult *r, clock_t duration, QueryError *err) {
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
 * Activates the pipeline embedded in `hreq`, and serializes the appropriate
 * response to the client, according to the RESP protocol used (2/3).
 *
 * Note: Currently this is used only by the `FT.HYBRID` command, that does
 * not support cursors and profiling, thus this function does not handle
 * those cases. Support should be added as these features are added.
 *
 * @param hreq The hybrid request with built pipeline
 * @param reply Redis module reply object
 * @param limit Maximum number of results to return
 * @param cv Cached variables for result processing
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
    QueryError err = QueryError_Default();
    HybridRequest_GetError(hreq, &err);
    HybridRequest_ClearErrors(hreq);
    if (ShouldReplyWithError(QueryError_GetCode(&err), hreq->reqConfig.timeoutPolicy, false)) {
      RedisModule_Reply_Error(reply, QueryError_GetUserError(&err));
      goto done_err;
    } else if (ShouldReplyWithTimeoutError(rc, hreq->reqConfig.timeoutPolicy, false)) {
      ReplyWithTimeoutError(reply);
      goto done_err;
    }

    RedisModule_Reply_Map(reply);

    // <total_results>
    RedisModule_ReplyKV_LongLong(reply, "total_results", qctx->totalResults);

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

    // warnings
    RedisModule_ReplyKV_Array(reply, "warnings"); // >warnings
    RedisSearchCtx *sctx = HREQ_SearchCtx(hreq);
    if (sctx->spec && sctx->spec->scan_failed_OOM) {
      RedisModule_Reply_SimpleString(reply, QUERY_WINDEXING_FAILURE);
    }
    if (QueryError_HasQueryOOMWarning(qctx->err)) {
      // Cluster mode only: handled directly here instead of through handleAndReplyWarning()
      // because this warning is not related to subqueries or post-processing terminology
      RedisModule_Reply_SimpleString(reply, QUERY_WOOM_CLUSTER);
    }

    replyWarningsWithSuffixes(reply, hreq, qctx, rc);

    RedisModule_Reply_ArrayEnd(reply); // >warnings

    // execution_time
    clock_t duration = clock() - hreq->initClock;
    double executionTime = (double)duration / CLOCKS_PER_MILLISEC;
    RedisModule_ReplyKV_Double(reply, "execution_time", executionTime);

    RedisModule_Reply_MapEnd(reply);

done_err:
    finishSendChunk_HREQ(hreq, results, &r, clock() - hreq->initClock, &err);
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
    RedisModule_Reply_SimpleString(reply, "warnings");
    if (QueryError_HasQueryOOMWarning(err)) {
        RedisModule_Reply_Array(reply);
        RedisModule_Reply_SimpleString(reply, QUERY_WOOM_CLUSTER);
        RedisModule_Reply_ArrayEnd(reply);
    } else {
        RedisModule_Reply_EmptyArray(reply);
    }

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
  HybridRequest_Free((HybridRequest *)ptr);
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

static inline void replyWithCursors(RedisModuleCtx *replyCtx, arrayof(Cursor*) cursors) {
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
        // This should never happen, we currently only support SEARCH and VSIM subqueries
        RS_ABORT_ALWAYS("Unknown subquery type");
      }
    }
    // Add warnings array
    RedisModule_ReplyKV_Array(reply, "warnings"); // >warnings
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
    // helper array to collect depleters so in async we can deplete them all at once before returning the cursors
    arrayof(ResultProcessor*) depleters = NULL;
    if (backgroundDepletion) {
      depleters = array_new(ResultProcessor *, req->nrequests);
    }
    arrayof(Cursor*) cursors = array_new(Cursor*, req->nrequests);
    for (size_t i = 0; i < req->nrequests; i++) {
      AREQ *areq = req->requests[i];
      if (backgroundDepletion) {
        if (areq->pipeline.qctx.endProc->type != RP_DEPLETER) {
          break;
        }
        array_ensure_append_1(depleters, areq->pipeline.qctx.endProc);
      }
      Cursor *cursor = Cursors_Reserve(getCursorList(false), areq->sctx->spec->own_ref, areq->cursorConfig.maxIdle, status);
      if (!cursor) {
        break;
      }
      // The cursor lifetime will determine the hybrid request lifetime
      cursor->execState = areq;
      cursor->hybrid_ref = StrongRef_Clone(hybrid_ref);
      areq->cursor_id = cursor->id;
      array_ensure_append_1(cursors, cursor);
    }

    if (array_len(cursors) != req->nrequests) {
      array_free_ex(cursors, Cursor_Free(*(Cursor**)ptr));
      if (depleters) {
        array_free(depleters);
      }
      // verify error exists
      RS_ASSERT(QueryError_HasError(status));
      return REDISMODULE_ERR;
    }

    if (backgroundDepletion) {
      int rc = RPDepleter_DepleteAll(depleters);
      array_free(depleters);
      if (rc != RS_RESULT_OK) {
        array_free_ex(cursors, Cursor_Free(*(Cursor**)ptr));
        if (rc == RS_RESULT_TIMEDOUT) {
          QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_TIMED_OUT, "Depleting timed out");
        } else {
          QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_GENERIC, "Failed to deplete set of results, rc=%d", rc);
        }
        return REDISMODULE_ERR;
      }
    }
    replyWithCursors(replyCtx, cursors);
    array_free(cursors);
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

  if (!isCursor) {
    HybridRequest_Execute(hreq, ctx, sctx);
  } else if (HybridRequest_StartCursors(hybrid_ref, ctx, status, depleteInBackground) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  freeHybridParams(hybridParams);
  return REDISMODULE_OK;
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
  if (RunInThread()) {
    // Multi-threaded execution path
    StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);

    // TODO: Dump the entire hreq when explain is implemented
    // Create a dummy AREQ for BlockQueryClient (it expects an AREQ but we'll use the first one)
    AREQ *dummy_req = hreq->requests[0];
    RedisModuleBlockedClient* blockedClient = BlockQueryClient(ctx, spec_ref, dummy_req, 0);

    blockedClientHybridCtx *BCHCtx = blockedClientHybridCtx_New(StrongRef_Clone(hybrid_ref), hybridParams, blockedClient, spec_ref, internal);

    // Mark the hreq as running in the background
    hreq->reqflags |= QEXEC_F_RUN_IN_BACKGROUND;
    // Mark the requests as thread safe, so that the pipeline will be built in a thread safe manner
    for (size_t i = 0; i < hreq->nrequests; i++) {
      AREQ_AddRequestFlags(hreq->requests[i], QEXEC_F_RUN_IN_BACKGROUND);
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
static inline int CleanupAndReplyStatus(RedisModuleCtx *ctx, StrongRef hybrid_ref, HybridPipelineParams *hybridParams, QueryError *status) {
    freeHybridParams(hybridParams);
    DefaultCleanup(hybrid_ref);
    // Update global query errors, this path is only used for SA and internal, both are considered shards.
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(status), 1, SHARD_ERR_WARN);
    return QueryError_ReplyAndClear(ctx, status);
}

/**
 * Main command handler for FT.HYBRID command.
 *
 * Parses command arguments, builds hybrid request structure, constructs execution pipeline,
 * and prepares for hybrid search execution.
 */
int hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool internal) {
  // Index name is argv[1]
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  QueryError status = QueryError_Default();

  // Memory guardrail
  if (QueryMemoryGuard(ctx)) {
    if (RSGlobalConfig.requestConfigParams.oomPolicy == OomPolicy_Fail) {
      return QueryMemoryGuardFailure_WithReply(ctx);
    }
    // Assuming OOM policy is return since we didn't ignore the memory guardrail
    RS_ASSERT(RSGlobalConfig.requestConfigParams.oomPolicy == OomPolicy_Return);
    return common_hybrid_query_reply_empty(ctx, QUERY_ERROR_CODE_OUT_OF_MEMORY, internal);
  }

  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError_SetWithUserDataFmt(&status, QUERY_ERROR_CODE_NO_INDEX, "No such index", " %s", indexname);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sctx->spec);
  CurrentThread_SetIndexSpec(spec_ref);

  HybridRequest *hybridRequest = MakeDefaultHybridRequest(sctx);
  StrongRef hybrid_ref = StrongRef_New(hybridRequest, &FreeHybridRequest);
  HybridPipelineParams hybridParams = {0};

  ParseHybridCommandCtx cmd = {0};
  cmd.search = hybridRequest->requests[SEARCH_INDEX];
  cmd.vector = hybridRequest->requests[VECTOR_INDEX];
  cmd.reqConfig = &hybridRequest->reqConfig;
  cmd.cursorConfig = &hybridRequest->cursorConfig;
  cmd.hybridParams = rm_calloc(1, sizeof(HybridPipelineParams));
  cmd.tailPlan = &hybridRequest->tailPipeline->ap;

  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hybridRequest, &ac, argv, argc);

  if (parseHybridCommand(ctx, &ac, sctx, &cmd, &status, internal) != REDISMODULE_OK) {
    return CleanupAndReplyStatus(ctx, hybrid_ref, cmd.hybridParams, &status);
  }

  for (int i = 0; i < hybridRequest->nrequests; i++) {
    AREQ *subquery = hybridRequest->requests[i];
    SearchCtx_UpdateTime(AREQ_SearchCtx(subquery), hybridRequest->reqConfig.queryTimeoutMS);
  }
  SearchCtx_UpdateTime(hybridRequest->sctx, hybridRequest->reqConfig.queryTimeoutMS);

  if (HybridRequest_BuildPipelineAndExecute(hybrid_ref, cmd.hybridParams, ctx, hybridRequest->sctx, &status, internal) != REDISMODULE_OK) {
    HybridRequest_GetError(hybridRequest, &status);
    HybridRequest_ClearErrors(hybridRequest);
    return CleanupAndReplyStatus(ctx, hybrid_ref, cmd.hybridParams, &status);
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
    QueryError_ReplyAndClear(outctx, &status);
    RedisModule_FreeThreadSafeContext(outctx);
    blockedClientHybridCtx_destroy(BCHCtx);
    return;
  }

  RedisSearchCtx *sctx = HREQ_SearchCtx(hreq);
  if (!(hreq->reqflags & QEXEC_F_IS_CURSOR)) {
    // Update the main search context with the thread-safe context
    sctx->redisCtx = outctx;
  }

  if (buildPipelineAndExecute(hybrid_ref, hybridParams, outctx, sctx, &status, BCHCtx->internal, true) == REDISMODULE_OK) {
    // Set hybridParams to NULL so they won't be freed in destroy
    BCHCtx->hybridParams = NULL;
  } else if (QueryError_HasError(&status)) {
    QueryError_ReplyAndClear(outctx, &status);
  }
  RedisModule_FreeThreadSafeContext(outctx);
  IndexSpecRef_Release(execution_ref);
  blockedClientHybridCtx_destroy(BCHCtx);
}
