#include "hybrid/hybrid_request.h"
#include <stdatomic.h>
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_construction.h"
#include "rlookup.h"
#include "rlookup.h"
#include "hybrid/hybrid_scoring.h"
#include "hybrid/hybrid_lookup_context.h"
#include "hybrid/hybrid_lookup_context.h"
#include "hybrid/hybrid_search_result.h"
#include "document.h"
#include "aggregate/aggregate_plan.h"
#include "aggregate/aggregate.h"
#include "rmutil/args.h"
#include "util/workers.h"
#include "cursor.h"
#include "info/info_redis/block_client.h"
#include "query_error_ffi.h"
#include "search_ctx.h"
#include "query_eval_ffi.h"
#include "spec.h"
#include "module.h"
#include "profile/profile.h"
#include "iterators_ffi.h"

#ifdef __cplusplus
extern "C" {
#endif

int HybridRequest_BuildDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params, bool depleteInBackground) {
    // Create synchronization context for coordinating depleter processors
    // This ensures thread-safe access when multiple depleters read from their pipelines
    StrongRef sync_ref = {0};
    int rc = REDISMODULE_OK;
    if (depleteInBackground) {
      sync_ref = DepleterSync_New(req->nrequests, true);
    }

    // Build individual pipelines for each search request
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ *areq = req->requests[i];

        const bool isProfile = IsProfile(areq);
        if (isProfile) {
          // Set initClock right before parsing this specific subquery
          rs_wall_clock_init(&areq->profileClocks.initClock);
        }

        // Pin the per-subquery disk view to the same point in time as the in-memory
        // trie/stats that QAST_Iterate is about to consult. Callers hold the spec read
        // lock at this point (matching AREQ_Execute_Callback / HREQ_Execute_Callback).
        // Aborts the hybrid pipeline if any subquery on a disk index can't take a
        // snapshot — falling back to live reads is unsafe because the parent unlock
        // is unconditional and subsequent depleter / cursor reads would race with GC.
        if (SearchCtx_TakeDiskSnapshot(AREQ_SearchCtx(areq), &req->errors[i]) != REDISMODULE_OK) {
            rc = REDISMODULE_ERR;
            break;
        }

        // Parse subquery: Convert AST to iterator tree
        areq->rootiter = QAST_Iterate(&areq->ast, &areq->searchopts, AREQ_SearchCtx(areq), areq->reqflags, areq, &req->errors[i]);

        rs_wall_clock parseClock;
        if (isProfile) {
          // Add a Profile iterators before every iterator in the tree
          Profile_AddIters(&areq->rootiter);
          // Initialize parseClock after adding profile iterators, we want that to be accounted in the parsing timing
          rs_wall_clock_init(&parseClock);
          // Calculate the time elapsed for subquery parsing (AST to iterator + profile setup)
          areq->profileClocks.profileParseTime = rs_wall_clock_diff_ns(&areq->profileClocks.initClock, &parseClock);
        }

        // Build the complete pipeline for this individual search request
        // This includes indexing (search/scoring) and any request-specific aggregation
        rc = AREQ_BuildPipeline(areq, &req->errors[i]);
        if (isProfile) {
          areq->profileClocks.profilePipelineBuildTime = rs_wall_clock_elapsed_ns(&parseClock);
        }
        if (rc != REDISMODULE_OK) {
            break;
        }

        // Obtain the query processing context for the current AREQ
        QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
        // Set the result limit for the current AREQ - hack for now, should use window value
        if (IsHybridVectorSubquery(areq)){
          qctx->resultLimit = areq->maxAggregateResults;
        } else if (IsHybridSearchSubquery(areq)) {
          qctx->resultLimit = areq->maxSearchResults;
        }
        if (depleteInBackground) {
          // Create a safe depleter processor to extract results from this pipeline
          // The safe depleter will feed results to the hybrid merger
          RedisSearchCtx *nextThread = params->aggregationParams.common.sctx; // We will use the context provided in the params
          RedisSearchCtx *depletingThread = AREQ_SearchCtx(areq); // when constructing the AREQ a new context should have been created
          ResultProcessor *depleter = RPSafeDepleter_New(StrongRef_Clone(sync_ref), depletingThread, nextThread, depleterPool);
          QITR_PushRP(qctx, depleter);
        } else {
          // Foreground depletion (WORKERS == 0): deplete synchronously on the
          // main thread. The depleter takes no spec lock; its upstream
          // query-iterator locks and revalidates against GC on each drain. The
          // caller must NOT hold the spec lock across this, or re-acquisition
          // deadlocks on the non-recursive, writer-preferring rwlock (see
          // buildPipelineAndExecute).
          ResultProcessor *depleter = RPDepleter_New();
          QITR_PushRP(qctx, depleter);
        }
        if (isProfile) {
          // Wrap the depleter with a Profile RP to match the expected end processor type
          ResultProcessor *profileRP = RPProfile_New(qctx->endProc, qctx);
          QITR_PushRP(qctx, profileRP);
        }
    }
    if (depleteInBackground) {
      // Release the sync reference as depleters now hold their own references
      StrongRef_Release(sync_ref);
    }
    return rc;
}

const RLookupKey *OpenMergeScoreKey(RLookup *tailLookup, const char *scoreAlias, QueryError *status) {
    const RLookupKey *scoreKey = NULL;
    if (scoreAlias) {
      scoreKey = RLookup_GetKey_Write(tailLookup, scoreAlias, RLOOKUP_F_NOFLAGS);
      if (!scoreKey) {
        QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_DUP_FIELD, "Could not create score alias, name already exists in query", ", score alias: %s", scoreAlias);
        return NULL;
      }
    } else {
      scoreKey = RLookup_GetKey_Read(tailLookup, UNDERSCORE_SCORE, RLOOKUP_F_NOFLAGS);
    }
    return scoreKey;
}

void HybridRequest_SynchronizeLookupKeys(HybridRequest *req) {
  RLookup *tailLookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
  // Add keys from all source lookups to create unified schema
  for (size_t i = 0; i < req->nrequests; i++) {
    RLookup *srcLookup = AGPLN_GetLookup(AREQ_AGGPlan(req->requests[i]), NULL, AGPLN_GETLOOKUP_FIRST);
    RS_ASSERT(srcLookup);
    RLookup_AddKeysFrom(srcLookup, tailLookup, RLOOKUP_F_NOFLAGS);
  }
}

void HybridPipelineParams_Cleanup(HybridPipelineParams *params) {
    if (!params) {
        return;
    }
    if (params->scoringCtx) {
        HybridScoringContext_Free(params->scoringCtx);
        params->scoringCtx = NULL;
    }
    if (params->explainCtx) {
        HybridExplainContext_Free(params->explainCtx);
        params->explainCtx = NULL;
    }
}

int HybridRequest_BuildMergePipeline(HybridRequest *req, const RLookupKey *scoreKey, HybridPipelineParams *params, QueryError *status) {
    // Array to collect upstream from each individual request pipeline
    arrayof(ResultProcessor*) upstreams = array_new(ResultProcessor *, req->nrequests);
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ *areq = req->requests[i];
        // In profile mode, the end processor must be RP_PROFILE (which wraps the depleter)
        if (IsProfile(req) && areq->pipeline.qctx.endProc->type != RP_PROFILE) {
            QueryError_SetWithoutUserDataFmt(
                status,
                QUERY_ERROR_CODE_GENERIC,
                "Expected %s processor at end of pipeline, found %s",
                RPTypeToString(RP_PROFILE),
                RPTypeToString(areq->pipeline.qctx.endProc->type));
            array_free(upstreams);
            return REDISMODULE_ERR;
        }
        // In non-profile mode, the end processor is either RP_SAFE_DEPLETER (background)
        // or RP_DEPLETER (foreground). Both implement the same Next interface.
        array_ensure_append_1(upstreams, areq->pipeline.qctx.endProc);
    }

    // the doc key is only relevant in coordinator mode, in standalone we can simply use the dmd
    // HybridRequest_SynchronizeLookupKeys copied all the rlookup keys from the upstreams to the tail lookup
    // we open the docKey as hidden in case the user didn't request it, if it already exists it will stay as it was
    // if it didn't then it will be marked as unresolved
    RLookup *tailLookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
    const RLookupKey *docKey = RLookup_GetKey_Read(tailLookup, UNDERSCORE_KEY, RLOOKUP_F_HIDDEN);
    // Pass whether LOAD * is active so RLookupRow_WriteFieldsFrom knows whether
    // to create missing keys
    bool createMissingKeys = (req->reqflags & QEXEC_AGG_LOAD_ALL) != 0;
    HybridLookupContext *lookupCtx = HybridLookupContext_New(req->requests, tailLookup, createMissingKeys);
    HybridExplainContext *explainCtx = params->explainCtx;
    params->explainCtx = NULL; // ownership transferred to merger (built in parseHybridCommand)
    ResultProcessor *merger = RPHybridMerger_New(params->aggregationParams.common.sctx,
                                                 params->scoringCtx, upstreams, req->nrequests,
                                                 docKey, scoreKey, req->subqueriesReturnCodes, lookupCtx,
                                                 explainCtx);
    params->scoringCtx = NULL; // ownership transferred to merger
    QITR_PushRP(&req->tailPipeline->qctx, merger);
    // Build the aggregation part of the tail pipeline for final result processing
    // This handles sorting, filtering, field loading, and output formatting of merged results
    // Skip the index-result copy unless the tail needs it. The tail misses this baseline
    // by skipping Pipeline_BuildQueryPart; BuildAggregationPart flips it back on as needed.
    req->tailPipeline->qctx.skipIndexResultDeepCopy =
        !QEFlags_RequireIndexResultsDownstream(params->aggregationParams.common.reqflags);

    uint32_t stateFlags = 0;
    int rc = Pipeline_BuildAggregationPart(req->tailPipeline, &params->aggregationParams, &stateFlags, status);

    // The tail's matched_terms()/highlighting reads each row's RSIndexResult, but the
    // per-subquery depleters were built earlier with their own skipIndexResultDeepCopy
    // decision and would drop the borrow before the merged row reaches the tail. The
    // flag is read at execution time, so forcing the subqueries to preserve the borrow
    // now reaches those depleters. Only ever force the copy on, never off, so a subquery
    // that independently needs the index result downstream is left untouched.
    if (rc == REDISMODULE_OK && !req->tailPipeline->qctx.skipIndexResultDeepCopy) {
      for (size_t i = 0; i < req->nrequests; i++) {
        req->requests[i]->pipeline.qctx.skipIndexResultDeepCopy = false;
      }
    }
    return rc;
}

int HybridRequest_BuildPipeline(HybridRequest *req, HybridPipelineParams *params, bool depleteInBackground, QueryError *status) {
    // Build the depletion pipeline for extracting results from individual search requests
    if (HybridRequest_BuildDepletionPipeline(req, params, depleteInBackground) != REDISMODULE_OK) {
      for (size_t i = 0; i < req->nrequests; i++) {
        if (QueryError_HasError(&req->errors[i])) {
          QueryError_CloneFrom(&req->errors[i], status);
          break;
        }
      }
      if (!QueryError_HasError(status)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "Failed to build hybrid pipeline");
      }
      return REDISMODULE_ERR;
    }
    RLookup *tailLookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
    // Init lookup since we dont call buildQueryPart
    RLookup_SetCache(tailLookup, IndexSpec_GetSpecCache(req->sctx->spec));

    // Add keys from all source lookups to create unified schema before opening
    // the score key.
    // Skip for 'LOAD *' - keys are created dynamically during loading and will
    // be synchronized lazily in RLookupRow_WriteFieldsFrom when first needed.
    if (!(req->reqflags & QEXEC_AGG_LOAD_ALL)) {
      HybridRequest_SynchronizeLookupKeys(req);
    }

    const RLookupKey *scoreKey = OpenMergeScoreKey(tailLookup, params->aggregationParams.common.scoreAlias, status);
    if (QueryError_HasError(status)) {
      return REDISMODULE_ERR;
    }

    // Build the merge pipeline for combining and processing results from the depletion pipeline
    return HybridRequest_BuildMergePipeline(req, scoreKey, params, status);
}

/**
 * Create a new HybridRequest that manages multiple search requests for hybrid search.
 * This function initializes the hybrid request structure and sets up the tail pipeline
 * that will be used to merge and process results from all individual search requests.
 *
 * @param sctx The search context for the hybrid request.
 * @param requests Array of AREQ pointers representing individual search requests, the hybrid request will take ownership of the array
 * @param nrequests Number of requests in the array
 * @return Newly allocated HybridRequest, or NULL on failure
 */
/**
 * Initialize an already-allocated (zeroed) HybridRequest.
 * Used when the HybridRequest is embedded in another struct (e.g., CoordRequestCtx).
 *
 * @param hybridReq Pointer to zeroed HybridRequest to initialize
 * @param sctx The search context for the hybrid request
 * @param requests Array of AREQ pointers, the hybrid request takes ownership
 * @param nrequests Number of requests in the array
 */
void HybridRequest_Init(HybridRequest *hybridReq, RedisSearchCtx *sctx, AREQ **requests, size_t nrequests) {
    hybridReq->requests = requests;
    hybridReq->nrequests = nrequests;
    hybridReq->sctx = sctx;
    hybridReq->kArgIndex = -1;

    rs_wall_clock now = {0};
    rs_wall_clock_init(&now);

    // Initialize error tracking for each individual request
    hybridReq->errors = array_new(QueryError, nrequests);
    memset(hybridReq->errors, 0, nrequests * sizeof(QueryError));

    // Initialize return codes array for tracking subqueries final states
    hybridReq->subqueriesReturnCodes = rm_calloc(nrequests, sizeof(RPStatus));

    // Initialize the tail pipeline that will merge results from all requests
    hybridReq->tailPipeline = rm_calloc(1, sizeof(Pipeline));
    AGPLN_Init(&hybridReq->tailPipeline->ap);
    hybridReq->tailPipelineError = QueryError_Default();
    Pipeline_Initialize(hybridReq->tailPipeline, requests[0]->pipeline.qctx.timeoutPolicy, &hybridReq->tailPipelineError);

    // Initialize pipelines for each individual request
    for (size_t i = 0; i < nrequests; i++) {
        initializeAREQ(requests[i]);
        hybridReq->errors[i] = QueryError_Default();
        Pipeline_Initialize(&requests[i]->pipeline, requests[i]->reqConfig.timeoutPolicy, &hybridReq->errors[i]);
    }
    hybridReq->profileClocks.initClock = now;

    // Initialize timeout coordination fields
    RequestSyncCtx_Init(&hybridReq->syncCtx);
    pthread_mutex_init(&hybridReq->cursorMutex, NULL);
    hybridReq->storedReplyState.err = QueryError_Default();


}

HybridRequest *HybridRequest_New(RedisSearchCtx *sctx, AREQ **requests, size_t nrequests) {
    HybridRequest *hybridReq = rm_calloc(1, sizeof(*hybridReq));
    HybridRequest_Init(hybridReq, sctx, requests, nrequests);
    return hybridReq;
}

void HybridRequest_InitArgsCursor(HybridRequest *req, ArgsCursor *ac, RedisModuleString **argv, int argc) {
  // skip command and index name
  const int step = argc > 2 ? 2 : argc;
  argv += step;
  argc -= step;
  req->args = rm_calloc(argc, sizeof(*req->args));
  req->nargs = argc;
  // Copy the arguments into an owned array of sds strings
  for (size_t ii = 0; ii < argc; ++ii) {
    size_t n;
    const char *s = RedisModule_StringPtrLen(argv[ii], &n);
    req->args[ii] = sdsnewlen(s, n);
  }

  // Parse the query and basic keywords first..
  ArgsCursor_InitSDS(ac, req->args, req->nargs);
}

/**
 * Free a HybridRequest and all its associated resources.
 * This function properly cleans up all individual AREQ requests, the tail pipeline,
 * error arrays, and the HybridRequest structure itself.
 *
 * @param req The HybridRequest to free
 */
static void HybridRequest_Free(HybridRequest *req) {
    if (!req) return;

    // Cursors should have been freed by the timeout callback or reply callback.
    // If we reach here with cursors still set, it indicates a bug in the cleanup logic.
    RS_ASSERT(req->cursors == NULL);

    // Free all individual AREQ requests and their pipelines.
    //
    // Order matters: AREQ_DecrRef → AREQ_Free → Pipeline_Clean must tear down
    // the subquery's iterators before SearchCtx_Free releases sctx->diskSnapshot,
    // because disk iterators (term/tag/wildcard) borrow the snapshot pointer at
    // construction time. Freeing sctx first would dangle those borrows during
    // iterator teardown. Detach areq->sctx so AREQ_Free leaves it alone, decref
    // to tear down iterators, then free sctx + thctx ourselves.
    for (size_t i = 0; i < req->nrequests; i++) {
      AREQ *areq = req->requests[i];
      RedisModuleCtx *thctx = NULL;
      RedisSearchCtx *sctx = NULL;
      uint32_t reqflags = 0;

      if (areq && areq->sctx && areq->sctx->redisCtx) {
        thctx = areq->sctx->redisCtx;
        sctx = areq->sctx;
        reqflags = areq->reqflags;
        areq->sctx = NULL;
      }

      AREQ_DecrRef(req->requests[i]);

      if (sctx) {
        if (reqflags & QEXEC_F_RUN_IN_BACKGROUND) {
          // Background thread: schedule async cleanup. The scheduled callback
          // runs after the current command completes, so iterator teardown
          // (which happened inside AREQ_DecrRef above) is already done.
          ScheduleContextCleanup(thctx, sctx);
        } else {
          // Main thread: iterators are already torn down by the AREQ_DecrRef
          // above, safe to release the snapshot now.
          SearchCtx_Free(sctx);
          if (thctx) {
            RedisModule_FreeThreadSafeContext(thctx);
          }
        }
      }
    }
    array_free(req->requests);

    array_free_ex(req->errors, QueryError_ClearError((QueryError*)ptr));

    rm_free(req->subqueriesReturnCodes);
    req->subqueriesReturnCodes = NULL;

    // Free the tail search context
    if (req->sctx) {
      SearchCtx_Free(req->sctx);
    }

    // Free the tail pipeline
    if (req->tailPipeline) {
      Pipeline_Clean(req->tailPipeline);
      rm_free(req->tailPipeline);
      req->tailPipeline = NULL;
    }

    // Clean up the tail pipeline error
    QueryError_ClearError(&req->tailPipelineError);

    // Clean up storedReplyState
    ChunkReplyState_Destroy(&req->storedReplyState);

    // Destroy the cursor mutex
    pthread_mutex_destroy(&req->cursorMutex);

    rm_free(req->debugParams);

    RequestSyncCtx_Destroy(&req->syncCtx);

    if (req->args) {
      for (size_t ii = 0; ii < req->nargs; ++ii) {
        sdsfree(req->args[ii]);
      }
      rm_free(req->args);
    }

    rm_free(req);
}

HybridRequest *HybridRequest_IncrRef(HybridRequest *req) {
  __atomic_fetch_add(&req->syncCtx.refcount, 1, __ATOMIC_RELAXED);
  return req;
}

void HybridRequest_DecrRef(HybridRequest *req) {
  // Use ACQ_REL: release ensures our writes are visible before decrement,
  // acquire ensures we see all writes from other threads when refcount reaches 0.
  if (req && !__atomic_sub_fetch(&req->syncCtx.refcount, 1, __ATOMIC_ACQ_REL)) {
    HybridRequest_Free(req);
  }
}

static bool isSoftTailPipelineErrorCode(QueryErrorCode code) {
    return code == QUERY_ERROR_CODE_NO_PROP_VAL;
}

/**
 * Get error information from a HybridRequest.
 * This function checks for errors in priority order:
 * 1. Tail pipeline errors (soft codes skipped — emitted as warnings instead)
 * 2. Individual AREQ errors (sub-query failures)
 *
 * @param hreq The HybridRequest to check for errors
 * @param status QueryError pointer to store error information on failure
 * @return REDISMODULE_OK if no errors found, REDISMODULE_ERR if error found
 */
int HybridRequest_GetError(HybridRequest *hreq, QueryError *status) {
    if (!hreq || !status) {
        return REDISMODULE_ERR;
    }

    // Skip soft codes so the reply path can render them as warnings.
    if (QueryError_HasError(&hreq->tailPipelineError) &&
        !isSoftTailPipelineErrorCode(QueryError_GetCode(&hreq->tailPipelineError))) {
        QueryError_CloneFrom(&hreq->tailPipelineError, status);
        return REDISMODULE_ERR;
    }

    // Priority 2: Individual AREQ errors (sub-query failures)
    for (size_t i = 0; i < hreq->nrequests; i++) {
        if (QueryError_HasError(&hreq->errors[i])) {
            QueryError_CloneFrom(&hreq->errors[i], status);
            return REDISMODULE_ERR;
        }
    }

    // No errors found
    return REDISMODULE_OK;
}

void HybridRequest_ClearErrors(HybridRequest *req) {
  QueryError_ClearError(&req->tailPipelineError);
  for (size_t i = 0; i < req->nrequests; i++) {
    QueryError_ClearError(&req->errors[i]);
  }
}

/**
 * Create a search context with a thread-safe redis module context.
 */
static RedisSearchCtx* createThreadSafeSearchContext(RedisModuleCtx *ctx, const char *indexname) {
  RedisModuleCtx *detachedCtx = RedisModule_GetDetachedThreadSafeContext(ctx);
  RedisModule_SelectDb(detachedCtx, RedisModule_GetSelectedDb(ctx));
  return NewSearchCtxC(detachedCtx, indexname, true);
}

HybridRequest *MakeDefaultHybridRequest(RedisSearchCtx *sctx) {
  extern size_t NumShards;  // Declared in module.c
  AREQ *search = AREQ_New();
  AREQ *vector = AREQ_New();
  const char *indexName = HiddenString_GetUnsafe(sctx->spec->specName, NULL);
  search->sctx = createThreadSafeSearchContext(sctx->redisCtx, indexName);
  vector->sctx = createThreadSafeSearchContext(sctx->redisCtx, indexName);
  arrayof(AREQ*) requests = array_new(AREQ*, HYBRID_REQUEST_NUM_SUBQUERIES);
  requests = array_ensure_append_1(requests, search);
  requests = array_ensure_append_1(requests, vector);
  return HybridRequest_New(sctx, requests, array_len(requests));
}

void AddValidationErrorContext(AREQ *req, QueryError *status) {
  if (QueryError_IsOk(status)) {
    return;
  }

  QEFlags reqFlags = AREQ_RequestFlags(req);

  // Check if this is a hybrid subquery
  bool isHybridVectorSubquery = reqFlags & QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY;
  bool isHybridSearchSubquery = reqFlags & QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY;

  RS_ASSERT (isHybridVectorSubquery ^ isHybridSearchSubquery);
  QueryErrorCode currentCode = QueryError_GetCode(status);

  if (currentCode == QUERY_ERROR_CODE_VECTOR_NOT_ALLOWED) {
    // Enhance generic vector error with hybrid context
    QueryError_ClearError(status);
    if (isHybridVectorSubquery) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_VECTOR_NOT_ALLOWED,
                                       "Vector expressions are not allowed in FT.HYBRID VSIM FILTER");
    } else if (isHybridSearchSubquery) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_VECTOR_NOT_ALLOWED,
                                       "Vector expressions are not allowed in FT.HYBRID SEARCH");
    } // won't reach here
  } else if (currentCode == QUERY_ERROR_CODE_WEIGHT_NOT_ALLOWED) {
    // Enhance generic weight error with hybrid context
    if (isHybridVectorSubquery) {
      QueryError_ClearError(status);
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_WEIGHT_NOT_ALLOWED,
                                       "Weight attributes are not allowed in FT.HYBRID VSIM FILTER");
    }
  }
}

void HybridRequest_SetTimedOut(HybridRequest *req) {
  RequestSyncCtx_SetTimedOut(&req->syncCtx);
  // Propagate to each subquery AREQ so its RPNet's MRChannel_PopWithTimeout
  // abort flag (&areq->syncCtx.timedOut) is flipped. Without this the BG
  // worker can stay parked on the channel even after the hybrid-level flag
  // is set.
  for (size_t i = 0; i < req->nrequests; i++) {
    if (req->requests[i]) {
      AREQ_SetTimedOut(req->requests[i]);
    }
  }
}

bool HybridRequest_TryClaimAggregateResults(HybridRequest *req) {
  bool expected = false;
  return atomic_compare_exchange_strong_explicit(&req->syncCtx.aggregatingResults, &expected, true,
                                                 memory_order_relaxed, memory_order_relaxed);
}

void HybridRequest_SignalAggregateResultsComplete(HybridRequest *req) {
  pthread_mutex_lock(&req->syncCtx.aggregateResultsLock);
  req->syncCtx.aggregateResultsDone = true;
  pthread_cond_broadcast(&req->syncCtx.aggregateResultsCond);
  pthread_mutex_unlock(&req->syncCtx.aggregateResultsLock);
}

void HybridRequest_WaitForAggregateResultsComplete(HybridRequest *req) {
  pthread_mutex_lock(&req->syncCtx.aggregateResultsLock);
  while (!req->syncCtx.aggregateResultsDone) {
    pthread_cond_wait(&req->syncCtx.aggregateResultsCond, &req->syncCtx.aggregateResultsLock);
  }
  pthread_mutex_unlock(&req->syncCtx.aggregateResultsLock);
}

#ifdef __cplusplus
}
#endif
