#include "hybrid/hybrid_request.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_construction.h"
#include "rlookup.h"
#include "rlookup.h"
#include "hybrid/hybrid_scoring.h"
#include "hybrid/hybrid_lookup_context.h"
#include "hybrid/hybrid_lookup_context.h"
#include "document.h"
#include "aggregate/aggregate_plan.h"
#include "aggregate/aggregate.h"
#include "rmutil/args.h"
#include "util/workers.h"
#include "cursor.h"
#include "info/info_redis/block_client.h"
#include "query_error.h"
#include "spec.h"
#include "module.h"

#ifdef __cplusplus
extern "C" {
#endif

int HybridRequest_BuildDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params) {
    // Create synchronization context for coordinating depleter processors
    // This ensures thread-safe access when multiple depleters read from their pipelines
    StrongRef sync_ref = DepleterSync_New(req->nrequests, params->synchronize_read_locks);

    // Build individual pipelines for each search request
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ *areq = req->requests[i];

        areq->rootiter = QAST_Iterate(&areq->ast, &areq->searchopts, AREQ_SearchCtx(areq), areq->reqflags, &req->errors[i]);

        // Build the complete pipeline for this individual search request
        // This includes indexing (search/scoring) and any request-specific aggregation
        int rc = AREQ_BuildPipeline(areq, &req->errors[i]);
        if (rc != REDISMODULE_OK) {
            StrongRef_Release(sync_ref);
            return REDISMODULE_ERR;
        }

        // Obtain the query processing context for the current AREQ
        QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
        // Set the result limit for the current AREQ - hack for now, should use window value
        if (IsHybridVectorSubquery(areq)){
          qctx->resultLimit = areq->maxAggregateResults;
        } else if (IsHybridSearchSubquery(areq)) {
          qctx->resultLimit = areq->maxSearchResults;
        }
        // Create a depleter processor to extract results from this pipeline
        // The depleter will feed results to the hybrid merger
        RedisSearchCtx *nextThread = params->aggregationParams.common.sctx; // We will use the context provided in the params
        RedisSearchCtx *depletingThread = AREQ_SearchCtx(areq); // when constructing the AREQ a new context should have been created
        ResultProcessor *depleter = RPDepleter_New(StrongRef_Clone(sync_ref), depletingThread, nextThread);
        QITR_PushRP(qctx, depleter);
    }

    // Release the sync reference as depleters now hold their own references
    StrongRef_Release(sync_ref);
    return REDISMODULE_OK;
}

const RLookupKey *OpenMergeScoreKey(RLookup *tailLookup, const char *scoreAlias, QueryError *status) {
    const RLookupKey *scoreKey = NULL;
    if (scoreAlias) {
      scoreKey = RLookup_GetKey_Write(tailLookup, scoreAlias, RLOOKUP_F_NOFLAGS);
      if (!scoreKey) {
        QueryError_SetWithUserDataFmt(status, QUERY_EDUPFIELD, "Could not create score alias, name already exists in query", ", score alias: %s", scoreAlias);
        return NULL;
      }
    } else {
      scoreKey = RLookup_GetKey_Read(tailLookup, UNDERSCORE_SCORE, RLOOKUP_F_NOFLAGS);
    }
    return scoreKey;
}

int HybridRequest_BuildMergePipeline(HybridRequest *req, HybridLookupContext *lookupCtx, const RLookupKey *scoreKey, HybridPipelineParams *params) {
    // Array to collect depleter processors from each individual request pipeline
    arrayof(ResultProcessor*) depleters = array_new(ResultProcessor *, req->nrequests);
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ *areq = req->requests[i];
        if (areq->pipeline.qctx.endProc->type != RP_DEPLETER) {
            array_free(depleters);
            return REDISMODULE_ERR;
        }
        array_ensure_append_1(depleters, areq->pipeline.qctx.endProc);
    }

    RLookup *tailLookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);

    // the doc key is only relevant in coordinator mode, in standalone we can simply use the dmd
    // InitializeHybridLookupContext copied all the rlookup keys from the upstreams to the tail lookup
    // we open the docKey as hidden in case the user didn't request it, if it already exists it will stay as it was
    // if it didn't then it will be marked as unresolved
    const RLookupKey *docKey = RLookup_GetKey_Read(tailLookup, UNDERSCORE_KEY, RLOOKUP_F_HIDDEN);
    ResultProcessor *merger = RPHybridMerger_New(params->scoringCtx, depleters, req->nrequests, docKey, scoreKey, req->subqueriesReturnCodes, lookupCtx);
    params->scoringCtx = NULL; // ownership transferred to merger
    QITR_PushRP(&req->tailPipeline->qctx, merger);
    // Build the aggregation part of the tail pipeline for final result processing
    // This handles sorting, filtering, field loading, and output formatting of merged results
    uint32_t stateFlags = 0;
    int rc = Pipeline_BuildAggregationPart(req->tailPipeline, &params->aggregationParams, &stateFlags);
    return rc;
}

int HybridRequest_BuildPipeline(HybridRequest *req, HybridPipelineParams *params) {
    // Build the depletion pipeline for extracting results from individual search requests
    if (HybridRequest_BuildDepletionPipeline(req, params) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
    RLookup *tailLookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
    // Init lookup since we dont call buildQueryPart
    RLookup_Init(tailLookup, IndexSpec_GetSpecCache(req->sctx->spec));

    // a lookup construct to help us translate an upstream rlookup to the tail lookup
    // Assumes all upstreams have non-null lookups
    HybridLookupContext *lookupCtx = InitializeHybridLookupContext(req->requests, tailLookup);

    const RLookupKey *scoreKey = OpenMergeScoreKey(tailLookup, params->aggregationParams.common.scoreAlias, &req->tailPipelineError);
    if (QueryError_HasError(&req->tailPipelineError)) {
      return REDISMODULE_ERR;
    }

    // Build the merge pipeline for combining and processing results from the depletion pipeline
    return HybridRequest_BuildMergePipeline(req, lookupCtx, scoreKey, params);
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
HybridRequest *HybridRequest_New(RedisSearchCtx *sctx, AREQ **requests, size_t nrequests) {
    HybridRequest *hybridReq = rm_calloc(1, sizeof(*hybridReq));
    hybridReq->requests = requests;
    hybridReq->nrequests = nrequests;
    hybridReq->sctx = sctx;

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
    hybridReq->initClock = clock();
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
void HybridRequest_Free(HybridRequest *req) {
    if (!req) return;

    // Free all individual AREQ requests and their pipelines
    for (size_t i = 0; i < req->nrequests; i++) {

      // Check if we need to manually free the thread-safe context
      AREQ *areq = req->requests[i];
      if (areq && areq->sctx && areq->sctx->redisCtx) {
        RedisModuleCtx *thctx = areq->sctx->redisCtx;
        RedisSearchCtx *sctx = areq->sctx;
        extern size_t NumShards;  // Declared in module.c

        if (NumShards > 1) {
          // Cluster mode: contexts are not detached, just free the search context
          // The Redis context belongs to the command handler and will be freed by the framework
          SearchCtx_Free(sctx);
        } else {
          // Standalone mode: handle detached contexts
          if (RunInThread()) {
            // Background thread: schedule async cleanup
            ScheduleContextCleanup(thctx, sctx);
          } else {
            // Main thread: safe to free directly
            SearchCtx_Free(sctx);
            if (thctx) {
              RedisModule_FreeThreadSafeContext(thctx);
            }
          }
        }

        areq->sctx = NULL;
      }

      AREQ_Free(req->requests[i]);
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
    if (req->args) {
      for (size_t ii = 0; ii < req->nargs; ++ii) {
        sdsfree(req->args[ii]);
      }
      rm_free(req->args);
    }

    rm_free(req);
}

/**
 * Get error information from a HybridRequest.
 * This function checks for errors in priority order:
 * 1. Tail pipeline errors (affects final result processing)
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

    // Priority 1: Tail pipeline error (affects final result processing)
    if (QueryError_HasError(&hreq->tailPipelineError)) {
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
 * Create a search context, detached only when necessary.
 * In cluster mode, we're already on a background thread, so no need for detached context.
 */
static RedisSearchCtx* createThreadSafeSearchContext(RedisModuleCtx *ctx, const char *indexname, size_t NumShards) {
  if (NumShards > 1) {
    // Cluster mode: we're already on DIST_THREADPOOL, use the existing context directly
    return NewSearchCtxC(ctx, indexname, true);
  } else {
    // Standalone mode: create detached context for thread safety
    RedisModuleCtx *detachedCtx = RedisModule_GetDetachedThreadSafeContext(ctx);
    RedisModule_SelectDb(detachedCtx, RedisModule_GetSelectedDb(ctx));
    return NewSearchCtxC(detachedCtx, indexname, true);
  }
}

HybridRequest *MakeDefaultHybridRequest(RedisSearchCtx *sctx) {
  extern size_t NumShards;  // Declared in module.c
  AREQ *search = AREQ_New();
  AREQ *vector = AREQ_New();
  const char *indexName = HiddenString_GetUnsafe(sctx->spec->specName, NULL);
  search->sctx = createThreadSafeSearchContext(sctx->redisCtx, indexName, NumShards);
  vector->sctx = createThreadSafeSearchContext(sctx->redisCtx, indexName, NumShards);
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

  if (currentCode == QUERY_EVECTOR_NOT_ALLOWED) {
    // Enhance generic vector error with hybrid context
    QueryError_ClearError(status);
    if (isHybridVectorSubquery) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_EVECTOR_NOT_ALLOWED,
                                       "Vector expressions are not allowed in FT.HYBRID VSIM FILTER");
    } else if (isHybridSearchSubquery) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_EVECTOR_NOT_ALLOWED,
                                       "Vector expressions are not allowed in FT.HYBRID SEARCH");
    } // won't reach here
  } else if (currentCode == QUERY_EWEIGHT_NOT_ALLOWED) {
    // Enhance generic weight error with hybrid context
    if (isHybridVectorSubquery) {
      QueryError_ClearError(status);
      QueryError_SetWithoutUserDataFmt(status, QUERY_EWEIGHT_NOT_ALLOWED,
                                       "Weight attributes are not allowed in FT.HYBRID VSIM FILTER");
    }
  }
}

#ifdef __cplusplus
}
#endif
