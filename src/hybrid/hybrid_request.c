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

/**
 * Initialize unified lookup schema and hybrid lookup context for field merging.
 *
 * @param requests Array of AREQ pointers containing source lookups (non-null)
 * @param tailLookup The destination lookup to populate with unified schema (non-null)
 * @return HybridLookupContext* to an initialized HybridLookupContext
 */
static HybridLookupContext *InitializeHybridLookupContext(arrayof(AREQ*) requests, RLookup *tailLookup) {
    RS_ASSERT(requests && tailLookup);
    size_t nrequests = array_len(requests);

    // Build lookup context for field merging
    HybridLookupContext *lookupCtx = rm_calloc(1, sizeof(HybridLookupContext));
    lookupCtx->tailLookup = tailLookup;
    lookupCtx->sourceLookups = array_newlen(const RLookup*, nrequests);

    // Add keys from all source lookups to create unified schema
    for (size_t i = 0; i < nrequests; i++) {
        RLookup *srcLookup = AGPLN_GetLookup(AREQ_AGGPlan(requests[i]), NULL, AGPLN_GETLOOKUP_FIRST);
        RS_ASSERT(srcLookup);
        RLookup_AddKeysFrom(srcLookup, tailLookup, RLOOKUP_F_NOFLAGS);
        lookupCtx->sourceLookups[i] = srcLookup;
    }

    return lookupCtx;
}

int HybridRequest_BuildMergePipeline(HybridRequest *req, HybridPipelineParams *params) {
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

    // Assumes all upstreams have non-null lookups
     // Init lookup since we dont call buildQueryPart
    RLookup *lookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
    RLookup_Init(lookup, IndexSpec_GetSpecCache(req->sctx->spec));
    HybridLookupContext *lookupCtx = InitializeHybridLookupContext(req->requests, lookup);
    const char *scoreAlias = params->aggregationParams.common.scoreAlias;
    const RLookupKey *docKey = RLookup_GetKey_Read(lookup, UNDERSCORE_KEY, RLOOKUP_F_NOFLAGS);
    const RLookupKey *scoreKey = NULL;
    if (scoreAlias) {
      scoreKey = RLookup_GetKey_Write(lookup, scoreAlias, RLOOKUP_F_NOFLAGS);
      if (!scoreKey) {
        array_free(depleters);
        QueryError_SetWithUserDataFmt(&req->tailPipelineError, QUERY_EDUPFIELD, "Could not create score alias, name already exists in query", ", score alias: %s", scoreAlias);
        return REDISMODULE_ERR;
      }
    } else {
      scoreKey = RLookup_GetKey_Read(lookup, UNDERSCORE_SCORE, RLOOKUP_F_HIDDEN);
    }
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
    // Build the merge pipeline for combining and processing results from the depletion pipeline
    return HybridRequest_BuildMergePipeline(req, params);
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
    QueryError_Init(&hybridReq->tailPipelineError);
    Pipeline_Initialize(hybridReq->tailPipeline, requests[0]->pipeline.qctx.timeoutPolicy, &hybridReq->tailPipelineError);

    // Initialize pipelines for each individual request
    for (size_t i = 0; i < nrequests; i++) {
        initializeAREQ(requests[i]);
        QueryError_Init(&hybridReq->errors[i]);
        Pipeline_Initialize(&requests[i]->pipeline, requests[i]->reqConfig.timeoutPolicy, &hybridReq->errors[i]);
    }
    hybridReq->initClock = clock();
    return hybridReq;
}

void HybridRequest_InitArgsCursor(HybridRequest *req, ArgsCursor *ac, RedisModuleString **argv, int argc) {
   // skip command and index name
  argv += 2;
  argc -= 2;
  req->args = rm_malloc(sizeof(*req->args) * argc);
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

        // Check if we're running in background thread
        if (RunInThread()) {
          // Background thread: schedule cleanup on main thread
          ScheduleContextCleanup(thctx, sctx);
        } else {
          // Main thread: safe to free directly
          SearchCtx_Free(sctx);
          if (thctx) {
            RedisModule_FreeThreadSafeContext(thctx);
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
    if (hreq->tailPipelineError.code != QUERY_OK) {
        QueryError_CloneFrom(&hreq->tailPipelineError, status);
        return REDISMODULE_ERR;
    }

    // Priority 2: Individual AREQ errors (sub-query failures)
    for (size_t i = 0; i < hreq->nrequests; i++) {
        if (hreq->errors[i].code != QUERY_OK) {
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
 * Create a detached thread-safe search context.
 */
static RedisSearchCtx* createDetachedSearchContext(RedisModuleCtx *ctx, const char *indexname) {
  RedisModuleCtx *detachedCtx = RedisModule_GetDetachedThreadSafeContext(ctx);
  RedisModule_SelectDb(detachedCtx, RedisModule_GetSelectedDb(ctx));
  return NewSearchCtxC(detachedCtx, indexname, true);
}

AREQ **MakeDefaultHybridUpstreams(RedisSearchCtx *sctx) {
  AREQ *search = AREQ_New();
  AREQ *vector = AREQ_New();
  initializeAREQ(search);
  initializeAREQ(vector);
  const char *indexName = HiddenString_GetUnsafe(sctx->spec->specName, NULL);
  search->sctx = createDetachedSearchContext(sctx->redisCtx, indexName);
  vector->sctx = createDetachedSearchContext(sctx->redisCtx, indexName);
  arrayof(AREQ*) requests = array_new(AREQ*, HYBRID_REQUEST_NUM_SUBQUERIES);
  requests = array_ensure_append_1(requests, search);
  requests = array_ensure_append_1(requests, vector);
  return requests;
}

HybridRequest *MakeDefaultHybridRequest(RedisSearchCtx *sctx) {
  AREQ *search = AREQ_New();
  AREQ *vector = AREQ_New();
  const char *indexName = HiddenString_GetUnsafe(sctx->spec->specName, NULL);
  search->sctx = createDetachedSearchContext(sctx->redisCtx, indexName);
  vector->sctx = createDetachedSearchContext(sctx->redisCtx, indexName);
  arrayof(AREQ*) requests = array_new(AREQ*, HYBRID_REQUEST_NUM_SUBQUERIES);
  requests = array_ensure_append_1(requests, search);
  requests = array_ensure_append_1(requests, vector);
  return HybridRequest_New(sctx, requests, array_len(requests));
}

void AddValidationErrorContext(AREQ *req, QueryError *status) {
  if (QueryError_GetCode(status) == QUERY_OK) {
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
