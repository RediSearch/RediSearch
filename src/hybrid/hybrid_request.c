#include "hybrid/hybrid_request.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_construction.h"
#include "hybrid/hybrid_scoring.h"
#include "document.h"
#include "aggregate/aggregate_plan.h"
#include "aggregate/aggregate.h"
#include "rmutil/args.h"
#include "util/workers.h"
#include "cursor.h"
#include "info/info_redis/block_client.h"
#include "query_error.h"
#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SEARCH_INDEX 0

arrayof(ResultProcessor*) HybridRequest_BuildDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params) {
    // Create synchronization context for coordinating depleter processors
    // This ensures thread-safe access when multiple depleters read from their pipelines
    StrongRef sync_ref = DepleterSync_New(req->nrequests, params->synchronize_read_locks);

    // Array to collect depleter processors from each individual request pipeline
    arrayof(ResultProcessor*) depleters = array_new(ResultProcessor *, req->nrequests);

    // Build individual pipelines for each search request
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ *areq = req->requests[i];

        areq->rootiter = QAST_Iterate(&areq->ast, &areq->searchopts, AREQ_SearchCtx(areq), &areq->conc, areq->reqflags, &req->errors[i]);

        // Build the complete pipeline for this individual search request
        // This includes indexing (search/scoring) and any request-specific aggregation
        int rc = AREQ_BuildPipeline(areq, &req->errors[i]);
        if (rc != REDISMODULE_OK) {
            StrongRef_Release(sync_ref);
            array_free(depleters);
            return NULL;
        }

        // Obtain the query processing context for the current AREQ
        QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
        // Create a depleter processor to extract results from this pipeline
        // The depleter will feed results to the hybrid merger
        RedisSearchCtx *nextThread = params->aggregationParams.common.sctx; // We will use the context provided in the params
        RedisSearchCtx *depletingThread = AREQ_SearchCtx(areq); // when constructing the AREQ a new context should have been created
        ResultProcessor *depleter = RPDepleter_New(StrongRef_Clone(sync_ref), depletingThread, nextThread);
        array_ensure_append_1(depleters, depleter);
        QITR_PushRP(qctx, depleter);
    }

    // Release the sync reference as depleters now hold their own references
    StrongRef_Release(sync_ref);
    return depleters;
}

int HybridRequest_BuildMergePipeline(HybridRequest *req, HybridPipelineParams *params, arrayof(ResultProcessor*) depleters) {
    // Assumes all upstream lookups are synced (required keys exist in all of them and reference the same row indices),
    // and contain only keys from the loading step
    // Init lookup since we dont call buildQueryPart
    RLookup *lookup = AGPLN_GetLookup(req->ap, NULL, AGPLN_GETLOOKUP_FIRST);
    RLookup_Init(lookup, IndexSpec_GetSpecCache(params->aggregationParams.common.sctx->spec));
    RLookup_CloneInto(lookup, AGPLN_GetLookup(AREQ_AGGPlan(req->requests[SEARCH_INDEX]), NULL, AGPLN_GETLOOKUP_FIRST));

    // Itzik said he will fix score key
    const RLookupKey *scoreKey = NULL;
    ResultProcessor *merger = RPHybridMerger_New(params->scoringCtx, depleters, req->nrequests, scoreKey);
    params->scoringCtx = NULL; // ownership transferred to merger
    QITR_PushRP(&req->tailPipeline->qctx, merger);

    // Build the aggregation part of the tail pipeline for final result processing
    // This handles sorting, filtering, field loading, and output formatting of merged results
    uint32_t stateFlags = 0;
    Pipeline_BuildAggregationPart(req->tailPipeline, &params->aggregationParams, &stateFlags);
    return REDISMODULE_OK;
}

int HybridRequest_BuildPipeline(HybridRequest *req, HybridPipelineParams *params) {
      // move the context to the hybrid request
    req->sctx = params->aggregationParams.common.sctx;
    params->aggregationParams.common.sctx = NULL;
    // Build the depletion pipeline for extracting results from individual search requests
    arrayof(ResultProcessor*) depleters = HybridRequest_BuildDepletionPipeline(req, params);
    if (!depleters) {
      return REDISMODULE_ERR;
    }
    // Build the merge pipeline for combining and processing results from the depletion pipeline
    return HybridRequest_BuildMergePipeline(req, params, depleters);
}

static void FreeHybridRequest(void *ptr) {
  HybridRequest_Free((HybridRequest *)ptr);
}

arrayof(Cursor*) HybridRequest_StartCursor(HybridRequest *req, arrayof(ResultProcessor*) depleters, bool coord) {
    if (req->nrequests == 0 || req->nrequests != array_len(depleters)) {
      return NULL;
    }
    StrongRef hybrid_ref = StrongRef_New(req, &FreeHybridRequest);
    if (!depleters) {
      arrayof(Cursor*) cursors = array_new(Cursor*, 1);
      // We don't have depleters, we will create a single cursor just for the hybrid request
      // This is needed for client facing API, client expects a single cursor id to receive the merged result set
      AREQ *first = req->requests[0];
      Cursor *cursor = Cursors_Reserve(getCursorList(coord), first->sctx->spec->own_ref, first->cursorConfig.maxIdle, &req->tailPipelineError);
      if (!cursor) {
        return NULL;
      }
      cursor->hybrid_ref = hybrid_ref;
      array_ensure_append_1(cursors, cursor);
      return cursors;
    }

    arrayof(Cursor*) cursors = array_new(Cursor*, req->nrequests);
    for (size_t i = 0; i < req->nrequests; i++) {
      AREQ *areq = req->requests[i];
      if (areq->pipeline.qctx.endProc->type != RP_DEPLETER) {
         break;
      }
      Cursor *cursor = Cursors_Reserve(getCursorList(coord), areq->sctx->spec->own_ref, areq->cursorConfig.maxIdle, &req->tailPipelineError);
      if (!cursor) {
        break;
      }
      // transfer ownership of the request to the cursor
      cursor->execState = areq;
      cursor->hybrid_ref = StrongRef_Clone(hybrid_ref);
      areq->cursor_id = cursor->id;
      array_ensure_append_1(cursors, cursor);
    }
    // Release the hybrid reference as cursors now hold their own references
    StrongRef_Release(hybrid_ref);

    if (array_len(cursors) != req->nrequests) {
      for (size_t i = 0; i < array_len(cursors); i++) {
        Cursor_Free(cursors[i]);
      }
      array_free(cursors);
      QueryError_SetError(&req->tailPipelineError, QUERY_ELIMIT, "Failed to allocate enough cursors");
      return NULL;
    }

    int rc = RPDepleter_DepleteAll(depleters, req->nrequests);
    array_free(depleters);
    if (rc != RS_RESULT_OK) {
      for (size_t i = 0; i < array_len(cursors); i++) {
        Cursor_Free(cursors[i]);
      }
      array_free(cursors);
      if (rc == RS_RESULT_TIMEDOUT) {
        QueryError_SetWithoutUserDataFmt(&req->tailPipelineError, QUERY_ETIMEDOUT, "Depleting timed out");
      } else {
        QueryError_SetWithoutUserDataFmt(&req->tailPipelineError, QUERY_EGENERIC, "Failed to deplete set of results, rc=%d", rc);
      }
      return NULL;
    }
    return cursors;
}

/**
 * Create a new HybridRequest that manages multiple search requests for hybrid search.
 * This function initializes the hybrid request structure and sets up the tail pipeline
 * that will be used to merge and process results from all individual search requests.
 *
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

    array_free(req->errors);

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
        QueryError_SetError(status, hreq->tailPipelineError.code,
                           hreq->tailPipelineError.detail);
        return REDISMODULE_ERR;
    }

    // Priority 2: Individual AREQ errors (sub-query failures)
    for (size_t i = 0; i < hreq->nrequests; i++) {
        if (hreq->errors[i].code != QUERY_OK) {
            QueryError_SetError(status, hreq->errors[i].code,
                               hreq->errors[i].detail);
            return REDISMODULE_ERR;
        }
    }

    // No errors found
    return REDISMODULE_OK;
}

HybridRequest *MakeDefaultHybridRequest() {
  AREQ *search = AREQ_New();
  AREQ *vector = AREQ_New();
  arrayof(AREQ*) requests = array_new(AREQ*, HYBRID_REQUEST_NUM_SUBQUERIES);
  requests = array_ensure_append_1(requests, search);
  requests = array_ensure_append_1(requests, vector);
  return HybridRequest_New(requests, array_len(requests));
}

#ifdef __cplusplus
}
#endif
