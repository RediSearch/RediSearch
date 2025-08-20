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

// Field names for implicit LOAD step
#define SEARCH_INDEX 0

/**
 * Create implicit LOAD step for document key when no explicit LOAD is specified.
 * Returns a PLN_LoadStep that loads only the HYBRID_IMPLICIT_KEY_FIELD.
 */
static PLN_LoadStep *createImplicitLoadStep(void) {
    // Use a static array for the field name - no memory management needed
    static const char *implicitArgv[] = {HYBRID_IMPLICIT_KEY_FIELD, UNDERSCORE_SCORE};

    PLN_LoadStep *implicitLoadStep = rm_calloc(1, sizeof(PLN_LoadStep));

    // Set up base step properties - use standard loadDtor
    implicitLoadStep->base.type = PLN_T_LOAD;
    implicitLoadStep->base.alias = NULL;
    implicitLoadStep->base.flags = 0;
    implicitLoadStep->base.dtor = loadDtor; // Use standard destructor

    // Create ArgsCursor with static array - no memory management needed
    ArgsCursor_InitCString(&implicitLoadStep->args, implicitArgv, 2);

    // Pre-allocate keys array for the number of fields to load
    implicitLoadStep->nkeys = 0;
    implicitLoadStep->keys = rm_calloc(implicitLoadStep->args.argc, sizeof(RLookupKey*));

    return implicitLoadStep;
}

/**
 * Build the complete hybrid search pipeline for processing multiple search requests.
 * This function constructs a sophisticated pipeline that:
 * 1. Builds individual pipelines for each AREQ (search request)
 * 2. Creates depleter processors to extract results from each pipeline concurrently
 * 3. Sets up a hybrid merger to combine and score results from all requests
 * 4. Applies aggregation processing (sorting, filtering, field loading) to merged results
 *
 * The pipeline architecture:
 * AREQ1 -> [Individual Pipeline] -> Depleter1 \
 * AREQ2 -> [Individual Pipeline] -> Depleter2  -> HybridMerger -> Aggregation -> Output
 * AREQ3 -> [Individual Pipeline] -> Depleter3 /
 *
 * @param req The HybridRequest containing multiple AREQ search requests
 * @param params Pipeline parameters including aggregation settings and scoring context
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on failure
 */
int HybridRequest_BuildPipeline(HybridRequest *req, const HybridPipelineParams *params) {
    // Find any LOAD step in the tail pipeline that specifies which fields to load
    // This step will be temporarily removed and re-added after merger setup
    PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(&req->tailPipeline->ap, NULL, NULL, PLN_T_LOAD);

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
            return rc;
        }

        // Set resultLimit for individual AREQ pipelines using the same logic as sendChunk()
        QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
        size_t limit = UINT64_MAX;  // Default for most cases
        if (IsHybridVectorSubquery(areq)) {
            // This is an aggregate request - use maxAggregateResults
            limit = areq->maxAggregateResults;
        }
        qctx->resultLimit = limit;
        RLookup *lookup = AGPLN_GetLookup(&areq->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST);
        PLN_LoadStep *subqueryLoadStep = NULL;

        // Determine which load step to use (explicit or implicit)
        subqueryLoadStep = loadStep ? PLNLoadStep_Clone(loadStep) : createImplicitLoadStep();

        // Add the load step to the aggplan for proper cleanup
        AGPLN_AddStep(&areq->pipeline.ap, &subqueryLoadStep->base);

        // Process the LOAD step (explicit or implicit) using the unified function
        ResultProcessor *loader = processLoadStep(subqueryLoadStep, lookup, AREQ_SearchCtx(areq), AREQ_RequestFlags(areq),
                                                 RLOOKUP_F_NOFLAGS, false, &areq->stateflags, &req->errors[i]);
        if (req->errors[i].code != QUERY_OK) {
            StrongRef_Release(sync_ref);
            array_free(depleters);
            // Note: HybridRequest_Free is called by the caller on failure
            return REDISMODULE_ERR;
        }
        if (loader) {
          QITR_PushRP(qctx, loader);
        }

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

    // Assumes all upstream lookups are synced (required keys exist in all of them and reference the same row indices),
    // and contain only keys from the loading step
    // Init lookup since we dont call buildQueryPart
    RLookup *lookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
    RLookup_Init(lookup, IndexSpec_GetSpecCache(params->aggregationParams.common.sctx->spec));
    RLookup *searchLookup = AGPLN_GetLookup(&req->requests[SEARCH_INDEX]->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST);
    RLookup_CloneInto(lookup, searchLookup);
    // TODO: sync SEARCH and VSIM subqueries' lookups after YIELD_DISTANCE_AS is enabled


    // scoreKey is not NULL if the score is loaded as a field (explicitly or implicitly)
    const RLookupKey *scoreKey = RLookup_GetKey_Read(lookup, UNDERSCORE_SCORE, RLOOKUP_F_NOFLAGS);

    ResultProcessor *merger = RPHybridMerger_New(params->scoringCtx, depleters, req->nrequests, scoreKey);
    QITR_PushRP(&req->tailPipeline->qctx, merger);

    // Add implicit sorting by score
    const PLN_BaseStep *arrangeStep = AGPLN_FindStep(&req->tailPipeline->ap, NULL, NULL, PLN_T_ARRANGE);
    if (!arrangeStep) {
        AGPLN_GetOrCreateArrangeStep(&req->tailPipeline->ap);
    }

    // Temporarily remove the LOAD step from the tail pipeline to avoid conflicts
    // during aggregation pipeline building, then restore it afterwards
    if (loadStep) {
        AGPLN_PopStep(&loadStep->base);
    }

    // Build the aggregation part of the tail pipeline for final result processing
    // This handles sorting, filtering, field loading, and output formatting of merged results
    uint32_t stateFlags = 0;
    int rc = Pipeline_BuildAggregationPart(req->tailPipeline, &params->aggregationParams, &stateFlags);
    if (rc != REDISMODULE_OK) {
        return rc;
    };

    // Restore the LOAD step to the tail pipeline for proper cleanup
    if (loadStep) {
        AGPLN_AddStep(&req->tailPipeline->ap, &loadStep->base);
    }
    return REDISMODULE_OK;
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
HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests) {
    HybridRequest *hybridReq = rm_calloc(1, sizeof(*hybridReq));
    hybridReq->requests = requests;
    hybridReq->nrequests = nrequests;

    // Initialize error tracking for each individual request
    hybridReq->errors = array_new(QueryError, nrequests);

    // Initialize the tail pipeline that will merge results from all requests
    hybridReq->tailPipeline = rm_calloc(1, sizeof(Pipeline));
    AGPLN_Init(&hybridReq->tailPipeline->ap);
    QueryError_Init(&hybridReq->tailPipelineError);
    Pipeline_Initialize(hybridReq->tailPipeline, requests[0]->pipeline.qctx.timeoutPolicy, &hybridReq->tailPipelineError);

    // Initialize pipelines for each individual request
    for (size_t i = 0; i < nrequests; i++) {
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
      if (req->requests[i]->sctx && req->requests[i]->sctx->redisCtx) {
        RedisModuleCtx *thctx = req->requests[i]->sctx->redisCtx;
        RedisSearchCtx *sctx = req->requests[i]->sctx;

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

        req->requests[i]->sctx = NULL;
      }

      AREQ_Free(req->requests[i]);
    }
    array_free(req->requests);

    array_free(req->errors);

    // Free the hybrid parameters
    if (req->hybridParams) {
      // Free the aggregationParams search context
      if(req->hybridParams->aggregationParams.common.sctx) {
        SearchCtx_Free(req->hybridParams->aggregationParams.common.sctx);
        req->hybridParams->aggregationParams.common.sctx = NULL;
      }

      // Free the hybrid parameters
      rm_free(req->hybridParams);
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
int HREQ_GetError(HybridRequest *hreq, QueryError *status) {
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

#ifdef __cplusplus
}
#endif
