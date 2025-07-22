#include "hybrid/hybrid_request.h"
#include "pipeline/pipeline.h"
#include "hybrid/hybrid_scoring.h"

#ifdef __cplusplus
extern "C" {
#endif

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

        // Build the complete pipeline for this individual search request
        // This includes indexing (search/scoring) and any request-specific aggregation
        // Worth noting that in the current syntax we expect the AGGPlan to be empty
        int rc = AREQ_BuildPipeline(areq, &req->errors[i]);
        if (rc != REDISMODULE_OK) {
            StrongRef_Release(sync_ref);
            array_free(depleters);
            return rc;
        }

        QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
        RLookup *lastLookup = AGPLN_GetLookup(&areq->pipeline->ap, NULL, AGPLN_GETLOOKUP_LAST);
        if (loadStep) {
            // Add a loader to load the fields specified in the LOAD step
            ResultProcessor *loader = RPLoader_New(AREQ_SearchCtx(areq), AREQ_RequestFlags(areq), lastLookup, loadStep->keys, loadStep->nkeys, false, &areq->stateflags);
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

    // Create the hybrid merger that combines results from all depleter processors
    // This is where the magic happens - results from different search modalities are merged
    ResultProcessor *merger = RPHybridMerger_New(params->scoringCtx, depleters, req->nrequests);
    QITR_PushRP(&req->tailPipeline->qctx, merger);

    // Temporarily remove the LOAD step from the tail pipeline to avoid conflicts
    // during aggregation pipeline building, then restore it afterwards
    if (loadStep) {
        AGPLN_PopStep(&loadStep->base);
    }

    // Build the aggregation part of the tail pipeline for final result processing
    // This handles sorting, filtering, field loading, and output formatting of merged results
    uint32_t stateFlags = 0;
    Pipeline_BuildAggregationPart(req->tailPipeline, &params->aggregationParams, &stateFlags);

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
    Pipeline_Initialize(hybridReq->tailPipeline, requests[0]->pipeline->qctx.timeoutPolicy, &hybridReq->tailPipelineError);

    // Initialize pipelines for each individual request
    for (size_t i = 0; i < nrequests; i++) {
        QueryError_Init(&hybridReq->errors[i]);
        Pipeline_Initialize(requests[i]->pipeline, requests[i]->reqConfig.timeoutPolicy, &hybridReq->errors[i]);
    }
    return hybridReq;
}

/**
 * Free a HybridRequest and all its associated resources.
 * This function properly cleans up all individual AREQ requests, the tail pipeline,
 * error arrays, and the HybridRequest structure itself.
 *
 * @param hybridReq The HybridRequest to free
 */
void HybridRequest_Free(HybridRequest *hybridReq) {
    if (!hybridReq) return;

    // Free the tail pipeline
    if (hybridReq->tailPipeline) {
        Pipeline_Clean(hybridReq->tailPipeline);
        rm_free(hybridReq->tailPipeline);
        hybridReq->tailPipeline = NULL;
    }

    // Free all individual AREQ requests
    for (size_t i = 0; i < hybridReq->nrequests; i++) {
        // Check if we need to manually free the thread-safe context
        if (hybridReq->requests[i]->sctx && hybridReq->requests[i]->sctx->redisCtx) {

            // Free the search context
            RedisModuleCtx *thctx = hybridReq->requests[i]->sctx->redisCtx;
            RedisSearchCtx *sctx = hybridReq->requests[i]->sctx;
            SearchCtx_Free(sctx);
            // Free the thread-safe context
            if (thctx) {
                RedisModule_FreeThreadSafeContext(thctx);
            }
            hybridReq->requests[i]->sctx = NULL;
        }

        AREQ_Free(hybridReq->requests[i]);
    }

    // Free the scoring context resources
    if (hybridReq->hybridParams) {
        HybridScoringContext_Free(hybridReq->hybridParams->scoringCtx);

        // Free the aggregationParams search context
        if(hybridReq->hybridParams->aggregationParams.common.sctx) {
            SearchCtx_Free(hybridReq->hybridParams->aggregationParams.common.sctx);
        }
        // Free the hybrid parameters
        rm_free(hybridReq->hybridParams);
    }



    // Free the arrays and tail pipeline
    array_free(hybridReq->requests);
    array_free(hybridReq->errors);

    rm_free(hybridReq);
}

#ifdef __cplusplus
}
#endif
