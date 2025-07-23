#include "hybrid/hybrid_request.h"
#include "pipeline/pipeline.h"
#include "hybrid/hybrid_scoring.h"
#include "document.h"
#include "aggregate/aggregate_plan.h"

#ifdef __cplusplus
extern "C" {
#endif

// Field names for implicit LOAD step
#define HYBRID_IMPLICIT_DOC_ID "doc_id"
#define HYBRID_IMPLICIT_COMBINED_SCORE "combined_score"

/**
 * Create an implicit LOAD step for hybrid search when no explicit LOAD is provided.
 * The implicit LOAD includes doc_id (document ID) and combined_score (combined score).
 *
 * @param plan The aggregation plan to get RLookup from
 * @return Newly allocated PLN_LoadStep with implicit fields
 */
static PLN_LoadStep* CreateImplicitLoadStep(AGGPlan *plan) {
    PLN_LoadStep *lstp = rm_calloc(1, sizeof(*lstp));
    lstp->base.type = PLN_T_LOAD;
    lstp->base.dtor = loadDtor;
    lstp->nkeys = 2;
    lstp->keys = rm_calloc(2, sizeof(*lstp->keys));

    // Get the RLookup from the plan to create proper RLookupKeys
    RLookup *lookup = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_LAST);
    if (!lookup) {
        lookup = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_FIRST);
    }

    // doc_id maps to the document key (__key internally)
    lstp->keys[0] = RLookup_GetKey_Load(lookup, HYBRID_IMPLICIT_DOC_ID, UNDERSCORE_KEY, RLOOKUP_F_NOFLAGS);
    // combined_score maps to the hybrid score (__score internally)
    lstp->keys[1] = RLookup_GetKey_Load(lookup, HYBRID_IMPLICIT_COMBINED_SCORE, UNDERSCORE_SCORE, RLOOKUP_F_NOFLAGS);

    lstp->args = (ArgsCursor){0};  // Zero-initialize for empty cursor

    return lstp;
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
    PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(&req->pipeline.ap, NULL, NULL, PLN_T_LOAD);

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
        RLookup *lastLookup = AGPLN_GetLookup(&areq->pipeline.ap, NULL, AGPLN_GETLOOKUP_LAST);
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
    QITR_PushRP(&req->pipeline.qctx, merger);

    // Create implicit LOAD step if none exists (MOD-10249: automatic @__key and @__score return)
    // This should happen after merge operation, for the final tail pipeline
    if (!loadStep) {
        loadStep = CreateImplicitLoadStep(&req->pipeline.ap);
        if (!loadStep) {
            return REDISMODULE_ERR;  // Memory allocation failed
        }
        AGPLN_AddStep(&req->pipeline.ap, &loadStep->base);
    }

    // Temporarily remove the LOAD step from the tail pipeline to avoid conflicts
    // during aggregation pipeline building, then restore it afterwards
    if (loadStep) {
        AGPLN_PopStep(&loadStep->base);
    }

    // Build the aggregation part of the tail pipeline for final result processing
    // This handles sorting, filtering, field loading, and output formatting of merged results
    uint32_t stateFlags = 0;
    Pipeline_BuildAggregationPart(&req->pipeline, &params->aggregationParams, &stateFlags);

    // Restore the LOAD step to the tail pipeline for proper cleanup
    if (loadStep) {
        AGPLN_AddStep(&req->pipeline.ap, &loadStep->base);
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
    HybridRequest *req = rm_calloc(1, sizeof(*req));
    req->requests = requests;
    req->nrequests = nrequests;

    // Initialize error tracking for each individual request plus the tail pipeline
    req->errors = array_new(QueryError, nrequests);

    // Initialize the tail pipeline that will merge results from all requests
    AGPLN_Init(&req->pipeline.ap);
    QueryError_Init(&req->pipelineError);
    Pipeline_Initialize(&req->pipeline, requests[0]->pipeline.qctx.timeoutPolicy, &req->pipelineError);

    // Initialize pipelines for each individual request
    for (size_t i = 0; i < nrequests; i++) {
        QueryError_Init(&req->errors[i]);
        Pipeline_Initialize(&requests[i]->pipeline, requests[i]->reqConfig.timeoutPolicy, &req->errors[i]);
    }
    return req;
}

/**
 * Free a HybridRequest and all its associated resources.
 * This function properly cleans up all individual AREQ requests, the tail pipeline,
 * error arrays, and the HybridRequest structure itself.
 *
 * @param req The HybridRequest to free
 */
void HybridRequest_Free(HybridRequest *req) {
    // Free all individual AREQ requests and their pipelines
    array_free_ex(req->requests, AREQ_Free(*(AREQ **)ptr));
    array_free(req->errors);
    Pipeline_Clean(&req->pipeline);  // Cleans up the merger and aggregation pipeline
    rm_free(req);
}

#ifdef __cplusplus
}
#endif
