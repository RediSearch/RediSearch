#include "hybrid/hybrid_request.h"
#include "pipeline/pipeline.h"
#include "hybrid/hybrid_scoring.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Clone RLookupKeys from a LOAD step into a different RLookup context.
 * This is necessary because each AREQ has its own RLookup context, but we need
 * to ensure that field loading keys are properly mapped across different lookup
 * contexts when building the hybrid pipeline. The lookup doesn't automatically
 * know about the load step, so we must explicitly clone the keys.
 *
 * @param loadStep The LOAD step containing the original RLookupKeys
 * @param lookup The target RLookup context where keys should be cloned
 * @return Array of cloned RLookupKeys that can be used in the target lookup context
 */
const RLookupKey **CloneKeysInDifferentRLookup(PLN_LoadStep *loadStep, RLookup *lookup) {
    const RLookupKey **clonedKeys = array_new(const RLookupKey *, loadStep->nkeys);
    for (size_t index = 0; index < loadStep->nkeys; index++) {
        const RLookupKey *key = loadStep->keys[index];
        clonedKeys[index] = RLookup_CloneKey(lookup, key);
    }
    return clonedKeys;
}

/**
 * Build the complete hybrid search pipeline for processing multiple search requests.
 * This function constructs a sophisticated pipeline that:
 * 1. Builds individual pipelines for each AREQ (search request)
 * 2. Creates depleter processors to extract results from each pipeline
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
    // Create synchronization context for coordinating depleter processors
    // This ensures thread-safe access when multiple depleters read from their pipelines
    StrongRef sync_ref = DepleterSync_New(req->nrequests, params->synchronize_read_locks);

    // Find any LOAD step in the tail pipeline that specifies which fields to load
    // This step will be temporarily removed and re-added after merger setup
    PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(&req->tail.ap, NULL, NULL, PLN_T_LOAD);

    // Array to collect depleter processors from each individual request pipeline
    arrayof(ResultProcessor*) depleters = array_new(ResultProcessor *, req->nrequests);
    // Build individual pipelines for each search request
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ *areq = req->requests[i];

        // Build the complete pipeline for this individual search request
        // This includes indexing (search/scoring) and any request-specific aggregation
        int rc = AREQ_BuildPipeline(areq, &req->errors[i]);
        if (rc != REDISMODULE_OK) {
            StrongRef_Release(sync_ref);
            array_free(depleters);
            return rc;
        }

        QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
        RLookup *lastLookup = AGPLN_GetLookup(&areq->pipeline.ap, NULL, AGPLN_GETLOOKUP_LAST);

        // If there's a LOAD step, add a loader to this individual pipeline
        // We use the last lookup since we push the loader at the end of the pipeline
        if (loadStep && lastLookup) {
            const RLookupKey **clonedKeys = CloneKeysInDifferentRLookup(loadStep, lastLookup);
            ResultProcessor *loader = RPLoader_New(AREQ_SearchCtx(areq), areq->reqflags, lastLookup, clonedKeys, loadStep->nkeys, false, &areq->stateflags);
            QITR_PushRP(qctx, loader);
        }

        // Create a depleter processor to extract results from this pipeline
        // The depleter will feed results to the hybrid merger
        ResultProcessor *depleter = RPDepleter_New(StrongRef_Clone(sync_ref), AREQ_SearchCtx(areq));
        array_ensure_append_1(depleters, depleter);
        QITR_PushRP(qctx, depleter);
    }

    // Release the sync reference as depleters now hold their own references
    StrongRef_Release(sync_ref);

    // Create the hybrid merger that combines results from all depleter processors
    // This is where the magic happens - results from different search modalities are merged
    ResultProcessor *merger = RPHybridMerger_New(params->scoringCtx, depleters, req->nrequests);
    QITR_PushRP(&req->tail.qctx, merger);

    // Temporarily remove the LOAD step from the tail pipeline to avoid conflicts
    // during aggregation pipeline building, then restore it afterwards
    if (loadStep) {
        AGPLN_PopStep(&loadStep->base);
    }

    // Build the aggregation part of the tail pipeline for final result processing
    // This handles sorting, filtering, field loading, and output formatting of merged results
    uint32_t stateFlags = 0;
    QueryPipeline_BuildAggregationPart(&req->tail, &params->aggregation, &stateFlags);

    // Restore the LOAD step to the tail pipeline for proper cleanup
    if (loadStep) {
        AGPLN_AddStep(&req->tail.ap, &loadStep->base);
    }
    return REDISMODULE_OK;
}

/**
 * Create a new HybridRequest that manages multiple search requests for hybrid search.
 * This function initializes the hybrid request structure and sets up the tail pipeline
 * that will be used to merge and process results from all individual search requests.
 *
 * @param requests Array of AREQ pointers representing individual search requests
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
    AGPLN_Init(&req->tail.ap);
    req->tail.qctx.timeoutPolicy = requests[0]->pipeline.qctx.timeoutPolicy;
    req->tail.qctx.rootProc = req->tail.qctx.endProc = NULL;
    req->tail.qctx.err = &req->tailError;
    QueryError_Init(&req->tailError);
    QueryPipeline_Initialize(&req->tail, requests[0]->pipeline.qctx.timeoutPolicy, &req->tailError);

    // Initialize pipelines for each individual request
    for (size_t i = 0; i < nrequests; i++) {
        QueryError_Init(&req->errors[i]);
        QueryPipeline_Initialize(&requests[i]->pipeline, requests[i]->reqConfig.timeoutPolicy, &req->errors[i]);
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
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ_Free(req->requests[i]);
    }

    // Free the arrays and tail pipeline
    array_free(req->requests);
    array_free(req->errors);
    QueryPipeline_Clean(&req->tail);  // Cleans up the merger and aggregation pipeline
    rm_free(req);
}

#ifdef __cplusplus
}
#endif
