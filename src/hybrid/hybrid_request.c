#include "hybrid/hybrid_request.h"
#include "pipeline/pipeline.h"
#include "hybrid/hybrid_scoring.h"
#include "document.h"
#include "aggregate/aggregate_plan.h"
#include "rmutil/args.h"

#ifdef __cplusplus
extern "C" {
#endif

// Field names for implicit LOAD step
#define HYBRID_IMPLICIT_KEY_FIELD "key"

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
        // Worth noting that in the current syntax we expect the AGGPlan to be empty
        int rc = AREQ_BuildPipeline(areq, &req->errors[i]);
        if (rc != REDISMODULE_OK) {
            StrongRef_Release(sync_ref);
            array_free(depleters);
            return rc;
        }

        QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
        RLookup *lookup = AGPLN_GetLookup(&areq->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST);
        RLookupKey **keys;
        size_t nkeys;
        RLookupKey *implicitLoadKeys[1];
        if (loadStep) {
          keys = loadStep->keys;
          nkeys = loadStep->nkeys;
        } else {
          // If load was not specified, implicitly load doc key
          RLookupKey *docIdKey = RLookup_GetKey_Load(lookup, HYBRID_IMPLICIT_KEY_FIELD, UNDERSCORE_KEY, RLOOKUP_F_NOFLAGS);
          implicitLoadKeys[0] = docIdKey;
          keys = implicitLoadKeys;
          nkeys = 1;
        }
        ResultProcessor *loader = RPLoader_New(AREQ_SearchCtx(areq), AREQ_RequestFlags(areq), lookup, keys, nkeys, false, &areq->stateflags);
        QITR_PushRP(qctx, loader);

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

    const RLookupKey *scoreKey = NULL;
    if (!loadStep) {
        // implicit load score as well as key
        RLookup *lookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
        scoreKey = RLookup_GetKey_Write(lookup, UNDERSCORE_SCORE, RLOOKUP_F_NOFLAGS);
    }
    ResultProcessor *merger = RPHybridMerger_New(params->scoringCtx, depleters, req->nrequests, scoreKey);
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
 * Execute the hybrid search pipeline and send results to the client.
 * This function creates a temporary AREQ wrapper around the tail pipeline
 * and uses the hybrid-specific result serialization functions.
 *
 * @param req The HybridRequest with built pipeline
 * @param ctx Redis module context for sending the reply
 */
void HybridRequest_Execute(HybridRequest *req, RedisModuleCtx *ctx) {
    // Create temporary AREQ wrapper around tail pipeline
    AREQ *tailAREQ = AREQ_New();

    // Transfer the tail pipeline to the AREQ (without copying)
    tailAREQ->pipeline = *req->tailPipeline;

    // Set up the search context from the hybrid parameters
    tailAREQ->sctx = req->hybridParams->aggregationParams.common.sctx;

    // Copy request configuration from the first individual request
    if (req->nrequests > 0) {
        tailAREQ->reqflags = req->requests[0]->reqflags;
        tailAREQ->reqConfig = req->requests[0]->reqConfig;
        tailAREQ->maxAggregateResults = req->requests[0]->maxAggregateResults;
        tailAREQ->maxSearchResults = req->requests[0]->maxSearchResults;
    }

    // Set up the reply
    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

    // Set up cached variables for result serialization
    AGGPlan *plan = AREQ_AGGPlan(tailAREQ);
    cachedVars cv = {
        .lastLk = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_LAST),
        .lastAstp = AGPLN_GetArrangeStep(plan)
    };

    // Execute the hybrid pipeline using the specialized hybrid serialization
    sendChunk_hybrid(tailAREQ, reply, UINT64_MAX, cv);

    RedisModule_EndReply(reply);

    // Clean up the temporary AREQ without freeing the pipeline
    // (the pipeline belongs to the HybridRequest)
    tailAREQ->pipeline = (Pipeline){0}; // Clear pipeline reference
    AREQ_Free(tailAREQ);
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

    // Free the scoring context resources
    if (req->hybridParams) {
      // The scoring context is freed by the hybrid merger
      // HybridScoringContext_Free(req->hybridParams->scoringCtx);

      // Free the aggregationParams search context
      if(req->hybridParams->aggregationParams.common.sctx) {
        SearchCtx_Free(req->hybridParams->aggregationParams.common.sctx);
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

#ifdef __cplusplus
}
#endif
