#include "hybrid/hybrid_request.h"
#include "pipeline/pipeline.h"
#include "hybrid/hybrid_scoring.h"
#include "document.h"
#include "aggregate/aggregate_plan.h"
#include "aggregate/aggregate.h"
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

        // Build the complete pipeline for this individual search request
        // This includes indexing (search/scoring) and any request-specific aggregation
        int rc = AREQ_BuildPipeline(areq, &req->errors[i]);
        if (rc != REDISMODULE_OK) {
            StrongRef_Release(sync_ref);
            array_free(depleters);
            return rc;
        }

        QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
        RLookup *lookup = AGPLN_GetLookup(&areq->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST);
        const RLookupKey **keys;
        size_t nkeys;
        RLookupKey *implicitLoadKeys[1];
        PLN_LoadStep *loadStepClone = NULL;
        //TODO: take this logic out of here and into the pipeline_construction.c
        if (loadStep) {
          // Create a clone of the loadStep for this AREQ to avoid consuming shared args
          loadStepClone = PLNLoadStep_Clone(loadStep);

          // Process the loadStep args to populate keys array (similar to pipeline_construction.c)
          // The loadStep may not have been processed yet, so we need to parse the args
          if (loadStepClone->nkeys == 0 && loadStepClone->args.argc > 0) {
            // Process args to populate keys array
            ArgsCursor tempAC = loadStepClone->args;
            while (!AC_IsAtEnd(&tempAC)) {
              size_t name_len;
              const char *name, *path = AC_GetStringNC(&tempAC, &name_len);
              if (*path == '@') {
                path++;
                name_len--;
              }
              if (AC_AdvanceIfMatch(&tempAC, SPEC_AS_STR)) {
                int rv = AC_GetString(&tempAC, &name, &name_len, 0);
                if (rv != AC_OK) {
                  // Error handling - continue with next arg or fail gracefully
                  continue;
                }
                if (!strcasecmp(name, SPEC_AS_STR)) {
                  // Alias for LOAD cannot be `AS` - skip this invalid entry
                  continue;
                }
              } else {
                // Set the name to the path. name_len is already the length of the path.
                name = path;
              }

              RLookupKey *kk = RLookup_GetKey_LoadEx(lookup, name, name_len, path, RLOOKUP_F_NOFLAGS);
              // We only get a NULL return if the key already exists, which means
              // that we don't need to retrieve it again.
              if (kk && loadStepClone->nkeys < loadStepClone->args.argc) {
                loadStepClone->keys[loadStepClone->nkeys++] = kk;
              }
            }
          }
          keys = loadStepClone->keys;
          nkeys = loadStepClone->nkeys;
        } else {
          // If load was not specified, implicitly load doc key
          RLookupKey *docIdKey = RLookup_GetKey_Load(lookup, HYBRID_IMPLICIT_KEY_FIELD, UNDERSCORE_KEY, RLOOKUP_F_NOFLAGS);
          implicitLoadKeys[0] = docIdKey;
          keys = implicitLoadKeys;
          nkeys = 1;
        }

        ResultProcessor *loader = RPLoader_New(AREQ_SearchCtx(areq), AREQ_RequestFlags(areq), lookup, keys, nkeys, false, &areq->stateflags);
        QITR_PushRP(qctx, loader);

        // Clean up the cloned loadStep after using its keys
        if (loadStepClone) {
          // Use the proper destructor, but need to handle alias separately since loadDtor doesn't free it
          if (loadStepClone->base.alias) {
            rm_free(loadStepClone->base.alias);
          }
          // Use the standard destructor for the rest
          loadDtor(&loadStepClone->base);
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
    //Init lookup since we dont call buildQueryPart
    RLookup *lookup = AGPLN_GetLookup(&req->tailPipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);
    RLookup_Init(lookup, NULL);
    RLookup_CloneInto(lookup, AGPLN_GetLookup(&req->requests[0]->pipeline.ap, NULL, AGPLN_GETLOOKUP_FIRST));



    const RLookupKey *scoreKey = NULL;
    if (!loadStep) {
        // implicit load score as well as key
        scoreKey = RLookup_GetKey_Write(lookup, UNDERSCORE_SCORE, RLOOKUP_F_NOFLAGS);
    }
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
