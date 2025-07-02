#include "hybrid/hybrid_request.h"
#include "aggregate/aggregate_pipeline.h"
#include "hybrid/hybrid_scoring.h"

#ifdef __cplusplus
extern "C" {
#endif

// Important to remember that the lookup doesn't know about the load step
const RLookupKey **CloneKeysInDifferentRLookup(PLN_LoadStep *loadStep, RLookup *lookup) {
    const RLookupKey **clonedKeys = array_new(const RLookupKey *, loadStep->nkeys);
    for (size_t index = 0; index < loadStep->nkeys; index++) {
        const RLookupKey *key = loadStep->keys[index];
        clonedKeys[index] = RLookup_CloneKey(lookup, key);
    }
    return clonedKeys;
}

int HybridRequest_BuildPipeline(HybridRequest *req, QueryError *status, RSSearchOptions *searchOpts) {
    StrongRef sync_ref = DepleterSync_New();
    PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(&req->merge.ap, NULL, NULL, PLN_T_LOAD);
    arrayof(ResultProcessor*) depleters = array_new(ResultProcessor *, req->nrequests);
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ *areq = req->requests[i];
        // Build the pipeline based on the areq AGGPlan
        int rc = AREQ_BuildPipeline(areq, status);
        if (rc != REDISMODULE_OK) {
            StrongRef_Release(sync_ref);
            array_free(depleters);
            return rc;
        }
        QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
        // it makes more sense to take the last lookup since we push the loader in the end.
        if (loadStep) {
            RLookup *lastLookup = AGPLN_GetLookup(&areq->pipeline.ap, NULL, AGPLN_GETLOOKUP_LAST);
            const RLookupKey **clonedKeys = CloneKeysInDifferentRLookup(loadStep, lastLookup);
            ResultProcessor *loader = RPLoader_New(&areq->pipeline, lastLookup, clonedKeys, loadStep->nkeys, false);
            QITR_PushRP(qctx, loader);
        }
        ResultProcessor *depleter = RPDepleter_New(StrongRef_Clone(sync_ref), AREQ_SearchCtx(areq));
        array_ensure_append_1(depleters, depleter);
        QITR_PushRP(qctx, depleter);
    }
    
    StrongRef_Release(sync_ref);
    ResultProcessor *merger = RPHybridMerger_New(&req->scoringCtx, depleters, req->nrequests);
    QITR_PushRP(&req->merge.qctx, merger);

    if (loadStep) {
        AGPLN_PopStep(&loadStep->base);
    }
    BuildPipeline(&req->merge, NULL, searchOpts, status, req->merge.qctx.timeoutPolicy);
    if (loadStep) {
        AGPLN_AddStep(&req->merge.ap, &loadStep->base);
    }
    return REDISMODULE_OK;
}

HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests, AGGPlan *plan) {
    HybridRequest *req = rm_calloc(1, sizeof(*req));
    req->requests = requests;
    req->nrequests = nrequests;
    req->merge.ap = *plan;
    req->merge.qctx.timeoutPolicy = requests[0]->pipeline.qctx.timeoutPolicy;
    req->merge.qctx.rootProc = req->merge.qctx.endProc = NULL;
    return req;
}

void HybridRequest_Free(HybridRequest *req) {
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ_Free(req->requests[i]);
    }
    array_free(req->requests);
    QueryPipeline_Clean(&req->merge);
    rm_free(req);
}

#ifdef __cplusplus
}
#endif
