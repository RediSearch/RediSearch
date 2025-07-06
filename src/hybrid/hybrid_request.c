#include "hybrid/hybrid_request.h"
#include "pipeline/pipeline.h"
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

int HybridRequest_BuildPipeline(HybridRequest *req, const AggregationPipelineParams *params, bool synchronize_read_locks) {
    StrongRef sync_ref = DepleterSync_New(req->nrequests, synchronize_read_locks);
    PLN_LoadStep *loadStep = (PLN_LoadStep *)AGPLN_FindStep(&req->tail.ap, NULL, NULL, PLN_T_LOAD);
    arrayof(ResultProcessor*) depleters = array_new(ResultProcessor *, req->nrequests);
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ *areq = req->requests[i];
        // Build the pipeline based on the areq AGGPlan
        int rc = AREQ_BuildPipeline(areq, &req->errors[i]);
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
            ResultProcessor *loader = RPLoader_New(AREQ_SearchCtx(areq), areq->reqflags, lastLookup, clonedKeys, loadStep->nkeys, false, &areq->stateflags);
            QITR_PushRP(qctx, loader);
        }
        ResultProcessor *depleter = RPDepleter_New(StrongRef_Clone(sync_ref), AREQ_SearchCtx(areq));
        array_ensure_append_1(depleters, depleter);
        QITR_PushRP(qctx, depleter);
    }
    
    StrongRef_Release(sync_ref);
    ResultProcessor *merger = RPHybridMerger_New(&req->scoringCtx, depleters, req->nrequests);
    QITR_PushRP(&req->tail.qctx, merger);

    if (loadStep) {
        AGPLN_PopStep(&loadStep->base);
    }
    uint32_t stateFlags = 0;
    QueryPipeline_BuildAggregationPart(&req->tail, params, &stateFlags);
    if (loadStep) {
        AGPLN_AddStep(&req->tail.ap, &loadStep->base);
    }
    return REDISMODULE_OK;
}

HybridRequest *HybridRequest_New(AREQ **requests, size_t nrequests) {
    HybridRequest *req = rm_calloc(1, sizeof(*req));
    req->requests = requests;
    req->nrequests = nrequests;
    req->errors = array_new(QueryError, nrequests);
    AGPLN_Init(&req->tail.ap);
    req->tail.qctx.timeoutPolicy = requests[0]->pipeline.qctx.timeoutPolicy;
    req->tail.qctx.rootProc = req->tail.qctx.endProc = NULL;
    req->tail.qctx.err = &req->tailError;
    QueryError_Init(&req->tailError);
    QueryPipeline_Initialize(&req->tail, requests[0]->pipeline.qctx.timeoutPolicy, &req->tailError);
    for (size_t i = 0; i < nrequests; i++) {
        QueryError_Init(&req->errors[i]);
        QueryPipeline_Initialize(&requests[i]->pipeline, requests[i]->reqConfig.timeoutPolicy, &req->errors[i]);
    }
    return req;
}

void HybridRequest_Free(HybridRequest *req) {
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ_Free(req->requests[i]);
    }
    array_free(req->requests);
    array_free(req->errors);
    QueryPipeline_Clean(&req->tail);
    rm_free(req);
}

#ifdef __cplusplus
}
#endif
