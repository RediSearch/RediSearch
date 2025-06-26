#include "hybrid/hybrid_request.h"
#include "aggregate/aggregate_pipeline.h"

typedef struct HybridScoringContext HybridScoringContext;

ResultProcessor *RPHybridMerger_New(HybridScoringContext *hybridScoringCtx, ResultProcessor **upstreams, size_t numUpstreams);


HybridRequest *HybridRequest_New(AREQ *requests, size_t nrequests, AGGPlan *plan, QueryError *status, RSSearchOptions *searchOpts, RSTimeoutPolicy timeoutPolicy) {
    HybridRequest *req = rm_calloc(1, sizeof(*req));
    req->requests = requests;
    req->nrequests = nrequests;
    AGPLN_Init(&req->merge.ap);
    req->merge.qctx.err = status;
    req->merge.qctx.timeoutPolicy = timeoutPolicy;
    req->merge.qctx.rootProc = req->merge->qctx.endProc = NULL;
    StrongRef sync_ref = DepleterSync_New();
    ResultProcessor *depleters = NULL;
    for (size_t i = 0; i < nrequests; i++) {
        AREQ *areq = &requests[i];
        ResultProcessor *rp = RPDepleter_New(StrongRef_Clone(sync_ref));
        array_ensure_append_1(depleters, rp);
        QITR_PushRP(&areq->pipeline.qctx, rp);
    }
    StrongRef_Release(sync_ref);
    ResultProcessor *merger = RPHybridMerger_New(NULL, depleters, nrequests);
    QITR_PushRP(&req->merge, merger);
    BuildPipeline(&req->merge, NULL, searchOpts, status, timeoutPolicy);
    return req;
}

void HybridRequest_Free(HybridRequest *req) {
    for (size_t i = 0; i < req->nrequests; i++) {
        AREQ_Free(&req->requests[i]);
    }
    array_free(req->requests);
    AGPLN_FreeSteps(&req->merge.ap);
    rm_free(req);
}