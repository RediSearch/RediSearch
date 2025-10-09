/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "dist_hybrid_plan.h"
#include "hybrid/hybrid_request.h"

// should make sure the product of AREQ_BuildPipeline(areq, &req->errors[i]) would result in rpSorter only (can set up the aggplan to be a sorter only)
int HybridRequest_BuildDistributedDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params) {
  // Create synchronization context for coordinating depleter processors
  // This ensures thread-safe access when multiple depleters read from their pipelines
  StrongRef sync_ref = DepleterSync_New(req->nrequests, params->synchronize_read_locks);

  // Build individual pipelines for each search request
  for (size_t i = 0; i < req->nrequests; i++) {
      AREQ *areq = req->requests[i];

      // areq->rootiter = QAST_Iterate(&areq->ast, &areq->searchopts, AREQ_SearchCtx(areq), areq->reqflags, &req->errors[i]);
      AREQ_AddRequestFlags(areq,QEXEC_F_BUILDPIPELINE_NO_ROOT);

      int rc = AREQ_BuildPipeline(areq, &req->errors[i]);
      if (rc != REDISMODULE_OK) {
          StrongRef_Release(sync_ref);
          return REDISMODULE_ERR;
      }

      // Obtain the query processing context for the current AREQ
      QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(areq);
      // Set the result limit for the current AREQ - hack for now, should use window value
      if (IsHybridVectorSubquery(areq)){
        qctx->resultLimit = areq->maxAggregateResults;
      } else if (IsHybridSearchSubquery(areq)) {
        qctx->resultLimit = areq->maxSearchResults;
      }
      // Create a depleter processor to extract results from this pipeline
      // The depleter will feed results to the hybrid merger
      RedisSearchCtx *nextThread = params->aggregationParams.common.sctx; // We will use the context provided in the params
      RedisSearchCtx *depletingThread = AREQ_SearchCtx(areq); // when constructing the AREQ a new context should have been created
      ResultProcessor *depleter = RPDepleter_New(StrongRef_Clone(sync_ref), depletingThread, nextThread);
      QITR_PushRP(qctx, depleter);
  }

  // Release the sync reference as depleters now hold their own references
  StrongRef_Release(sync_ref);
  return REDISMODULE_OK;
}

static void hybridRequestSetupCoordinatorSubqueriesRequests(HybridRequest *hreq, const HybridPipelineParams *hybridParams, RLookup *tailLookup) {
  RS_ASSERT(hybridParams->scoringCtx);
  size_t window = hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF ? hybridParams->scoringCtx->rrfCtx.window : hybridParams->scoringCtx->linearCtx.window;

  bool isKNN = hreq->requests[VECTOR_INDEX]->ast.root->type == QN_VECTOR;
  size_t K = isKNN ? hreq->requests[VECTOR_INDEX]->ast.root->vn.vq->knn.k : 0;

  // Put all the loaded keys in the tail so he is aware of them
  for (size_t i = 0; i < hreq->nrequests; i++) {
    AREQ *areq = hreq->requests[i];
    PLN_LoadStep *step = (PLN_LoadStep *)AGPLN_FindStep(AREQ_AGGPlan(areq), NULL, NULL, PLN_T_LOAD);
    if (!step) {
      continue;
    }
    while (!AC_IsAtEnd(&step->args)) {
      size_t length = 0;
      const char *s = AC_GetStringNC(&step->args, &length);
      if (!s) {
        continue;
      }
      RLookup_GetKey_ReadEx(tailLookup, s, length, RLOOKUP_F_NOFLAGS);
    }
  }

  array_free_ex(hreq->requests, AREQ_Free(*(AREQ**)ptr));
  hreq->requests = MakeDefaultHybridUpstreams(hreq->sctx);
  hreq->nrequests = array_len(hreq->requests);

  AREQ_AddRequestFlags(hreq->requests[SEARCH_INDEX], QEXEC_F_IS_HYBRID_COORDINATOR_SUBQUERY);
  AREQ_AddRequestFlags(hreq->requests[VECTOR_INDEX], QEXEC_F_IS_HYBRID_COORDINATOR_SUBQUERY);

  PLN_ArrangeStep *searchArrangeStep = AGPLN_GetOrCreateArrangeStep(AREQ_AGGPlan(hreq->requests[SEARCH_INDEX]));
  searchArrangeStep->limit = window;

  PLN_ArrangeStep *vectorArrangeStep = AGPLN_GetOrCreateArrangeStep(AREQ_AGGPlan(hreq->requests[VECTOR_INDEX]));
  if (isKNN) {
    // Vector subquery is a KNN query
    // Heapsize should be min(window, KNN K)
    // ast structure is: root = vector node <- filter node <- ... rest
    vectorArrangeStep->limit = MIN(window, K);
  } else {
    // its range, limit = window
    vectorArrangeStep->limit = window;
  }
}

int HybridRequest_BuildDistributedPipeline(HybridRequest *hreq,
    HybridPipelineParams *hybridParams,
    AREQDIST_UpstreamInfo *us,
    QueryError *status) {

    RLookup *lookup = AGPLN_GetLookup(HybridRequest_TailAGGPlan(hreq), NULL, AGPLN_GETLOOKUP_FIRST);
    RS_ASSERT(lookup);

    lookup->options |= RLOOKUP_OPT_UNRESOLVED_OK;
    hybridRequestSetupCoordinatorSubqueriesRequests(hreq, hybridParams, lookup);

    int rc = HybridRequest_BuildDistributedDepletionPipeline(hreq, hybridParams);
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    rc = HybridRequest_BuildMergePipeline(hreq, hybridParams, lookup);
    lookup->options &= ~RLOOKUP_OPT_UNRESOLVED_OK;
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    // Collect unresolved fields from tail lookup for LOAD command
    std::vector<const RLookupKey *> loadFields;
    for (RLookupKey *kk = lookup->head; kk != NULL; kk = kk->next) {
        if (kk->flags & RLOOKUP_F_UNRESOLVED) {
            loadFields.push_back(kk);
        }
    }

    auto dstp = (PLN_DistributeStep *)AGPLN_FindStep(HybridRequest_TailAGGPlan(hreq), NULL, NULL, PLN_T_DISTRIBUTE);
    RS_ASSERT(dstp);
    auto &ser_args = *dstp->serialized;
    if (!loadFields.empty()) {
        ser_args.push_back(rm_strndup("LOAD", 4));
        char *ldsze;
        rm_asprintf(&ldsze, "%lu", (unsigned long)loadFields.size());
        ser_args.push_back(ldsze);
        for (auto kk : loadFields) {
            ser_args.push_back(rm_strndup(kk->name, kk->name_len));
        }
    }

    us->lookup = &dstp->lk;
    us->serialized = ser_args.data();
    us->nserialized = ser_args.size();
    return REDISMODULE_OK;
}
