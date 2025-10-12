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
      } else {
        RS_ASSERT(IsHybridSearchSubquery(areq));
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

// modifies the load step to include the doc key
static bool addKeysToLoadStep(SerializedSteps *target, std::vector<const RLookupKey *> &keys) {
  arrayof(char *) *arr = &target->steps[PLN_T_LOAD];
  if (SerializedSteps_AddStepOnce(target, PLN_T_LOAD)) {
    // If we didn't have a load step, fill it up
    array_append(*arr, rm_strndup("LOAD", 4));
    char *noCount = NULL;
    array_append(*arr, noCount);
  } else if (array_len(*arr) < 2) {
    return false;
  }
  
  size_t count = array_len(*arr) - 2 + keys.size() /* LOAD <count> */;
  if ((*arr)[1]) {
    rm_free((*arr)[1]);
  }
  rm_asprintf(&(*arr)[1], "%zu", count);
  for (const RLookupKey *kk : keys) {
    array_append(*arr, rm_strndup(kk->name, kk->name_len));
  }
  return true;
}

SerializedSteps *HybridRequest_BuildDistributedPipeline(HybridRequest *hreq,
    HybridPipelineParams *hybridParams,
    RLookup **lookups,
    QueryError *status) {

    RLookup *tailLookup = AGPLN_GetLookup(HybridRequest_TailAGGPlan(hreq), NULL, AGPLN_GETLOOKUP_FIRST);
    // Init lookup since we dont call buildQueryPart
    RLookup_Init(tailLookup, IndexSpec_GetSpecCache(hreq->sctx->spec));

    int rc = HybridRequest_BuildDistributedDepletionPipeline(hreq, hybridParams);
    if (rc != REDISMODULE_OK) return NULL;

    tailLookup->options |= RLOOKUP_OPT_UNRESOLVED_OK;
    rc = HybridRequest_BuildMergePipeline(hreq, hybridParams);
    tailLookup->options &= ~RLOOKUP_OPT_UNRESOLVED_OK;
    if (rc != REDISMODULE_OK) return NULL;

    std::vector<const RLookupKey *> unresolvedKeys;
    for (RLookupKey *kk = tailLookup->head; kk; kk = kk->next) {
      if (kk->flags & RLOOKUP_F_UNRESOLVED) {
        unresolvedKeys.push_back(kk);
      }
    }

    SerializedSteps *serialized = NULL;
    for (int i = 0; i < hreq->nrequests; i++) {
      AREQ *areq = hreq->requests[i];
      auto dstp = (PLN_DistributeStep *)AGPLN_FindStep(AREQ_AGGPlan(areq), NULL, NULL, PLN_T_DISTRIBUTE);
      RS_ASSERT(dstp);
      for (const RLookupKey *kk : unresolvedKeys) {
        // Add the unresolved keys to the upstream lookup since we will add them to the LOAD clause
        RLookup_GetKey_Write(&dstp->lk, kk->name, kk->flags & ~RLOOKUP_F_UNRESOLVED);
      }
      addKeysToLoadStep(&dstp->serialized, unresolvedKeys);
      lookups[i] = &dstp->lk;
      serialized = &dstp->serialized;
    }
    return serialized;
}
