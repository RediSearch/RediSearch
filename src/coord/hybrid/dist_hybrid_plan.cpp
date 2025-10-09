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
#include <set>
#include <string>

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

      // RPNet *rpNet = RPNet_New(xcmd, rpnetNext_StartWithMappings);
      // RPNet_SetDispatcher(rpNet, dispatcher);
      // QITR_PushRP(qctx, &rpNet->base);
  }

  // Release the sync reference as depleters now hold their own references
  StrongRef_Release(sync_ref);
  return REDISMODULE_OK;
}

int HybridRequest_BuildDistributedPipeline(HybridRequest *hreq,
    HybridPipelineParams *hybridParams,
    arrayof(AREQDIST_UpstreamInfo) us,
    QueryError *status) {

    RLookup *tailLookup = AGPLN_GetLookup(HybridRequest_TailAGGPlan(hreq), NULL, AGPLN_GETLOOKUP_FIRST);

    int rc = HybridRequest_BuildDistributedDepletionPipeline(hreq, hybridParams);
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    tailLookup->options |= RLOOKUP_OPT_UNRESOLVED_OK;
    rc = HybridRequest_BuildMergePipeline(hreq, hybridParams, true);
    tailLookup->options &= ~RLOOKUP_OPT_UNRESOLVED_OK;
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;


    // Collect unresolved fields from tail lookup for LOAD command
    std::set<std::string> loadFields;
    for (RLookupKey *kk = tailLookup->head; kk != NULL; kk = kk->next) {
        if (kk->flags & RLOOKUP_F_UNRESOLVED && kk->name) {
            loadFields.emplace(kk->name, kk->name_len);
        }
    }

    for (int i = 0; i < hreq->nrequests; i++) {
        AREQ *areq = hreq->requests[i];
        auto dstp = (PLN_DistributeStep *)AGPLN_FindStep(AREQ_AGGPlan(areq), NULL, NULL, PLN_T_DISTRIBUTE);
        RS_ASSERT(dstp);
        auto &ser_args = *dstp->serialized;
        if (!loadFields.empty()) {
          ser_args.push_back(rm_strndup("LOAD", 4));
          char *ldsze;
          rm_asprintf(&ldsze, "%lu", (unsigned long)loadFields.size());
          ser_args.push_back(ldsze);
          for (auto& kk : loadFields) {
              ser_args.push_back(rm_strndup(kk.c_str(), kk.size()));
          }
          // This lookup goes to the rpnet - we need the lookup keys its write will be what the merger expects
          us[i].lookup = &dstp->lk;
          us[i].serialized = ser_args.data();
          us[i].nserialized = ser_args.size();
        }
      }
    return REDISMODULE_OK;
}
