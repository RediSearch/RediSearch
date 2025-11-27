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
#include "hybrid/hybrid_lookup_context.h"


static void pushDepleter(QueryProcessingCtx *qctx, ResultProcessor *depleter) {
  depleter->upstream = qctx->endProc;
  depleter->parent = qctx;
  qctx->endProc = depleter;
}

// should make sure the product of AREQ_BuildPipeline(areq, &req->errors[i]) would result in rpSorter only (can set up the aggplan to be a sorter only)
int HybridRequest_BuildDistributedDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params) {
  // Create synchronization context for coordinating depleter processors
  // We avoid taking the index lock since we are not directly accessing the index at all
  // This avoids deadlocks with main thread while it is trying to access the index
  StrongRef sync_ref = DepleterSync_New(req->nrequests, false);

  // Build individual pipelines for each search request
  for (size_t i = 0; i < req->nrequests; i++) {
      AREQ *areq = req->requests[i];

      AREQ_AddRequestFlags(areq, QEXEC_F_BUILDPIPELINE_NO_ROOT);

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
      pushDepleter(qctx, depleter);
  }

  // Release the sync reference as depleters now hold their own references
  StrongRef_Release(sync_ref);
  return REDISMODULE_OK;
}

static void serializeUnresolvedKeys(arrayof(char*) *target, std::vector<const RLookupKey *> &keys) {
  if (!keys.empty()) {
    array_append(*target, rm_strndup("LOAD", 4));
    char *buffer;
    rm_asprintf(&buffer, "%lu", (unsigned long)keys.size());
    array_append(*target, buffer);
    for (auto kk : keys) {
      // json paths should be serialized as is to avoid weird names
      if (kk->name[0] == '$') {
        buffer = rm_strndup(kk->name, kk->name_len);
      } else {
        rm_asprintf(&buffer, "@%.*s", (int)kk->name_len, kk->name);
      }
      array_append(*target, buffer);
    }
  }
}

arrayof(char*) HybridRequest_BuildDistributedPipeline(HybridRequest *hreq,
    HybridPipelineParams *hybridParams,
    RLookup **lookups,
    QueryError *status) {

    // The score alias for text is not part of a step to be distributed at this present time
    // We need to open the alias in the distributed lookup
    AREQ *searchReq = hreq->requests[SEARCH_INDEX];
    if (searchReq->searchopts.scoreAlias) {
      auto dstp = (PLN_DistributeStep *)AGPLN_FindStep(AREQ_AGGPlan(searchReq), NULL, NULL, PLN_T_DISTRIBUTE);
      RLookup_GetKey_Write(&dstp->lk, searchReq->searchopts.scoreAlias, RLOOKUP_F_NOFLAGS);
    }

    RLookup *tailLookup = AGPLN_GetLookup(HybridRequest_TailAGGPlan(hreq), NULL, AGPLN_GETLOOKUP_FIRST);
    // Init lookup since we dont call buildQueryPart
    RLookup_Init(tailLookup, IndexSpec_GetSpecCache(hreq->sctx->spec));

    int rc = HybridRequest_BuildDistributedDepletionPipeline(hreq, hybridParams);
    if (rc != REDISMODULE_OK) {
      // The error is set at either the tail or the subqueries error array
      // need to copy it to the status so it will be visible to the user
      HybridRequest_GetError(hreq, status);
      HybridRequest_ClearErrors(hreq);
      return NULL;
    }

    // Add keys from all source lookups to create unified schema before opening the score key
    HybridRequest_SynchronizeLookupKeys(hreq);

    // Open the key outside the RLOOKUP_OPT_UNRESOLVED_OK scope so it won't be marked as unresolved
    const RLookupKey *scoreKey = OpenMergeScoreKey(tailLookup, hybridParams->aggregationParams.common.scoreAlias, status);
    if (QueryError_HasError(status)) {
      return NULL;
    }

    tailLookup->options |= RLOOKUP_OPT_UNRESOLVED_OK;
    rc = HybridRequest_BuildMergePipeline(hreq, scoreKey, hybridParams);
    tailLookup->options &= ~RLOOKUP_OPT_UNRESOLVED_OK;
    if (rc != REDISMODULE_OK) {
      // The error is set at the tail, copy it into status
      QueryError_CloneFrom(&hreq->tailPipelineError, status);
      QueryError_ClearError(&hreq->tailPipelineError);
      return NULL;
    }

    std::vector<const RLookupKey *> unresolvedKeys;
    for (RLookupKey *kk = tailLookup->head; kk; kk = kk->next) {
      if (kk->flags & RLOOKUP_F_UNRESOLVED) {
        unresolvedKeys.push_back(kk);
      }
    }

    arrayof(char*) serialized = NULL;
    for (int i = 0; i < hreq->nrequests; i++) {
      AREQ *areq = hreq->requests[i];
      auto dstp = (PLN_DistributeStep *)AGPLN_FindStep(AREQ_AGGPlan(areq), NULL, NULL, PLN_T_DISTRIBUTE);
      RS_ASSERT(dstp);
      for (const RLookupKey *kk : unresolvedKeys) {
        // Add the unresolved keys to the upstream lookup since we will add them to the LOAD clause
        RLookup_GetKey_Write(&dstp->lk, kk->name, kk->flags & ~RLOOKUP_F_UNRESOLVED);
      }
      serializeUnresolvedKeys(&dstp->serialized, unresolvedKeys);
      lookups[i] = &dstp->lk;
      serialized = dstp->serialized;
    }
    return serialized;
}
