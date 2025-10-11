/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "dist_hybrid.h"
#include "hybrid/hybrid_request.h"
#include "hybrid/hybrid_exec.h"
#include "hybrid/dist_hybrid_plan.h"
#include "hybrid/parse_hybrid.h"
#include "dist_plan.h"
#include "rmr/rmr.h"
#include "rmutil/util.h"
#include "commands.h"
#include "rpnet.h"
#include "hybrid_cursor_mappings.h"


// The function transforms FT.HYBRID index SEARCH query VSIM field vector
// into _FT.HYBRID index SEARCH query VSIM field vector WITHCURSOR
// _NUM_SSTRING _INDEX_PREFIXES ...
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                            AREQDIST_UpstreamInfo *us, MRCommand *xcmd,
                            IndexSpec *sp, HybridPipelineParams *hybridParams) {
  const char *index_name = RedisModule_StringPtrLen(argv[1], NULL);

  // Build _FT.HYBRID command (no profiling support)
  *xcmd = MR_NewCommand(2, "_FT.HYBRID", index_name);

  // Find the indices of the hybrid query components
  // SEARCH and VSIM are mandatory, COMBINE and PARAMS are optional
  int search_index = RMUtil_ArgIndex("SEARCH", argv + 2, argc - 2) + 2;
  int vsim_index = RMUtil_ArgIndex("VSIM", argv + search_index, argc - search_index) + search_index;

  // PARAMS is optional, but if present, it must come after VSIM + 2
  int params_index = RMUtil_ArgIndex("PARAMS", argv + vsim_index + 2, argc - (vsim_index + 2) );
  if (params_index != -1) {
    params_index += vsim_index + 2;
  }
  long long nparams;
  if (params_index == -1) {
    nparams = 0;
  } else {
    RedisModule_StringToLongLong(argv[params_index + 1], &nparams);
  }

  // Add SEARCH arguments
  int current_index = 2; // Skip the command and index name
  for (int i = current_index; i < vsim_index + 2; i++) {
    MRCommand_AppendRstr(xcmd, argv[i]);
  }
  current_index = vsim_index + 2;

  // Add VSIM + COMBINE arguments
  int limit = (params_index == -1 ? argc : params_index);
  for (int i = current_index; i < limit; i++) {
    if (i == vsim_index + 2 && argv[i] != '$') {
      MRCommand_AppendRstr(xcmd, argv[i]);
    } else {
      size_t len;
      const char *str = RedisModule_StringPtrLen(argv[i], &len);
      MRCommand_Append(xcmd, str, len);
    }
  }
  current_index = limit;

  // Add PARAMS arguments if present
  if (params_index != -1) {
    for (int i = params_index; i < params_index + nparams + 2; i++) {
      MRCommand_AppendRstr(xcmd, argv[i]);
    }
    current_index = params_index + nparams + 2;
  }

  // Add the remaining arguments (TIMEOUT, DIALECT, FILTER, etc.)
  for (int i = current_index; i < argc; i++) {
    size_t len;
    const char *str = RedisModule_StringPtrLen(argv[i], &len);
    MRCommand_Append(xcmd, str, len);
  }

  // Add WITHCURSOR
  MRCommand_Append(xcmd, "WITHCURSOR", strlen("WITHCURSOR"));

  MRCommand_Append(xcmd, "WITHSCORES", strlen("WITHSCORES"));
  // Numeric responses are encoded as simple strings.
  MRCommand_Append(xcmd, "_NUM_SSTRING", strlen("_NUM_SSTRING"));

}

// UPDATED: Set RPNet types when creating them
// NOTE: Caller should clone the dispatcher_ref before calling this function
static void HybridRequest_buildDistRPChain(AREQ *r, MRCommand *xcmd,
                          AREQDIST_UpstreamInfo *us,
                          int (*nextFunc)(ResultProcessor *, SearchResult *)) {
  // Establish our root processor, which is the distributed processor
  MRCommand cmd = MRCommand_Copy(xcmd);

  RPNet *rpRoot = RPNet_New(&cmd, nextFunc);

  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
  rpRoot->base.parent = qctx;
  rpRoot->lookup = us->lookup;
  rpRoot->areq = r;

  ResultProcessor *rpProfile = NULL;
  if (IsProfile(r)) {
    rpProfile = RPProfile_New(&rpRoot->base, qctx);
  }

  // RS_ASSERT(!AREQ_QueryProcessingCtx(r)->rootProc);
  // Get the deepest-most root:
  int found = 0;
  for (ResultProcessor *rp = AREQ_QueryProcessingCtx(r)->endProc; rp; rp = rp->upstream) {
    if (!rp->upstream) {
      rp->upstream = IsProfile(r) ? rpProfile : &rpRoot->base;
      found = 1;
      break;
    }
  }

  // update root and end with RPNet
  qctx->rootProc = &rpRoot->base;
  if (!found) {
    qctx->endProc = &rpRoot->base;
  }

  // allocate memory for replies and update endProc if necessary
  if (IsProfile(r)) {
    // 2 is just a starting size, as we most likely have more than 1 shard
    rpRoot->shardsProfile = array_new(MRReply*, 2);
    if (!found) {
      qctx->endProc = rpProfile;
    }
  }
}

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

      // RPNet *rpNet = RPNet_New(xcmd, rpnetNext_StartWithMappings);
      // RPNet_SetDispatcher(rpNet, dispatcher);
      // QITR_PushRP(qctx, &rpNet->base);
  }

  // Release the sync reference as depleters now hold their own references
  StrongRef_Release(sync_ref);
  return REDISMODULE_OK;
}

static void hybridRequestSetupCoordinatorSubqueriesRequests(HybridRequest *hreq, const HybridPipelineParams *hybridParams) {
  RS_ASSERT(hybridParams->scoringCtx);
  size_t window = hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF ? hybridParams->scoringCtx->rrfCtx.window : hybridParams->scoringCtx->linearCtx.window;

  bool isKNN = hreq->requests[VECTOR_INDEX]->ast.root->type == QN_VECTOR;
  size_t K = isKNN ? hreq->requests[VECTOR_INDEX]->ast.root->vn.vq->knn.k : 0;

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

static int HybridRequest_prepareForExecution(HybridRequest *hreq, RedisModuleCtx *ctx,
        RedisModuleString **argv, int argc, IndexSpec *sp, QueryError *status) {

    hreq->tailPipeline->qctx.err = status;

    // Initialize timeout for all subqueries BEFORE building pipelines
    for (int i = 0; i < hreq->nrequests; i++) {
        AREQ *subquery = hreq->requests[i];
        RedisSearchCtx *sctx = AREQ_SearchCtx(subquery);
        SearchCtx_UpdateTime(sctx, subquery->reqConfig.queryTimeoutMS);
    }

    // Parse the hybrid command (equivalent to AREQ_Compile)
    HybridPipelineParams hybridParams = {0};
    ParseHybridCommandCtx cmd = {0};
    cmd.search = hreq->requests[SEARCH_INDEX];
    cmd.vector = hreq->requests[VECTOR_INDEX];
    cmd.cursorConfig = &hreq->cursorConfig;
    cmd.hybridParams = &hybridParams;
    cmd.tailPlan = &hreq->tailPipeline->ap;
    cmd.reqConfig = &hreq->reqConfig;

    ArgsCursor ac = {0};
    HybridRequest_InitArgsCursor(hreq, &ac, argv, argc);
    int rc = parseHybridCommand(ctx, &ac, hreq->sctx, &cmd, status, false);
    // we only need parse the combine and what comes after it
    // we can manually create the subqueries pipelines (depleter -> sorter(window)-> RPNet(shared dispatcher ))
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    // // Set request flags from hybridParams
    // hreq->reqflags = hybridParams.aggregationParams.common.reqflags;

    // TODO: Do we need a different AGGPLN_Hybrid_Distribute()?
    // by now I'm reusing AGGPLN_Distribute()
    rc = AGGPLN_Distribute(HybridRequest_TailAGGPlan(hreq), status);
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    AREQDIST_UpstreamInfo us = {NULL};

    hybridRequestSetupCoordinatorSubqueriesRequests(hreq, &hybridParams);

    rc = HybridRequest_BuildDistributedDepletionPipeline(hreq, &hybridParams);
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    // AREQ_AddRequestFlags(hreq somehow,QEXEC_F_BUILDPIPELINE_NO_ROOT);

    rc = HybridRequest_BuildMergePipeline(hreq, &hybridParams);
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    // Construct the command string
    MRCommand xcmd;
    HybridRequest_buildMRCommand(argv, argc, &us, &xcmd, sp, &hybridParams);
    xcmd.protocol = is_resp3(ctx) ? 3 : 2;
    // xcmd.forCursor = hreq->reqflags & QEXEC_F_IS_CURSOR;
    // xcmd.forProfiling = false;  // No profiling support for hybrid yet
    xcmd.rootCommand = C_AGG;   // Response is equivalent to a `CURSOR READ` response

    // UPDATED: Use new start function with mappings (no dispatcher needed)
    HybridRequest_buildDistRPChain(hreq->requests[0], &xcmd, &us, rpnetNext_StartWithMappings);
    HybridRequest_buildDistRPChain(hreq->requests[1], &xcmd, &us, rpnetNext_StartWithMappings);

    // Free the command
    MRCommand_Free(&xcmd);
    return REDISMODULE_OK;
}

static void FreeCursorMappings(void *mappings) {
  CursorMappings *vsimOrSearch = (CursorMappings *)mappings;
  array_free(vsimOrSearch->mappings);
  rm_free(mappings);
}

static int HybridRequest_executePlan(HybridRequest *hreq, struct ConcurrentCmdCtx *cmdCtx,
                        RedisModule_Reply *reply, QueryError *status) {

    // Get RPNet structures from query context
    QueryProcessingCtx *searchQctx = AREQ_QueryProcessingCtx(hreq->requests[0]);
    QueryProcessingCtx *vsimQctx = AREQ_QueryProcessingCtx(hreq->requests[1]);
    RPNet *searchRPNet = (RPNet *)searchQctx->rootProc;
    RPNet *vsimRPNet = (RPNet *)vsimQctx->rootProc;
    RLookup *searchLookup = AGPLN_GetLookup(AREQ_AGGPlan(hreq->requests[0]), NULL, AGPLN_GETLOOKUP_FIRST);
    RLookup *vsimLookup = AGPLN_GetLookup(AREQ_AGGPlan(hreq->requests[1]), NULL, AGPLN_GETLOOKUP_FIRST);
    RLookup_Init(searchLookup, NULL);
    RLookup_Init(vsimLookup, NULL);
    searchRPNet->lookup = searchLookup;
    vsimRPNet->lookup = vsimLookup;

    CursorMappings *search = rm_calloc(1, sizeof(CursorMappings));
    CursorMappings *vsim = rm_calloc(1, sizeof(CursorMappings));
    search->type = TYPE_SEARCH;
    vsim->type = TYPE_VSIM;

    StrongRef searchMappingsRef = StrongRef_New(search, FreeCursorMappings);
    StrongRef vsimMappingsRef = StrongRef_New(vsim, FreeCursorMappings);


    // Get the command from the RPNet (it was set during prepareForExecution)
    MRCommand *cmd = &searchRPNet->cmd;
    int numShards = GetNumShards_UnSafe();

    if (!ProcessHybridCursorMappings(cmd, numShards, searchMappingsRef, vsimMappingsRef, status)) {
        // Handle error
        StrongRef_Release(searchMappingsRef);
        StrongRef_Release(vsimMappingsRef);
        return REDISMODULE_ERR;
    }

    // Store mappings in RPNet structures
    searchRPNet->mappings = StrongRef_Clone(searchMappingsRef);
    vsimRPNet->mappings = StrongRef_Clone(vsimMappingsRef);
    StrongRef_Release(searchMappingsRef);
    StrongRef_Release(vsimMappingsRef);

    bool isCursor = hreq->reqflags & QEXEC_F_IS_CURSOR;
    if (isCursor) {
        // // TODO:
        // // Keep the original concurrent context
        // ConcurrentCmdCtx_KeepRedisCtx(cmdCtx);

        // StrongRef dummy_spec_ref = {.rm = NULL};

        // if (HybridRequest_StartCursor(hreq, reply, &dummy_spec_ref, status, true) != REDISMODULE_OK) {
        //     return REDISMODULE_ERR;
        // }
    } else {
        // TODO: Validate cv use
        AGGPlan *plan = &hreq->tailPipeline->ap;
        cachedVars cv = {
            .lastLookup = AGPLN_GetLookup(plan, NULL, AGPLN_GETLOOKUP_LAST),
            .lastAstp = AGPLN_GetArrangeStep(plan)
        };
        sendChunk_hybrid(hreq, reply, UINT64_MAX, cv);
        HybridRequest_Free(hreq);
    }
    return REDISMODULE_OK;
}

static void DistHybridCleanups(RedisModuleCtx *ctx,
    struct ConcurrentCmdCtx *cmdCtx, IndexSpec *sp, StrongRef *strong_ref,
    HybridRequest *hreq, RedisModule_Reply *reply,
    QueryError *status) {

    RS_ASSERT(QueryError_HasError(status));
    QueryError_ReplyAndClear(ctx, status);
    WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    if (sp) {
        IndexSpecRef_Release(*strong_ref);
    }
    if (hreq) {
        HybridRequest_Free(hreq);
    }
    RedisModule_EndReply(reply);
}

void RSExecDistHybrid(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                        struct ConcurrentCmdCtx *cmdCtx) {
    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
    QueryError status = {0};

    // CMD, index, expr, args...
    const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
    RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
    if (!sctx) {
        QueryError_SetWithUserDataFmt(&status, QUERY_ENOINDEX, "No such index", " %s", indexname);
        // return QueryError_ReplyAndClear(ctx, &status);
        goto err;
    }

    // Check if the index still exists, and promote the ref accordingly
    StrongRef strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    IndexSpec *sp = StrongRef_Get(strong_ref);
    if (!sp) {
        QueryError_SetCode(&status, QUERY_EDROPPEDBACKGROUND);
        goto err;
    }

    HybridRequest *hreq = MakeDefaultHybridRequest(sctx);

    if (HybridRequest_prepareForExecution(hreq, ctx, argv, argc, sp, &status) != REDISMODULE_OK) {
        goto err;
    }


    if (HybridRequest_executePlan(hreq, cmdCtx, reply, &status) != REDISMODULE_OK) {
        goto err;
    }
    WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    IndexSpecRef_Release(strong_ref);
    RedisModule_EndReply(reply);
    return;

err:
    DistHybridCleanups(ctx, cmdCtx, sp, &strong_ref, hreq, reply, &status);
}
