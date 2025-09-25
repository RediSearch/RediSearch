/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hybrid/hybrid_request.h"
#include "hybrid/hybrid_exec.h"
#include "hybrid/dist_hybrid_plan.h"
#include "hybrid/parse_hybrid.h"
#include "dist_plan.h"
#include "rmr/rmr.h"
#include "rmutil/util.h"
#include "commands.h"
#include "rpnet.h"
#include "hybrid_dispatcher.h"


static void nopCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  MRIteratorCallback_AddReply(ctx, rep);
  MRIteratorCallback_Done(ctx, 0);
}

static int rpnetNext_StartDispatcher(ResultProcessor *rp, SearchResult *r) {
  RPNet *nc = (RPNet *)rp;
  HybridDispatcher *dispatcher = nc->dispatcher;

  //TODO:len is number of shards
  arrayof(CursorMapping *) searchMappings = array_new(CursorMapping* ,10);
  arrayof(CursorMapping *) vsimMappings = array_new(CursorMapping*, 10);


  // TODO: better initialization of the mappings (pipeline building flow dependent)
  HybridDispatcher_SetMappingArray(dispatcher, searchMappings, true);
  HybridDispatcher_SetMappingArray(dispatcher, vsimMappings, false);

  if (!HybridDispatcher_IsStarted(dispatcher)) {
    HybridDispatcher_Dispatch(dispatcher);
  } else {
    // Wait for completion - this can be called from multiple threads
    HybridDispatcher_WaitForMappingsComplete(dispatcher);
  }
  // for hybrid commands, the index name is at position 1
  const char *idx = MRCommand_ArgStringPtrLen(&dispatcher->cmd, 1, NULL);

  MRCommand cmd = MR_NewCommand(4, "_FT.CURSOR", "READ", idx);
  nc->it = MR_IterateWithPrivateData(&nc->cmd, nopCallback, NULL, iterCursorMappingCb, searchMappings);
  nc->base.Next = rpnetNext;
  return rpnetNext(rp, r);
}


// The function transforms FT.HYBRID index SEARCH query VSIM field vector
// into _FT.HYBRID index SEARCH query VSIM field vector WITHCURSOR
// _NUM_SSTRING _INDEX_PREFIXES ...
static void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                                       AREQDIST_UpstreamInfo *us, MRCommand *xcmd,
                                       IndexSpec *sp, HybridPipelineParams *hybridParams) {
  // We need to prepend the array with the command, index, and query that
  // we want to use.
  const char **tmparr = array_new(const char *, us->nserialized);

  const char *index_name = RedisModule_StringPtrLen(argv[1], NULL);

  // Build _FT.HYBRID command (no profiling support)
  array_append(tmparr, "_FT.HYBRID");                           // Command
  array_append(tmparr, index_name);                             // Index name

  // Add the hybrid query arguments (SEARCH ... VSIM ...)
  for (int i = 2; i < argc; i++) {
    array_append(tmparr, RedisModule_StringPtrLen(argv[i], NULL));
  }

  // Add WITHCURSOR
  array_append(tmparr, "WITHCURSOR");
  // Numeric responses are encoded as simple strings.
  array_append(tmparr, "_NUM_SSTRING");

  // Add the index prefixes to the command, for validation in the shard
  array_append(tmparr, "_INDEX_PREFIXES");
  arrayof(HiddenUnicodeString*) prefixes = sp->rule->prefixes;
  char *n_prefixes;
  rm_asprintf(&n_prefixes, "%u", array_len(prefixes));
  array_append(tmparr, n_prefixes);
  for (uint i = 0; i < array_len(prefixes); i++) {
    array_append(tmparr, HiddenUnicodeString_GetUnsafe(prefixes[i], NULL));
  }

  // Add upstream serialized parameters
  for (size_t ii = 0; ii < us->nserialized; ++ii) {
    array_append(tmparr, us->serialized[ii]);
  }

  *xcmd = MR_NewCommandArgv(array_len(tmparr), tmparr);

  // Handle PARAMS if present
  int loc = RMUtil_ArgIndex("PARAMS", argv + 2, argc - 2);
  if (loc != -1) {
    long long nargs;
    int rc = RedisModule_StringToLongLong(argv[loc + 2 + 1], &nargs);

    // append params string including PARAMS keyword and nargs
    for (int i = 0; i < nargs + 2; ++i) {
      MRCommand_AppendRstr(xcmd, argv[loc + 2 + i]);
    }
  }

  // Handle TIMEOUT if present
  int timeout_index = RMUtil_ArgIndex("TIMEOUT", argv + 2, argc - 3);
  if (timeout_index != -1) {
    MRCommand_AppendRstr(xcmd, argv[timeout_index + 2]);
    MRCommand_AppendRstr(xcmd, argv[timeout_index + 3]);
  }

  // Handle DIALECT if present
  int dialect_index = RMUtil_ArgIndex("DIALECT", argv + 2, argc - 3);
  if (dialect_index != -1) {
    MRCommand_AppendRstr(xcmd, argv[dialect_index + 2]);
    MRCommand_AppendRstr(xcmd, argv[dialect_index + 3]);
  }

  array_free(tmparr);
  rm_free(n_prefixes);
}

// TODO: Fix this
static void HybridRequest_buildDistRPChain(HybridRequest *hreq, MRCommand *xcmd,
                      AREQDIST_UpstreamInfo *us, HybridDispatcher *dispatcher) {
  // Establish our root processor, which is the distributed processor
  RPNet *rpRoot = RPNet_New(xcmd, rpnetNext_StartDispatcher); // This will take ownership of the command
  RPNet_SetDispatcher(rpRoot, dispatcher);
  QueryProcessingCtx *qctx = &hreq->tailPipeline->qctx;
  rpRoot->base.parent = qctx;
  rpRoot->lookup = us->lookup;
  // TODO: Is this enough?
  rpRoot->areq = NULL; // Hybrid requests don't use a single AREQ - they manage multiple requests internally

  // RS_ASSERT(!qctx->rootProc);
  // Get the deepest-most root:
  int found = 0;
  for (ResultProcessor *rp = qctx->endProc; rp; rp = rp->upstream) {
    if (!rp->upstream) {
      rp->upstream = &rpRoot->base;
      found = 1;
      break;
    }
  }

  // update root and end with RPNet
  qctx->rootProc = &rpRoot->base;
  if (!found) {
    qctx->endProc = &rpRoot->base;
  }
}

// should make sure the product of AREQ_BuildPipeline(areq, &req->errors[i]) would result in rpSorter only (can set up the aggplan to be a sorter only)
int HybridRequest_BuildDistributedDepletionPipeline(HybridRequest *req, const HybridPipelineParams *params, MRCommand *xcmd) {
  // Create synchronization context for coordinating depleter processors
  // This ensures thread-safe access when multiple depleters read from their pipelines
  StrongRef sync_ref = DepleterSync_New(req->nrequests, params->synchronize_read_locks);
  StrongRef dispatcher_ref = HybridDispatcher_New(xcmd, 4);
  HybridDispatcher *dispatcher = StrongRef_Get(dispatcher_ref);

  // Build individual pipelines for each search request
  for (size_t i = 0; i < req->nrequests; i++) {
      AREQ *areq = req->requests[i];

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

      RPNet *rpNet = RPNet_New(xcmd, rpnetNext_StartDispatcher);
      RPNet_SetDispatcher(rpNet, dispatcher);
      QITR_PushRP(qctx, &rpNet->base);
  }

  // Release the sync reference as depleters now hold their own references
  StrongRef_Release(sync_ref);
  return REDISMODULE_OK;
}

static int HybridRequest_prepareForExecution(HybridRequest *hreq, RedisModuleCtx *ctx,
        RedisModuleString **argv, int argc, IndexSpec *sp, QueryError *status) {

    hreq->tailPipeline->qctx.err = status;

    // Parse the hybrid command (equivalent to AREQ_Compile)
    HybridPipelineParams hybridParams = {0};
    ParseHybridCommandCtx cmd = {0};
    cmd.search = hreq->requests[SEARCH_INDEX];
    cmd.vector = hreq->requests[VECTOR_INDEX];
    cmd.cursorConfig = &hreq->cursorConfig;
    cmd.hybridParams = &hybridParams;
    cmd.tailPlan = &hreq->tailPipeline->ap;
    cmd.reqConfig = &hreq->reqConfig;

    const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
    int rc = parseHybridCommand(ctx, argv, argc, hreq->sctx, indexname, &cmd, status);
    // we only need parse the combine and what comes after it
    // we can manually create the subqueries pipelines (depleter -> sorter(window)-> RPNet(shared dispatcher ))
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    // Set request flags from hybridParams
    hreq->reqflags = hybridParams.aggregationParams.common.reqflags;

    // TODO: Do we need a different AGGPLN_Hybrid_Distribute()?
    // by now I'm reusing AGGPLN_Distribute()
    rc = AGGPLN_Distribute(HybridRequest_TailAGGPlan(hreq), status);
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    AREQDIST_UpstreamInfo us = {NULL};
    rc = HybridRequest_BuildDistributedPipeline(hreq, &hybridParams, &us, status);
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    // Construct the command string
    MRCommand xcmd;
    HybridRequest_buildMRCommand(argv, argc, &us, &xcmd, sp, &hybridParams);
    xcmd.protocol = is_resp3(ctx) ? 3 : 2;
    // xcmd.forCursor = hreq->reqflags & QEXEC_F_IS_CURSOR;
    // xcmd.forProfiling = false;  // No profiling support for hybrid yet
    // xcmd.rootCommand = C_AGG;   // Response is equivalent to a `CURSOR READ` response

    // TODO: Build the result processor chain
    StrongRef dispatcher_ref = HybridDispatcher_New(&xcmd, 4);
    HybridDispatcher *dispatcher = StrongRef_Get(dispatcher_ref);
    HybridRequest_buildDistRPChain(hreq->requests[1], &xcmd, &us, dispatcher);
    HybridRequest_buildDistRPChain(hreq->requests[2], &xcmd, &us, dispatcher);

    return REDISMODULE_OK;
}

static int HybridRequest_executePlan(HybridRequest *hreq, struct ConcurrentCmdCtx *cmdCtx,
                        RedisModule_Reply *reply, QueryError *status) {
    bool isCursor = hreq->reqflags & QEXEC_F_IS_CURSOR;
    if (isCursor) {
        // TODO:
        // // Keep the original concurrent context
        // ConcurrentCmdCtx_KeepRedisCtx(cmdCtx);

        // StrongRef dummy_spec_ref = {.rm = NULL};

        // if (HybridRequest_StartCursor(hreq, reply, dummy_spec_ref, status, true) != REDISMODULE_OK) {
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
