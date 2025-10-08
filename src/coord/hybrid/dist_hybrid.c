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
  int argOffset;
  const char *index_name = RedisModule_StringPtrLen(argv[1], NULL);

  // Build _FT.HYBRID command (no profiling support)
  *xcmd = MR_NewCommand(2, "_FT.HYBRID", index_name);

  // Add SEARCH
  int searchOffset = RMUtil_ArgIndex("SEARCH", argv, argc);
  MRCommand_AppendRstr(xcmd, argv[searchOffset]);
  MRCommand_AppendRstr(xcmd, argv[searchOffset + 1]);

  // Add VSIM
  int vsimOffset = RMUtil_ArgIndex("VSIM", argv, argc);
  MRCommand_AppendRstr(xcmd, argv[vsimOffset]);
  MRCommand_AppendRstr(xcmd, argv[vsimOffset + 1]);

  size_t paramLen;
  const char *paramStr = RedisModule_StringPtrLen(argv[vsimOffset + 2], &paramLen);
  if (paramLen > 0 && paramStr[0] == '$') {
    // It's a parameter placeholder - forward as is
    MRCommand_AppendRstr(xcmd, argv[vsimOffset + 2]);
  } else {
    // It's raw data - forward as binary
    MRCommand_Append(xcmd, paramStr, paramLen);
  }


  // Add KNN/RANGE and its arguments
  argOffset = RMUtil_ArgIndex("KNN", argv + vsimOffset, argc - vsimOffset);
  if (argOffset == -1) {
    argOffset = RMUtil_ArgIndex("RANGE", argv + vsimOffset, argc - vsimOffset);
  }
  argOffset = argOffset != -1 ? argOffset + vsimOffset : -1;
  if (argOffset != -1 && argOffset < argc - 2) {
    long long nargs;
    RedisModule_StringToLongLong(argv[argOffset + 1], &nargs);

    for (int i = 0; i < nargs + 2; ++i) {
      MRCommand_AppendRstr(xcmd, argv[argOffset + i]);
    }
  }

  // Add VSIM FILTER
  int vsimFilterOffset = RMUtil_ArgIndex("FILTER", argv + vsimOffset, argc - vsimOffset);
  vsimFilterOffset = vsimFilterOffset != -1 ? vsimFilterOffset + vsimOffset : -1;
  bool hasFilter = vsimFilterOffset != -1;

  int combineOffset = RMUtil_ArgIndex("COMBINE", argv + vsimOffset, argc - vsimOffset);
  combineOffset = combineOffset != -1 ? combineOffset + vsimOffset : -1;
  bool hasCombine = combineOffset != -1;

  // Possible cases:
  // VSIM ... FILTER xx
  // VSIM ... FILTER xx COMBINE xx
  // VSIM ... FILTER xx COMBINE xx FILTER xx
  // VSIM ... FILTER xx FILTER xx
  // VSIM ... COMBINE xx FILTER xx --> This is a post query filter ignore it
  if ((vsimFilterOffset < argc - 2) &&
        (   hasFilter && !hasCombine ||
            (hasFilter && hasCombine && (combineOffset > vsimFilterOffset))
        )
    ) {
    MRCommand_AppendRstr(xcmd, argv[vsimFilterOffset]);
    MRCommand_AppendRstr(xcmd, argv[vsimFilterOffset + 1]);
  }

  // Add COMBINE
  if (combineOffset != -1) {
    MRCommand_AppendRstr(xcmd, argv[combineOffset]);
    // Add RRF/LINEAR and its arguments
    argOffset = RMUtil_ArgIndex("RRF", argv + vsimOffset, argc - vsimOffset);
    if (argOffset == -1) {
      argOffset = RMUtil_ArgIndex("LINEAR", argv + vsimOffset, argc - vsimOffset);
    }
    argOffset = argOffset != -1 ? argOffset + vsimOffset : -1;
    if (argOffset != -1 && argOffset < argc - 2) {
      long long nargs;
      RedisModule_StringToLongLong(argv[argOffset + 1], &nargs);

      for (int i = 0; i < nargs + 2; ++i) {
        MRCommand_AppendRstr(xcmd, argv[argOffset + i]);
      }
    }
  }

  // Add LOAD arguments from upstream info
  for (size_t ii = 0; ii < us->nserialized; ++ii) {
    MRCommand_Append(xcmd, us->serialized[ii], strlen(us->serialized[ii]));
  }

  // Add PARAMS arguments if present
  argOffset = RMUtil_ArgIndex("PARAMS", argv + vsimOffset, argc - vsimOffset);
  argOffset = argOffset != -1 ? argOffset + vsimOffset : -1;
  if (argOffset != -1) {
    long long nargs;
    int rc = RedisModule_StringToLongLong(argv[argOffset + 1], &nargs);

    // PARAMS keyword and count - treat as string
    MRCommand_AppendRstr(xcmd, argv[argOffset]);
    MRCommand_AppendRstr(xcmd, argv[argOffset + 1]);

    // append params string including PARAMS keyword and nargs
    for (int i = 2; i < nargs + 2; ++i) {
      if (i % 2 == 0) {
        // Parameter name - treat as string
        MRCommand_AppendRstr(xcmd, argv[argOffset + i]);
      } else {
        // Parameter value - could be binary, treat as binary
        size_t valueLen;
        const char *valueData = RedisModule_StringPtrLen(argv[argOffset + i], &valueLen);
        MRCommand_Append(xcmd, valueData, valueLen);
      }
    }
  }

  // check for timeout argument and append it to the command.
  // If TIMEOUT exists, it was already validated at AREQ_Compile.
  argOffset = RMUtil_ArgIndex("TIMEOUT", argv, argc);
  if (argOffset != -1) {
    MRCommand_AppendRstr(xcmd, argv[argOffset]);
    MRCommand_AppendRstr(xcmd, argv[argOffset + 1]);
  }

  // Add WITHCURSOR
  MRCommand_Append(xcmd, "WITHCURSOR", strlen("WITHCURSOR"));

  MRCommand_Append(xcmd, "WITHSCORES", strlen("WITHSCORES"));
  // Numeric responses are encoded as simple strings.
  MRCommand_Append(xcmd, "_NUM_SSTRING", strlen("_NUM_SSTRING"));

  // At the end of HybridRequest_buildMRCommand, add:
  RedisModule_Log(NULL, REDISMODULE_LOGLEVEL_DEBUG, "Final MRCommand has %d args:", xcmd->num);
  for (int i = 0; i < xcmd->num; i++) {
    if (xcmd->strs[i] && xcmd->lens[i] > 0) {
      RedisModule_Log(NULL, REDISMODULE_LOGLEVEL_DEBUG, "  [%d]: %.*s (len=%zu)", i, (int)xcmd->lens[i], xcmd->strs[i], xcmd->lens[i]);
    } else {
      RedisModule_Log(NULL, REDISMODULE_LOGLEVEL_DEBUG, "  [%d]: NULL or empty", i);
    }
  }
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

    ArgsCursor ac = {0};
    HybridRequest_InitArgsCursor(hreq, &ac, argv, argc);
    int rc = parseHybridCommand(ctx, &ac, hreq->sctx, &cmd, status, false);
    // we only need parse the combine and what comes after it
    // we can manually create the subqueries pipelines (depleter -> sorter(window)-> RPNet(shared dispatcher ))
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    // // Set request flags from hybridParams
    // hreq->reqflags = hybridParams.aggregationParams.common.reqflags;

    rc = Hybrid_AGGPLN_Distribute(HybridRequest_TailAGGPlan(hreq), status);
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    AREQDIST_UpstreamInfo us = {0};
    rc = HybridRequest_BuildDistributedPipeline(hreq, &hybridParams, &us, status);
    if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

    // Construct the command string
    MRCommand xcmd;
    HybridRequest_buildMRCommand(argv, argc, &us, &xcmd, sp, &hybridParams);
    xcmd.protocol = 3;
    // xcmd.forCursor = hreq->reqflags & QEXEC_F_IS_CURSOR;
    // xcmd.forProfiling = false;  // No profiling support for hybrid yet
    // xcmd.rootCommand = C_AGG;   // Response is equivalent to a `CURSOR READ` response

    // UPDATED: Use new start function with mappings (no dispatcher needed)
    HybridRequest_buildDistRPChain(hreq->requests[0], &xcmd, &us, rpnetNext_StartWithMappings);
    HybridRequest_buildDistRPChain(hreq->requests[1], &xcmd, &us, rpnetNext_StartWithMappings);

    // Add timeout initialization for each subquery after building RPNet chains
    for (int i = 0; i < hreq->nrequests; i++) {
        AREQ *subquery = hreq->requests[i];
        RedisSearchCtx *sctx = AREQ_SearchCtx(subquery);
        SearchCtx_UpdateTime(sctx, subquery->reqConfig.queryTimeoutMS);
    }

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

    int result = ProcessHybridCursorMappings(cmd, numShards, searchMappingsRef, vsimMappingsRef);
    if (result != RS_RESULT_OK) {
        // Handle error
        StrongRef_Release(searchMappingsRef);
        StrongRef_Release(vsimMappingsRef);
        return REDISMODULE_ERR;
    }

    // Store mappings in RPNet structures
    searchRPNet->mappings = searchMappingsRef;
    vsimRPNet->mappings = vsimMappingsRef;
    CursorMappings *searchMappings = StrongRef_Get(searchMappingsRef);
    CursorMappings *vsimMappings = StrongRef_Get(vsimMappingsRef);

    RedisModule_Log(NULL, "verbose", "searchMappings length: %d, vsimMappings length: %d", array_len(searchMappings->mappings), array_len(vsimMappings->mappings));


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
        StrongRef_Release(searchMappingsRef);
        StrongRef_Release(vsimMappingsRef);
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
