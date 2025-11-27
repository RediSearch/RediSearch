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
#include "info/global_stats.h"

// We mainly need the resp protocol to be three in order to easily extract the "score" key from the response
#define HYBRID_RESP_PROTOCOL_VERSION 3

/**
 * Appends all SEARCH-related arguments to MR command.
 * This includes SEARCH keyword, query, and optional SCORER and YIELD_SCORE_AS parameters
 * that come immediately after the query in sequence.
 *
 * @param argv - source command arguments array
 * @param argc - total argument count
 * @param xcmd - destination MR command to append arguments to
 * @param searchOffset - offset where SEARCH keyword appears
 */
static void HybridRequest_appendSearch(RedisModuleString **argv, int argc, MRCommand *xcmd, int searchOffset) {
  // Add SEARCH keyword and query
  MRCommand_AppendRstr(xcmd, argv[searchOffset]);     // SEARCH
  MRCommand_AppendRstr(xcmd, argv[searchOffset + 1]); // query

  // Process optional parameters sequentially right after the query
  int currentOffset = searchOffset + 2; // Start after SEARCH "query"

  // Process SCORER and YIELD_SCORE_AS in any order, but they must be sequential
  while (currentOffset < argc) {
    const char *argStr = RedisModule_StringPtrLen(argv[currentOffset], NULL);

    if (strcmp(argStr, "SCORER") == 0 && currentOffset < argc - 1) {
      // Found SCORER - append it and its argument
      MRCommand_AppendRstr(xcmd, argv[currentOffset]);     // SCORER
      MRCommand_AppendRstr(xcmd, argv[currentOffset + 1]); // scorer name
      currentOffset += 2;
    } else if (strcmp(argStr, "YIELD_SCORE_AS") == 0 && currentOffset < argc - 1) {
      // Found YIELD_SCORE_AS - append it and its argument
      MRCommand_AppendRstr(xcmd, argv[currentOffset]);     // YIELD_SCORE_AS
      MRCommand_AppendRstr(xcmd, argv[currentOffset + 1]); // score alias
      currentOffset += 2;
    } else {
      // Not a SEARCH parameter - we've reached the end of SEARCH section
      break;
    }
  }
}

/**
 * Appends all VSIM-related arguments to MR command.
 * This includes VSIM keyword, field, vector, KNN/RANGE method, and VSIM FILTER if present.
 *
 * @param argv - source command arguments array
 * @param argc - total argument count
 * @param xcmd - destination MR command to append arguments to
 * @param vsimOffset - offset where VSIM keyword appears
 */
static void HybridRequest_appendVsim(RedisModuleString **argv, int argc, MRCommand *xcmd, int vsimOffset) {
  // Add VSIM keyword and field
  MRCommand_AppendRstr(xcmd, argv[vsimOffset]);     // VSIM
  MRCommand_AppendRstr(xcmd, argv[vsimOffset + 1]); // field

  // Add vector data (handle parameter placeholders vs raw data)
  size_t paramLen;
  const char *paramStr = RedisModule_StringPtrLen(argv[vsimOffset + 2], &paramLen);
  if (paramLen > 0 && paramStr[0] == '$') {
    // It's a parameter placeholder - forward as is
    MRCommand_AppendRstr(xcmd, argv[vsimOffset + 2]);
  } else {
    // It's raw data - forward as binary
    MRCommand_Append(xcmd, paramStr, paramLen);
  }

  // Find and add KNN/RANGE method and its arguments
  long long methodNargs = 0;
  int vectorMethodOffset = -1;

  int argOffset = RMUtil_ArgIndex("KNN", argv + vsimOffset, argc - vsimOffset);
  if (argOffset == -1) {
    argOffset = RMUtil_ArgIndex("RANGE", argv + vsimOffset, argc - vsimOffset);
  }

  if (argOffset != -1) {
    vectorMethodOffset = argOffset + vsimOffset;
    if (vectorMethodOffset < argc - 2) {
      RedisModule_StringToLongLong(argv[vectorMethodOffset + 1], &methodNargs);

      // Append method name, argument count, and all method arguments
      for (int i = 0; i < methodNargs + 2; ++i) {
        MRCommand_AppendRstr(xcmd, argv[vectorMethodOffset + i]);
      }
    }
  }

  // Add VSIM FILTER if present at expected position
  // Format: VSIM <field> <vector> [KNN/RANGE <count> <args...>] FILTER <expression>
  int expectedFilterOffset = vsimOffset + 3; // VSIM + field + vector
  if (vectorMethodOffset != -1) {
    expectedFilterOffset += 2 + methodNargs; // method + count + args
  }

  int actualFilterOffset = RMUtil_ArgIndex("FILTER", argv + vsimOffset, argc - vsimOffset);
  actualFilterOffset = actualFilterOffset != -1 ? actualFilterOffset + vsimOffset : -1;
  int expectedYieldScoreOffset = expectedFilterOffset;

  if (actualFilterOffset == expectedFilterOffset && actualFilterOffset < argc - 1) {
    // This is a VSIM FILTER - append it to the command
    MRCommand_AppendRstr(xcmd, argv[actualFilterOffset]);     // FILTER keyword
    MRCommand_AppendRstr(xcmd, argv[actualFilterOffset + 1]); // filter expression
    expectedYieldScoreOffset += 2; // Update expected offset after processing FILTER
  }

  // Add YIELD_SCORE_AS if present
  // Format: VSIM <field> <vector> [KNN/RANGE <count> <args...>] [FILTER <expression>] YIELD_SCORE_AS <alias>
  int yieldScoreOffset = RMUtil_ArgIndex("YIELD_SCORE_AS", argv + vsimOffset, argc - vsimOffset);
  yieldScoreOffset = yieldScoreOffset != -1 ? yieldScoreOffset + vsimOffset : -1;

  if (yieldScoreOffset == expectedYieldScoreOffset && yieldScoreOffset < argc - 1) {
    // This is a VSIM YIELD_SCORE_AS - append it to the command
    MRCommand_AppendRstr(xcmd, argv[yieldScoreOffset]);     // YIELD_SCORE_AS keyword
    MRCommand_AppendRstr(xcmd, argv[yieldScoreOffset + 1]); // score alias
  }
}

// The function transforms FT.HYBRID index SEARCH query VSIM field vector
// into _FT.HYBRID index SEARCH query VSIM field vector WITHCURSOR
// _NUM_SSTRING _INDEX_PREFIXES ...
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                            MRCommand *xcmd, arrayof(char*) serialized,
                            IndexSpec *sp, HybridPipelineParams *hybridParams) {
  int argOffset;
  const char *index_name = RedisModule_StringPtrLen(argv[1], NULL);

  // Build _FT.HYBRID command (no profiling support)
  *xcmd = MR_NewCommand(2, "_FT.HYBRID", index_name);

  // Add all SEARCH-related arguments (SEARCH, query, optional SCORER, YIELD_SCORE_AS)
  int searchOffset = RMUtil_ArgIndex("SEARCH", argv, argc);
  HybridRequest_appendSearch(argv, argc, xcmd, searchOffset);

  // Add all VSIM-related arguments (VSIM, field, vector, methods, filter)
  int vsimOffset = RMUtil_ArgIndex("VSIM", argv, argc);
  HybridRequest_appendVsim(argv, argc, xcmd, vsimOffset);

  int combineOffset = RMUtil_ArgIndex("COMBINE", argv + vsimOffset, argc - vsimOffset);
  combineOffset = combineOffset != -1 ? combineOffset + vsimOffset : -1;
  bool hasCombine = combineOffset != -1;

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

  if (serialized) {
    for (size_t ii = 0; ii < array_len(serialized); ++ii) {
      const char *token = serialized[ii];
      MRCommand_Append(xcmd, token, strlen(token));
    }
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

  // Add DIALECT arguments if present
  argOffset = RMUtil_ArgIndex("DIALECT", argv, argc);
  if (argOffset != -1) {
    MRCommand_AppendRstr(xcmd, argv[argOffset]);
    MRCommand_AppendRstr(xcmd, argv[argOffset + 1]);
  }

  // Add WITHCURSOR
  MRCommand_Append(xcmd, "WITHCURSOR", strlen("WITHCURSOR"));

  MRCommand_Append(xcmd, "WITHSCORES", strlen("WITHSCORES"));
  // Numeric responses are encoded as simple strings.
  MRCommand_Append(xcmd, "_NUM_SSTRING", strlen("_NUM_SSTRING"));

  // Prepare command for slot info (Cluster mode)
  MRCommand_PrepareForSlotInfo(xcmd, xcmd->num);

  if (sp && sp->rule && sp->rule->prefixes && array_len(sp->rule->prefixes) > 0) {
    MRCommand_Append(xcmd, "_INDEX_PREFIXES", strlen("_INDEX_PREFIXES"));
    arrayof(HiddenUnicodeString*) prefixes = sp->rule->prefixes;
    char *n_prefixes;
    rm_asprintf(&n_prefixes, "%u", array_len(prefixes));
    MRCommand_Append(xcmd, n_prefixes, strlen(n_prefixes));
    rm_free(n_prefixes);

    for (uint32_t i = 0; i < array_len(prefixes); i++) {
      size_t len;
      const char* prefix = HiddenUnicodeString_GetUnsafe(prefixes[i], &len);
      MRCommand_Append(xcmd, prefix, len);
    }
  }
}

// UPDATED: Set RPNet types when creating them
// NOTE: Caller should clone the dispatcher_ref before calling this function
static void HybridRequest_buildDistRPChain(AREQ *r, MRCommand *xcmd,
                          RLookup *lookup,
                          int (*nextFunc)(ResultProcessor *, SearchResult *)) {
  // Establish our root processor, which is the distributed processor
  MRCommand cmd = MRCommand_Copy(xcmd);

  RPNet *rpRoot = RPNet_New(&cmd, nextFunc);

  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
  rpRoot->base.parent = qctx;
  rpRoot->lookup = lookup;
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

static void setupCoordinatorArrangeSteps(AREQ *searchRequest, AREQ *vectorRequest, HybridPipelineParams *hybridParams) {
  const size_t window = hybridParams->scoringCtx->scoringType == HYBRID_SCORING_RRF ? hybridParams->scoringCtx->rrfCtx.window : hybridParams->scoringCtx->linearCtx.window;

  // TODO: would be better to look for a vector node (recursive search on the ast) and decide according to its query type (knn/range)
  const bool isKNN = vectorRequest->ast.root->type == QN_VECTOR;
  const size_t K = isKNN ? vectorRequest->ast.root->vn.vq->knn.k : 0;

  PLN_ArrangeStep *searchArrangeStep = AGPLN_GetOrCreateArrangeStep(AREQ_AGGPlan(searchRequest));
  searchArrangeStep->limit = window;

  PLN_ArrangeStep *vectorArrangeStep = AGPLN_GetOrCreateArrangeStep(AREQ_AGGPlan(vectorRequest));
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
    if (rc != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }

    // Initialize timeout for all subqueries BEFORE building pipelines
    // but after the parsing to know the timeout values
    for (int i = 0; i < hreq->nrequests; i++) {
        AREQ *subquery = hreq->requests[i];
        SearchCtx_UpdateTime(AREQ_SearchCtx(subquery), hreq->reqConfig.queryTimeoutMS);
    }
    SearchCtx_UpdateTime(hreq->sctx, hreq->reqConfig.queryTimeoutMS);

    // // Set request flags from hybridParams
    // hreq->reqflags = hybridParams.aggregationParams.common.reqflags;

    for (size_t i = 0; i < hreq->nrequests; i++) {
        AREQ *areq = hreq->requests[i];
        if (AGGPLN_Distribute(AREQ_AGGPlan(areq), status) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }
    }
    // apply the sorting changes after the distribute phase
    setupCoordinatorArrangeSteps(hreq->requests[SEARCH_INDEX], hreq->requests[VECTOR_INDEX], &hybridParams);
    RLookup *lookups[HYBRID_REQUEST_NUM_SUBQUERIES] = {0};

    arrayof(char*) serialized = HybridRequest_BuildDistributedPipeline(hreq, &hybridParams, lookups, status);
    if (!serialized) {
      return REDISMODULE_ERR;
    }

    // Construct the command string
    MRCommand xcmd;
    HybridRequest_buildMRCommand(argv, argc, &xcmd, serialized, sp, &hybridParams);

    xcmd.protocol = HYBRID_RESP_PROTOCOL_VERSION;
    xcmd.forCursor = hreq->reqflags & QEXEC_F_IS_CURSOR;
    xcmd.forProfiling = false;  // No profiling support for hybrid yet
    xcmd.rootCommand = C_READ;

    // UPDATED: Use new start function with mappings (no dispatcher needed)
    HybridRequest_buildDistRPChain(hreq->requests[0], &xcmd, lookups[0], rpnetNext_StartWithMappings);
    HybridRequest_buildDistRPChain(hreq->requests[1], &xcmd, lookups[1], rpnetNext_StartWithMappings);

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

    CursorMappings *search = rm_calloc(1, sizeof(CursorMappings));
    CursorMappings *vsim = rm_calloc(1, sizeof(CursorMappings));
    search->type = TYPE_SEARCH;
    vsim->type = TYPE_VSIM;

    StrongRef searchMappingsRef = StrongRef_New(search, FreeCursorMappings);
    StrongRef vsimMappingsRef = StrongRef_New(vsim, FreeCursorMappings);

    // Get the command from the RPNet (it was set during prepareForExecution)
    MRCommand *cmd = &searchRPNet->cmd;
    int numShards = GetNumShards_UnSafe();

    const RSOomPolicy oomPolicy = hreq->reqConfig.oomPolicy;
    if (!ProcessHybridCursorMappings(cmd, numShards, searchMappingsRef, vsimMappingsRef, hreq->tailPipeline->qctx.err, oomPolicy)) {
        // Handle error
        StrongRef_Release(searchMappingsRef);
        StrongRef_Release(vsimMappingsRef);
        return REDISMODULE_ERR;
    }

    RS_ASSERT(array_len(search->mappings) == array_len(vsim->mappings));
    if (array_len(search->mappings) == 0) {
      // No mappings available - set next function to EOF.
      // Error handling relies on QueryError status and return codes, not on mapping availability.
      searchRPNet->base.Next = rpnetNext_EOF;
      vsimRPNet->base.Next = rpnetNext_EOF;
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

    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(status), 1, COORD_ERR_WARN);

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
        QueryError_SetWithUserDataFmt(&status, QUERY_ERROR_CODE_NO_INDEX, "No such index", " %s", indexname);
        // return QueryError_ReplyAndClear(ctx, &status);
        DistHybridCleanups(ctx, cmdCtx, NULL, NULL, NULL, reply, &status);
        return;
    }

    // Check if the index still exists, and promote the ref accordingly
    StrongRef strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    IndexSpec *sp = StrongRef_Get(strong_ref);
    if (!sp) {
        QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
        DistHybridCleanups(ctx, cmdCtx, sp, &strong_ref, NULL, reply, &status);
        return;
    }

    HybridRequest *hreq = MakeDefaultHybridRequest(sctx);

    if (HybridRequest_prepareForExecution(hreq, ctx, argv, argc, sp, &status) != REDISMODULE_OK) {
      DistHybridCleanups(ctx, cmdCtx, sp, &strong_ref, hreq, reply, &status);
      return;
    }


    if (HybridRequest_executePlan(hreq, cmdCtx, reply, &status) != REDISMODULE_OK) {
        DistHybridCleanups(ctx, cmdCtx, sp, &strong_ref, hreq, reply, &status);
        return;
    }
    WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    IndexSpecRef_Release(strong_ref);
    RedisModule_EndReply(reply);
}
