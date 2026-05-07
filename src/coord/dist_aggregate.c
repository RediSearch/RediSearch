/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <stdatomic.h>
#include "result_processor.h"
#include "rmr/rmr.h"
#include "rmutil/util.h"
#include "commands.h"
#include "aggregate/aggregate.h"
#include "dist_plan.h"
#include "module.h"
#include "profile/profile.h"
#include "util/timeout.h"
#include "resp3.h"
#include "coord/config.h"
#include "config.h"
#include "dist_profile.h"
#include "shard_window_ratio.h"
#include "util/misc.h"
#include "aggregate/aggregate_debug.h"
#include "info/info_redis/threads/current_thread.h"
#include "rpnet.h"
#include "coord/dist_utils.h"
#include "info/global_stats.h"
#include "search_disk.h"
#include "debug_commands.h"
#include "coord_request_ctx.h"
#include "aggregate/reply_empty.h"

static const RLookupKey *keyForField(RPNet *nc, const char *s) {
  RLOOKUP_FOREACH(kk, nc->lookup, {
    if (!strcmp(RLookupKey_GetName(kk), s)) {
      return kk;
    }
  });

  return NULL;
}

void processResultFormat(uint32_t *flags, MRReply *map) {
  // Logic of which format to use is done by the shards
  MRReply *format = MRReply_MapElement(map, "format");
  RS_LOG_ASSERT(format, "missing format specification");
  if (MRReply_StringEquals(format, "EXPAND", false)) {
    *flags |= QEXEC_FORMAT_EXPAND;
  } else {
    *flags &= ~QEXEC_FORMAT_EXPAND;
  }
  *flags &= ~QEXEC_FORMAT_DEFAULT;
}

static int rpnetNext_Start(ResultProcessor *rp, SearchResult *r) {
  RPNet *nc = (RPNet *)rp;

#ifdef ENABLE_ASSERT
  // Sync point (debug): park BG just before the initial timeout check.
  SyncPoint_WaitUntil(SYNC_POINT_BEFORE_RPNET_START, areq_timed_out, nc->areq);
#endif

  // Check if the request timed out before starting the iterator
  if (AREQ_TimedOut(nc->areq)) {
    return RS_RESULT_TIMEDOUT;
  }

  // Initialize shard response barrier if WITHCOUNT is enabled
  if (HasWithCount(nc->areq) && IsAggregate(nc->areq)) {
    ShardResponseBarrier *barrier = shardResponseBarrier_New();
    if (!barrier) {
      return RS_RESULT_ERROR;
    }
    nc->shardResponseBarrier = barrier;
  }

  // Pass barrier as private data to callback (only if WITHCOUNT enabled)
  // The barrier is freed by MRIterator via shardResponseBarrier_Free destructor
  // shardResponseBarrier_Init is called from iterStartCb when numShards is known from topology
  MRIterator *it = nc->shardResponseBarrier
                   ? MR_IterateWithPrivateData(&nc->cmd, netCursorCallback, nc->shardResponseBarrier,
                                               shardResponseBarrier_Free, shardResponseBarrier_Init,
                                               iterStartCb, NULL)
                   : MR_Iterate(&nc->cmd, netCursorCallback);

  if (!it) {
    // Clean up on error - iterator never started so no callbacks running
    // Must free manually since iterator didn't take ownership
    if (nc->shardResponseBarrier) {
      shardResponseBarrier_Free(nc->shardResponseBarrier);
      nc->shardResponseBarrier = NULL;
    }
    return RS_RESULT_ERROR;
  }

  nc->it = it;
  // Register the iterator's channel so the main-thread timeout callback can wake
  // this reader if it blocks in MRIterator_NextWithTimeout after AREQ timed out.
  // Paired with RequestSyncCtx_UnregisterAbortWakeChannel in rpnetFree.
  RequestSyncCtx_RegisterAbortWakeChannel(&nc->areq->syncCtx, MRIterator_GetChannel(it));
  nc->base.Next = rpnetNext;
  return rpnetNext(rp, r);
}

static void buildMRCommand(RedisModuleString **argv, int argc, ProfileOptions profileOptions,
                           AREQDIST_UpstreamInfo *us, MRCommand *xcmd, IndexSpec *sp, specialCaseCtx *knnCtx) {
  // We need to prepend the array with the command, index, and query that
  // we want to use.
  const char **tmparr = array_new(const char *, array_len(us->serialized));

  const char *index_name = RedisModule_StringPtrLen(argv[1], NULL);

  int profileArgs = 0;
  if (profileOptions == EXEC_NO_FLAGS) {
    array_append(tmparr, RS_AGGREGATE_CMD);                         // Command
    array_append(tmparr, index_name);  // Index name
  } else {
    profileArgs += 2; // SEARCH/AGGREGATE + QUERY
    array_append(tmparr, RS_PROFILE_CMD);
    array_append(tmparr, index_name);  // Index name
    array_append(tmparr, "AGGREGATE");
    if (profileOptions & EXEC_WITH_PROFILE_LIMITED) {
      array_append(tmparr, "LIMITED");
      profileArgs++;
    }
    array_append(tmparr, "QUERY");
  }

  array_append(tmparr, RedisModule_StringPtrLen(argv[2 + profileArgs], NULL));  // Query
  array_append(tmparr, "WITHCURSOR");
  // Numeric responses are encoded as simple strings.
  array_append(tmparr, "_NUM_SSTRING");

  int argOffset = 0;
  // Preserve WITHCOUNT flag from the original command
  argOffset  = RMUtil_ArgIndex("WITHCOUNT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (argOffset != -1) {
    array_append(tmparr, "WITHCOUNT");
  }

  // Add the index prefixes to the command, for validation in the shard
  array_append(tmparr, "_INDEX_PREFIXES");
  arrayof(HiddenUnicodeString*) prefixes = sp->rule->prefixes;
  char *n_prefixes;
  rm_asprintf(&n_prefixes, "%u", array_len(prefixes));
  array_append(tmparr, n_prefixes);
  for (uint32_t i = 0; i < array_len(prefixes); i++) {
    array_append(tmparr, HiddenUnicodeString_GetUnsafe(prefixes[i], NULL));
  }

  // Slots info will be added here
  uint32_t slotsInfoPos = array_len(tmparr);

  argOffset = RMUtil_ArgIndex("DIALECT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (argOffset != -1 && argOffset + 3 + 1 + profileArgs < argc) {
    array_append(tmparr, "DIALECT");
    array_append(tmparr, RedisModule_StringPtrLen(argv[argOffset + 3 + 1 + profileArgs], NULL));  // the dialect
  }

  argOffset = RMUtil_ArgIndex("FORMAT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (argOffset != -1 && argOffset + 3 + 1 + profileArgs < argc) {
    array_append(tmparr, "FORMAT");
    array_append(tmparr, RedisModule_StringPtrLen(argv[argOffset + 3 + 1 + profileArgs], NULL));  // the format
  }

  argOffset = RMUtil_ArgIndex("SCORER", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (argOffset != -1 && argOffset + 3 + 1 + profileArgs < argc) {
    array_append(tmparr, "SCORER");
    array_append(tmparr, RedisModule_StringPtrLen(argv[argOffset + 3 + 1 + profileArgs], NULL));  // the scorer
  }

  if (RMUtil_ArgIndex("ADDSCORES", argv + 3 + profileArgs, argc - 3 - profileArgs) != -1) {
    array_append(tmparr, "ADDSCORES");
  }

  if (RMUtil_ArgIndex("VERBATIM", argv + 3 + profileArgs, argc - 3 - profileArgs) != -1) {
    array_append(tmparr, "VERBATIM");
  }

  for (size_t ii = 0; ii < array_len(us->serialized); ++ii) {
    array_append(tmparr, us->serialized[ii]);
  }

  *xcmd = MR_NewCommandArgv(array_len(tmparr), tmparr);

  // Prepare command for slot info (Cluster mode)
  MRCommand_PrepareForSlotInfo(xcmd, slotsInfoPos);

  // Prepare placeholder for dispatch time (will be filled in when sending to shards)
  MRCommand_PrepareForDispatchTime(xcmd, xcmd->num);

  // PARAMS was already validated at AREQ_Compile
  int loc = RMUtil_ArgIndex("PARAMS", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (loc != -1) {
    long long nargs;
    int rc = RedisModule_StringToLongLong(argv[loc + 3 + 1 + profileArgs], &nargs);

    // append params string including PARAMS keyword and nargs
    for (int i = 0; i < nargs + 2; ++i) {
      MRCommand_AppendRstr(xcmd, argv[loc + 3 + i + profileArgs]);
    }
  }

  // Handle KNN with shard ratio optimization for both multi-shard and standalone
  if (knnCtx) {
    KNNVectorQuery *knn_query = &knnCtx->knn.queryNode->vn.vq->knn;
    double ratio = knn_query->shardWindowRatio;

    if (ratio < MAX_SHARD_WINDOW_RATIO) {
      // Apply optimization only if ratio is valid and < 1.0 (ratio = 1.0 means no optimization)
      // Calculate effective K based on deployment mode
      size_t numShards = GetNumShards_UnSafe();
      size_t effectiveK = calculateEffectiveK(knn_query->k, ratio, numShards);

      // Modify the command to replace KNN k (shards will ignore $SHARD_K_RATIO)
      modifyKNNCommand(xcmd, 2 + profileArgs, effectiveK, knnCtx->knn.queryNode->vn.vq);
    }
  }

  // check for timeout argument and append it to the command.
  // If TIMEOUT exists, it was already validated at AREQ_Compile.
  int timeout_index = RMUtil_ArgIndex("TIMEOUT", argv + 3 + profileArgs, argc - 4 - profileArgs);
  if (timeout_index != -1) {
    MRCommand_AppendRstr(xcmd, argv[timeout_index + 3 + profileArgs]);
    MRCommand_AppendRstr(xcmd, argv[timeout_index + 4 + profileArgs]);
  }

  // Check for the `BM25STD_TANH_FACTOR` argument
  int bm25std_tanh_factor_index = RMUtil_ArgIndex("BM25STD_TANH_FACTOR", argv + 3 + profileArgs, argc - 4 - profileArgs);
  if (bm25std_tanh_factor_index != -1) {
    MRCommand_AppendRstr(xcmd, argv[bm25std_tanh_factor_index + 3 + profileArgs]);
    MRCommand_AppendRstr(xcmd, argv[bm25std_tanh_factor_index + 4 + profileArgs]);
  }

  MRCommand_SetPrefix(xcmd, "_FT");

  rm_free(n_prefixes);
  array_free(tmparr);
}

// True iff draining endProc->Next after a RETURN-STRICT timeout produces a
// valid (possibly empty) partial answer.
//
// Accepted shapes (top = end of pipeline):
//   1. RPNet                                  -- bare network root.
//   2. RPPager_Limiter -> RPNet               -- pager directly above RPNet.
//   3. [RPPager_Limiter ->] RPSorter -> ...   -- end is RPSorter (optionally
//                                                under a pager); anything
//                                                between the sorter and RPNet
//                                                is allowed.
//
// Shape (3) is safe because rpsortNext_Yield (the state RPSorter enters on
// TIMEDOUT) only pops from the sorter's heap, and drain only invokes
// endProc->Next -- intermediate RPs are never re-entered after returning
// TIMEDOUT.
//
// Profile is excluded: it wraps every RP and is not yet supported under
// RETURN-STRICT drain.
static bool pipelineCanYieldPartialResults(AREQ *r) {
  if (IsProfile(r)) {
    return false;
  }

  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
  ResultProcessor *end = qctx->endProc;
  ResultProcessor *root = qctx->rootProc;

  if (!end || !root) {
    return false;
  }

  // Coordinator pipelines are always rooted at RPNet.
  RS_ASSERT(root->type == RP_NETWORK);

  // RPPager_Limiter is transparent here: peel it and look at what's beneath.
  // The pager is never the network root, so it always has an upstream.
  ResultProcessor *rp = end;
  if (rp->type == RP_PAGER_LIMITER) {
    rp = rp->upstream;
    RS_ASSERT(rp);
  }

  // Accept if what's below the (optional) pager is the RPNet root (shapes 1
  // and 2) or an RPSorter somewhere above it (shape 3 -- drain pops from the
  // sorter's heap, so what sits between RPSorter and RPNet doesn't matter).
  return rp == root || rp->type == RP_SORTER;
}

static void buildDistRPChain(AREQ *r, MRCommand *xcmd, AREQDIST_UpstreamInfo *us, int (*nextFunc)(ResultProcessor *, SearchResult *)) {
  // Establish our root processor, which is the distributed processor
  RPNet *rpRoot = RPNet_New(xcmd, nextFunc); // This will take ownership of the command
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
  rpRoot->base.parent = qctx;
  rpRoot->lookup = us->lookup;
  rpRoot->areq = r;

  ResultProcessor *rpProfile = NULL;
  if (IsProfile(r)) {
    rpProfile = RPProfile_New(&rpRoot->base, qctx);
  }

  RS_ASSERT(!AREQ_QueryProcessingCtx(r)->rootProc);
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

  qctx->canYieldPartialResults = pipelineCanYieldPartialResults(r);
}

void PrintShardProfile(RedisModule_Reply *reply, void *ctx);

void printAggProfile(RedisModule_Reply *reply, void *ctx) {
  // profileRP replace netRP as end PR
  AREQ *req = ctx;
  RPNet *rpnet = (RPNet *)AREQ_QueryProcessingCtx(req)->rootProc;
  // Calling getNextReply alone is insufficient here, as we might have already encountered EOF from the shards,
  // which caused the call to getNextReply from RPNet to set cond->wait to true.
  // We can't also set cond->wait to false because we might still be waiting for shards' replies containing profile information.

  // Therefore, we loop to drain all remaining replies from the channel.
  // Pending might be zero, but there might still be replies in the channel to read.
  // We may have pulled all the replies from the channel and arrived here due to a timeout,
  // and now we're waiting for the profile results.
  if (MRIterator_GetPending(rpnet->it) || MRIterator_GetChannelSize(rpnet->it)) {
    do {
      MRReply_Free(rpnet->current.root);
    } while (getNextReply(rpnet) != RS_RESULT_EOF);
  }

  size_t num_shards = MRIterator_GetNumShards(rpnet->it);
  size_t profile_count = array_len(rpnet->shardsProfile);

  PrintShardProfile_ctx sCtx = {
    .count = profile_count,
    .replies = rpnet->shardsProfile,
    .isSearch = false,
  };

  if (profile_count != num_shards) {
    RedisModule_Log(RSDummyContext, "warning", "Profile data received from %zu out of %zu shards",
                    profile_count, num_shards);
  }

  Profile_PrintInFormat(reply, PrintShardProfile, &sCtx, Profile_Print, req);
}

int parseProfileArgs(RedisModuleString **argv, int argc, AREQ *r) {
  // Profile args
  int profileArgs = 0;
  if (RMUtil_ArgIndex("FT.PROFILE", argv, 1) != -1) {
    profileArgs += 2;     // SEARCH/AGGREGATE + QUERY
    AREQ_AddRequestFlags(r, QEXEC_F_PROFILE);
    if (RMUtil_ArgIndex("LIMITED", argv + 3, 1) != -1) {
      profileArgs++;
      AREQ_AddRequestFlags(r, QEXEC_F_PROFILE_LIMITED);
    }
    if (RMUtil_ArgIndex("QUERY", argv + 3, 2) == -1) {
      QueryError_SetError(AREQ_QueryProcessingCtx(r)->err, QUERY_ERROR_CODE_PARSE_ARGS, "The QUERY keyword is expected");
      return -1;
    }
  }
  return profileArgs;
}

static bool shouldCheckInPipelineTimeoutCoord(AREQ *req) {
  // We should check for timeout in pipeline if policy is return and timeout > 0
  return req->reqConfig.queryTimeoutMS > 0 &&
         (req->reqConfig.timeoutPolicy == TimeoutPolicy_Return);
}

static int prepareForExecution(AREQ *r, RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         IndexSpec *sp, specialCaseCtx **knnCtx_ptr, QueryError *status) {
  AREQ_QueryProcessingCtx(r)->err = status;
  AREQ_AddRequestFlags(r, QEXEC_F_IS_AGGREGATE | QEXEC_F_BUILDPIPELINE_NO_ROOT);
  rs_wall_clock_init(&r->profileClocks.initClock);

  ProfileOptions profileOptions = EXEC_NO_FLAGS;
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv, argc);

  int rc = ParseProfile(&ac, status, &profileOptions);
  if (rc == REDISMODULE_ERR) return REDISMODULE_ERR;
  ApplyProfileOptions(AREQ_QueryProcessingCtx(r), &r->reqflags, profileOptions);

  // For non-profile commands, skip past command name (FT.AGGREGATE) and index name
  if (profileOptions == EXEC_NO_FLAGS) {
    if (AC_AdvanceBy(&ac, 2) != AC_OK) {
      return REDISMODULE_ERR;
    }
  }

  rc = AREQ_Compile(r, ctx, argv + ac.offset, argc - ac.offset, SearchDisk_IsEnabledForValidation(), status);
  if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

  r->profile = printAggProfile;

  unsigned int dialect = r->reqConfig.dialectVersion;
  specialCaseCtx *knnCtx = NULL;

  if(dialect >= 2) {
    // Check if we have KNN in the query string, and if so, parse the query string to see if it is
    // a KNN section in the query. IN that case, we treat this as a SORTBY+LIMIT step.
    if(strcasestr(r->query, "KNN")) {
      // For distributed aggregation, command type detection is automatic
      knnCtx = prepareOptionalTopKCase(r->query, argv, argc, dialect, status);
      *knnCtx_ptr = knnCtx;
      if (QueryError_HasError(status)) {
        return REDISMODULE_ERR;
      }
      if (knnCtx != NULL) {
        // If we found KNN, add an arange step, so it will be the first step after
        // the root (which is first plan step to be executed after the root).
        AGPLN_AddKNNArrangeStep(AREQ_AGGPlan(r), knnCtx->knn.k, knnCtx->knn.fieldName);
      }
    }
  }

  rc = AGGPLN_Distribute(AREQ_AGGPlan(r), status);
  if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

  AREQDIST_UpstreamInfo us = {NULL};
  rc = AREQ_BuildDistributedPipeline(r, &us, status);
  if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

  // Construct the command string
  MRCommand xcmd;
  buildMRCommand(argv , argc, profileOptions, &us, &xcmd, sp, knnCtx);
  xcmd.protocol = is_resp3(ctx) ? 3 : 2;
  xcmd.forCursor = AREQ_RequestFlags(r) & QEXEC_F_IS_CURSOR;
  xcmd.forProfiling = IsProfile(r);
  xcmd.rootCommand = C_AGG;  // Response is equivalent to a `CURSOR READ` response
  xcmd.coordStartTime = r->profileClocks.coordStartTime;

  // Build the result processor chain
  buildDistRPChain(r, &xcmd, &us, rpnetNext_Start);

  if (IsProfile(r)) r->profileClocks.profileParseTime = rs_wall_clock_elapsed_ns(&r->profileClocks.initClock);

  // Create the Search context
  // (notice with cursor, we rely on the existing mechanism of AREQ to free the ctx object when the cursor is exhausted)
  r->sctx = rm_new(RedisSearchCtx);
  *r->sctx = SEARCH_CTX_STATIC(ctx, NULL);
  r->sctx->apiVersion = dialect;
  SearchCtx_UpdateTime(r->sctx, r->reqConfig.queryTimeoutMS);
  // Propagate skipTimeoutChecks from request to sctx.
  // AREQ_Compile set req->skipTimeoutChecks before sctx existed, so the flag
  // was not propagated. RPNet and startPipeline read from sctx->time.skipTimeoutChecks.
  r->sctx->time.skipTimeoutChecks = r->skipTimeoutChecks;
  // r->sctx->expanded should be received from shards

  AREQ_SetSkipTimeoutChecks(r, !shouldCheckInPipelineTimeoutCoord(r));

  return REDISMODULE_OK;
}

static int executePlan(AREQ *r, struct ConcurrentCmdCtx *cmdCtx, RedisModule_Reply *reply, QueryError *status) {
  if (AREQ_RequestFlags(r) & QEXEC_F_IS_CURSOR) {
    // Keep the original concurrent context
    ConcurrentCmdCtx_KeepRedisCtx(cmdCtx);

    StrongRef dummy_spec_ref = {.rm = NULL};

    if (AREQ_StartCursor(r, reply, dummy_spec_ref, status, true) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  } else {
    sendChunk(r, reply, UINT64_MAX);
    AREQ_DecrRef(r);
  }
  return REDISMODULE_OK;
}

static void DistAggregateCleanups(RedisModuleCtx *ctx, struct ConcurrentCmdCtx *cmdCtx, IndexSpec *sp,
                          StrongRef *strong_ref, specialCaseCtx *knnCtx, AREQ *r, RedisModule_Reply *reply, QueryError *status) {

  CoordRequestCtx *reqCtx = RedisModule_BlockClientGetPrivateData(ConcurrentCmdCtx_GetBlockedClient(cmdCtx));

  // If timeout already occurred, the timeout callback already replied - don't reply again
  if (CoordRequestCtx_TimedOut(reqCtx)) {
    if (QueryError_HasError(status)) {
      QueryError_ClearError(status);
    }
    goto cleanup;
  }

  RS_ASSERT(QueryError_HasError(status));

  if (!r) {
    // Currently only possible in _FT.DEBUG path
    CoordRequestCtx_ReplyOrStoreError(reqCtx, ctx, status);
  } else {
    AREQ_ReplyOrStoreError(r, ctx, status);
  }

cleanup:
  WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  if (sp) {
    IndexSpecRef_Release(*strong_ref);
  }
  SpecialCaseCtx_Free(knnCtx);
  if (r) AREQ_DecrRef(r);
  RedisModule_EndReply(reply);
  return;
}

void RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx) {

  CoordRequestCtx *reqCtx = RedisModule_BlockClientGetPrivateData(ConcurrentCmdCtx_GetBlockedClient(cmdCtx));
  if(CoordRequestCtx_TimedOut(reqCtx)) {
    // Query timed out before request creation
    WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    return;
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  // Lock before creating request to prevent race with timeout callback
  CoordRequestCtx_LockSetRequest(reqCtx);

  // Check if already timed out
  if (CoordRequestCtx_TimedOut(reqCtx)) {
    // Timeout callback will handle reply - just unlock and cleanup
    CoordRequestCtx_UnlockSetRequest(reqCtx);
    WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    RedisModule_EndReply(reply);
    return;
  }

  // CMD, index, expr, args...
  AREQ *r = AREQ_New();

  if (r->reqConfig.timeoutPolicy == TimeoutPolicy_ReturnStrict) {
    r->syncCtx.requiresAggregateResultsSync = true;
  }
  CoordRequestCtx_SetRequest(reqCtx, r);
  CoordRequestCtx_UnlockSetRequest(reqCtx);

  QueryError status = QueryError_Default();
  specialCaseCtx *knnCtx = NULL;

  // Store coordinator start time for dispatch time tracking
  r->profileClocks.coordStartTime = ConcurrentCmdCtx_GetCoordStartTime(cmdCtx);

  // Check if the index still exists, and promote the ref accordingly
  StrongRef strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  IndexSpec *sp = StrongRef_Get(strong_ref);
  if (!sp) {
    QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
    goto err;
  }

  if (prepareForExecution(r, ctx, argv, argc, sp, &knnCtx, &status) != REDISMODULE_OK) {
    goto err;
  }

  if (executePlan(r, cmdCtx, reply, &status) != REDISMODULE_OK) {
    goto err;
  }

  SpecialCaseCtx_Free(knnCtx);
  WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  IndexSpecRef_Release(strong_ref);
  RedisModule_EndReply(reply);
  return;

// See if we can distribute the plan...
err:
  DistAggregateCleanups(ctx, cmdCtx, sp, &strong_ref, knnCtx, r, reply, &status);
  return;
}

// Timeout callback for Coordinator AREQ execution
// Called on the main thread when the blocking client times out (FAIL policy only).
int DistAggregateTimeoutFailClient(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);

  CoordRequestCtx *CoordReqCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!CoordReqCtx) {
    // This shouldn't happen but handle gracefully
    return RedisModule_ReplyWithError(ctx, "Internal error: timeout with no context");
  }

  RS_ASSERT(CoordReqCtx->type == COMMAND_AGGREGATE);

  // Lock to coordinate with request creation in background thread
  CoordRequestCtx_LockSetRequest(CoordReqCtx);

  // Signal timeout to the background thread
  CoordRequestCtx_SetTimedOut(CoordReqCtx);

  CoordRequestCtx_UnlockSetRequest(CoordReqCtx);

  // Reply with timeout error
  QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
  RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));

  return REDISMODULE_OK;
}

// Drain any queued partial results into `storedReplyState.results` on the main
// thread after the background pipeline has aborted. Only safe for pipelines
// classified as yielding partial results (see pipelineCanYieldPartialResults):
// endProc->Next either pulls from RPNet in drainOnly mode (shapes 1-2) or pops
// from the sorter's heap (shape 3).
//
// Caller must have already flipped syncCtx.timedOut and waited for BG to exit
// the pipeline via AREQ_WaitForAggregateResultsComplete. The pager's internal
// `remaining` and qctx->resultLimit reflect the post-abort budget, so this
// loop naturally respects the user's LIMIT and terminates at EOF.
static void drainPartialResultsAfterTimeout(AREQ *req) {
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
  if (!qctx->canYieldPartialResults) {
    return;
  }

  RS_ASSERT(qctx->rootProc->type == RP_NETWORK);
  RPNet *rpnet = (RPNet *)qctx->rootProc;
  rpnet->drainOnly = true;

  ResultProcessor *endProc = qctx->endProc;
  ChunkReplyState *stored = &req->storedReplyState;
  if (!stored->results) {
    stored->results = array_new(SearchResult *, 8);
  }

  SearchResult r = SearchResult_New();
  while (qctx->resultLimit && endProc->Next(endProc, &r) == RS_RESULT_OK) {
    qctx->resultLimit--;
    array_append(stored->results, SearchResult_AllocateMove(&r));
    r = SearchResult_New();
  }
  SearchResult_Destroy(&r);
}

// Timeout callback for Coordinator AREQ execution
// Called on the main thread when the blocking client times out (RETURN-STRICT policy only).
int DistAggregateTimeoutReturnStrictClient(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  CoordRequestCtx *CoordReqCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!CoordReqCtx) {
    // This shouldn't happen but handle gracefully
    return RedisModule_ReplyWithError(ctx, "Internal error: timeout with no context");
  }

  RS_ASSERT(CoordReqCtx->type == COMMAND_AGGREGATE);

  // Lock to coordinate with request creation in background thread
  CoordRequestCtx_LockSetRequest(CoordReqCtx);

  // Signal timeout to the background thread
  CoordRequestCtx_SetTimedOut(CoordReqCtx);

  CoordRequestCtx_UnlockSetRequest(CoordReqCtx);

  AREQ *req = (AREQ *)CoordRequestCtx_GetRequest(CoordReqCtx);

  if (!req || AREQ_TryClaimAggregateResults(req)) {
    // Either the request is NULL or We were able to claim the aggregation results.
    // That means that the background thread didn't reach the aggregation phase (startPipelineCommon) yet.
    // Reply with empty results
    coord_aggregate_query_reply_empty(ctx, argv, argc, QUERY_ERROR_CODE_TIMED_OUT);
    return REDISMODULE_OK;
  }

  // Losing TryClaim means BG owns the claim, it may be blocked in MRIterator_NextWithTimeout.
  // Wake it so it observes the Timeout and exits the pipeline promptly.
  RequestSyncCtx_WakeAbortChannel(&req->syncCtx);

  // Sync with the background thread
  AREQ_WaitForAggregateResultsComplete(req);

  // BG signals only after AREQ_StoreResults
  RS_ASSERT(req->storedReplyState.hasStoredResults);

  // Harvest any shard replies that landed in the channel before the deadline.
  // No-op for already-complete runs.
  drainPartialResultsAfterTimeout(req);

  // Rejected pipelines discard their buffer on TIMEDOUT, but RPNet may have
  // already accumulated `total_results` from admitted shard replies. Zero it
  // for consistency with the empty results.
  ChunkReplyState *stored = &req->storedReplyState;
  if (!AREQ_QueryProcessingCtx(req)->canYieldPartialResults &&
      array_len(stored->results) == 0) {
    AREQ_QueryProcessingCtx(req)->totalResults = 0;
  }

  AREQ_ReplyWithStoredResults(ctx, req);

  return REDISMODULE_OK;
}

// Main-thread reply callback for coord AREQ (FAIL / RETURN-STRICT). Reads results
// stored by the BG thread in req->storedReplyState. NOT called if timeout fired
int DistAggregateReplyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);

  CoordRequestCtx *CoordReqCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!CoordReqCtx) {
    RedisModule_Log(ctx, "warning", "DistAggregateReplyCallback: no context");
    return RedisModule_ReplyWithError(ctx, "ERR Internal error: no request context");
  }

  RS_ASSERT(CoordReqCtx->type == COMMAND_AGGREGATE);

  AREQ *req = (AREQ *)CoordRequestCtx_GetRequest(CoordReqCtx);
  if (!req) {
    // We expect CoordReqCtx to hold the error if req is NULL
    if (QueryError_HasError(&CoordReqCtx->preRequestError)) {
      QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(&CoordReqCtx->preRequestError), 1, COORD_ERR_WARN);
      QueryError_ReplyAndClear(ctx, &CoordReqCtx->preRequestError);
      return REDISMODULE_OK;
    }
    // This should not happen, but handle gracefully
    RedisModule_Log(ctx, "warning", "DistAggregateReplyCallback: no AREQ and no preRequestError");
    return RedisModule_ReplyWithError(ctx, "Internal error: no AREQ and no preRequestError");
  }

  // Check if results were stored (background thread completed successfully)
  if (!req->storedReplyState.hasStoredResults) {
    // Background thread didn't store results - some early error occurred.
    if (QueryError_HasError(&req->storedReplyState.err)) {
      QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(&req->storedReplyState.err), 1, COORD_ERR_WARN);
      QueryError_ReplyAndClear(ctx, &req->storedReplyState.err);
    } else {
      RedisModule_ReplyWithError(ctx, "Internal error: no results stored");
    }
    return REDISMODULE_OK;
  }

  AREQ_ReplyWithStoredResults(ctx, req);

  // Note: No AREQ_DecrRef here - CoordRequestCtx_Free releases the context's reference.
  return REDISMODULE_OK;
}


/* ======================= DEBUG ONLY ======================= */
void DEBUG_RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx) {

  CoordRequestCtx *reqCtx = RedisModule_BlockClientGetPrivateData(ConcurrentCmdCtx_GetBlockedClient(cmdCtx));
  if(CoordRequestCtx_TimedOut(reqCtx)) {
    // Query timed out before request creation
    WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    return;
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  AREQ *r = NULL;
  IndexSpec *sp = NULL;
  specialCaseCtx *knnCtx = NULL;
  AREQ_Debug_params debug_params = {0};
  StrongRef strong_ref = {0};
  int debug_argv_count = 0;
  MRCommand *cmd = NULL;

  // debug_req and &debug_req->r are allocated in the same memory block, so it will be freed
  // when AREQ_Free is called
  QueryError status = QueryError_Default();

  // Lock before creating request to prevent race with timeout callback
  CoordRequestCtx_LockSetRequest(reqCtx);

  // Check if already timed out
  if (CoordRequestCtx_TimedOut(reqCtx)) {
    // Timeout callback will handle reply - just unlock and cleanup
    CoordRequestCtx_UnlockSetRequest(reqCtx);
    WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
    RedisModule_EndReply(reply);
    return;
  }

  AREQ_Debug *debug_req = AREQ_Debug_New(argv, argc, &status);
  if (!debug_req) {
    CoordRequestCtx_UnlockSetRequest(reqCtx);
    goto err;
  }
  // CMD, index, expr, args...
  r = &debug_req->r;

  if (r->reqConfig.timeoutPolicy == TimeoutPolicy_ReturnStrict) {
    r->syncCtx.requiresAggregateResultsSync = true;
  }
  CoordRequestCtx_SetRequest(reqCtx, r);
  CoordRequestCtx_UnlockSetRequest(reqCtx);

  // Store coordinator start time for dispatch time tracking
  r->profileClocks.coordStartTime = ConcurrentCmdCtx_GetCoordStartTime(cmdCtx);
  debug_params = debug_req->debug_params;
  // Check if the index still exists, and promote the ref accordingly
  strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  sp = StrongRef_Get(strong_ref);
  if (!sp) {
    QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
    goto err;
  }

  debug_argv_count = debug_params.debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>` strings
  if (prepareForExecution(r, ctx, argv, argc - debug_argv_count, sp, &knnCtx, &status) != REDISMODULE_OK) {
    goto err;
  }

  // rpnet now owns the command
  cmd = &(((RPNet *)AREQ_QueryProcessingCtx(r)->rootProc)->cmd);

  MRCommand_Insert(cmd, 0, "_FT.DEBUG", sizeof("_FT.DEBUG") - 1);
  // insert also debug params at the end
  for (size_t i = 0; i < debug_argv_count; i++) {
    size_t n;
    const char *arg = RedisModule_StringPtrLen(debug_params.debug_argv[i], &n);
    MRCommand_Append(cmd, arg, n);
  }

  if (parseAndCompileDebug(debug_req, &status) != REDISMODULE_OK) {
    goto err;
  }

  if (executePlan(r, cmdCtx, reply, &status) != REDISMODULE_OK) {
    goto err;
  }

  SpecialCaseCtx_Free(knnCtx);
  WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  IndexSpecRef_Release(strong_ref);
  RedisModule_EndReply(reply);
  return;

// See if we can distribute the plan...
err:
  DistAggregateCleanups(ctx, cmdCtx, sp, &strong_ref, knnCtx, r, reply, &status);
  return;
}
