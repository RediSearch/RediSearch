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

static const RLookupKey *keyForField(RPNet *nc, const char *s) {
  RLOOKUP_FOREACH(kk, nc->lookup, {
    if (!strcmp(RLookupKey_GetName(kk), s)) {
      return kk;
    }
  });

  return NULL;
}

// Context for SHARD_K_RATIO optimization in FT.AGGREGATE
// Stores information needed to modify the KNN K value in the command
typedef struct {
  VectorQuery *vq;        // VectorQuery containing K position info (NOT owned)
  size_t queryArgIndex;   // Index of the query argument in the MRCommand
} AggregateKnnContext;

// Combined context for MR_IterateWithPrivateData in FT.AGGREGATE
// Contains optional barrier (for WITHCOUNT) and optional KNN context (for SHARD_K_RATIO)
typedef struct {
  ShardResponseBarrier *barrier;  // May be NULL if WITHCOUNT not enabled
  AggregateKnnContext *knnCtx;    // May be NULL if no KNN optimization needed
} AggregateIteratorContext;

// Free the AggregateIteratorContext and its contents
static void aggregateIteratorContext_Free(void *ptr) {
  AggregateIteratorContext *ctx = (AggregateIteratorContext *)ptr;
  if (ctx) {
    if (ctx->barrier) {
      shardResponseBarrier_Free(ctx->barrier);
    }
    rm_free(ctx->knnCtx);  // knnCtx->vq is not owned, so just free the struct
    rm_free(ctx);
  }
}

// Initialize the barrier in AggregateIteratorContext (called from iterStartCb)
static void aggregateIteratorContext_Init(void *ptr, const MRIterator *it) {
  AggregateIteratorContext *ctx = (AggregateIteratorContext *)ptr;
  if (ctx && ctx->barrier) {
    shardResponseBarrier_Init(ctx->barrier, it);
  }
}

// Command modifier callback for SHARD_K_RATIO optimization in FT.AGGREGATE
// Called from iterStartCb on IO thread before commands are sent to shards
static void aggregateKnnCommandModifier(MRCommand *cmd, size_t numShards, void *privateData) {
  if (!privateData || !cmd) {
    return;
  }
  AggregateIteratorContext *ctx = (AggregateIteratorContext *)privateData;
  AggregateKnnContext *knnCtx = ctx->knnCtx;
  if (!knnCtx || !knnCtx->vq) {
    return;
  }
  const KNNVectorQuery *knn_query = &knnCtx->vq->knn;
  // Only apply optimization for multi-shard deployments with valid ratio
  if (numShards <= 1 || knn_query->shardWindowRatio >= MAX_SHARD_WINDOW_RATIO) {
    return;
  }
  size_t effectiveK = calculateEffectiveK(knn_query->k, knn_query->shardWindowRatio, numShards);
  if (effectiveK == knn_query->k) {
    return;
  }

  // Modify the command to replace KNN k
  modifyKNNCommand(cmd, knnCtx->queryArgIndex, effectiveK, knnCtx->vq);
}

// Aggregate-specific cursor callback that extracts ShardResponseBarrier from AggregateIteratorContext
// This wraps the common netCursorCallback logic but correctly handles the wrapper context type
static void aggregateNetCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  // Extract the actual ShardResponseBarrier from the AggregateIteratorContext wrapper
  AggregateIteratorContext *iterCtx = (AggregateIteratorContext *)MRIteratorCallback_GetPrivateData(ctx);
  ShardResponseBarrier *barrier = iterCtx ? iterCtx->barrier : NULL;

  // Call the common cursor callback logic with the extracted barrier
  netCursorCallbackWithBarrier(ctx, rep, barrier);
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

  // Create the iterator context wrapper for privateData
  // This holds both optional barrier (for WITHCOUNT) and optional KNN context (for SHARD_K_RATIO)
  AggregateIteratorContext *iterCtx = rm_calloc(1, sizeof(AggregateIteratorContext));
  if (!iterCtx) {
    return RS_RESULT_ERROR;
  }

  // Initialize shard response barrier if WITHCOUNT is enabled
  if (HasWithCount(nc->areq) && IsAggregate(nc->areq)) {
    ShardResponseBarrier *barrier = shardResponseBarrier_New();
    if (!barrier) {
      rm_free(iterCtx);
      return RS_RESULT_ERROR;
    }
    iterCtx->barrier = barrier;
    nc->shardResponseBarrier = barrier;  // Keep reference for getNextReply
  }

  // Initialize KNN context if SHARD_K_RATIO optimization is needed
  if (nc->knnVectorQuery) {
    AggregateKnnContext *knnCtx = rm_calloc(1, sizeof(AggregateKnnContext));
    if (!knnCtx) {
      aggregateIteratorContext_Free(iterCtx);
      return RS_RESULT_ERROR;
    }
    knnCtx->vq = nc->knnVectorQuery;
    knnCtx->queryArgIndex = nc->knnQueryArgIndex;
    iterCtx->knnCtx = knnCtx;
  }

  // Determine if we need the command modifier callback
  MRCommandModifier cmdModifier = iterCtx->knnCtx ? &aggregateKnnCommandModifier : NULL;

  // Always use MR_IterateWithPrivateData with the wrapper context
  // The iterator takes ownership of iterCtx and will free it via aggregateIteratorContext_Free
  // Use aggregateNetCursorCallback to properly extract ShardResponseBarrier from AggregateIteratorContext
  MRIterator *it = MR_IterateWithPrivateData(&nc->cmd, aggregateNetCursorCallback, iterCtx,
                                              aggregateIteratorContext_Free,
                                              aggregateIteratorContext_Init,
                                              cmdModifier, iterStartCb, NULL);

  if (!it) {
    // Clean up on error - iterator never started so no callbacks running
    // Must free manually since iterator didn't take ownership
    nc->shardResponseBarrier = NULL;  // Will be freed by aggregateIteratorContext_Free
    aggregateIteratorContext_Free(iterCtx);
    return RS_RESULT_ERROR;
  }

  nc->it = it;
  nc->base.Next = rpnetNext;
  return rpnetNext(rp, r);
}

// Build the distributed MR command for FT.AGGREGATE
// If knnCtx is provided with valid ratio, outputs VectorQuery and query arg index for command modifier
static void buildMRCommand(RedisModuleString **argv, int argc, ProfileOptions profileOptions,
                           AREQDIST_UpstreamInfo *us, MRCommand *xcmd, IndexSpec *sp, specialCaseCtx *knnCtx,
                           VectorQuery **outKnnVq, size_t *outQueryArgIndex) {
  // Initialize output parameters
  if (outKnnVq) *outKnnVq = NULL;
  if (outQueryArgIndex) *outQueryArgIndex = 0;

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

  // KNN optimization is now handled by the command modifier callback in rpnetNext_Start
  // Store the query arg index and VectorQuery in output parameters if KNN context is present
  // The command modifier will use the actual numShards from the IO thread's topology
  if (knnCtx) {
    const KNNVectorQuery *knn_query = &knnCtx->knn.queryNode->vn.vq->knn;
    double ratio = knn_query->shardWindowRatio;
    if (ratio < MAX_SHARD_WINDOW_RATIO) {
      // Store the VectorQuery and query arg index for the command modifier
      if (outKnnVq) *outKnnVq = knnCtx->knn.queryNode->vn.vq;
      if (outQueryArgIndex) *outQueryArgIndex = 2 + profileArgs;  // Query is at index 2 + profileArgs
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

static void buildDistRPChain(AREQ *r, const MRCommand *xcmd, AREQDIST_UpstreamInfo *us, int (*nextFunc)(ResultProcessor *, SearchResult *),
                             VectorQuery *knnVq, size_t knnQueryArgIndex) {
  // Establish our root processor, which is the distributed processor
  RPNet *rpRoot = RPNet_New(xcmd, nextFunc); // This will take ownership of the command
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
  rpRoot->base.parent = qctx;
  rpRoot->lookup = us->lookup;
  rpRoot->areq = r;

  // Store KNN context for SHARD_K_RATIO optimization (used by rpnetNext_Start)
  rpRoot->knnVectorQuery = knnVq;
  rpRoot->knnQueryArgIndex = knnQueryArgIndex;

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

  rc = AREQ_Compile(r, argv + ac.offset, argc - ac.offset, status);
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
  VectorQuery *knnVq = NULL;
  size_t knnQueryArgIndex = 0;
  buildMRCommand(argv, argc, profileOptions, &us, &xcmd, sp, knnCtx, &knnVq, &knnQueryArgIndex);
  xcmd.protocol = is_resp3(ctx) ? 3 : 2;
  xcmd.forCursor = AREQ_RequestFlags(r) & QEXEC_F_IS_CURSOR;
  xcmd.forProfiling = IsProfile(r);
  xcmd.rootCommand = C_AGG;  // Response is equivalent to a `CURSOR READ` response
  xcmd.coordStartTime = r->profileClocks.coordStartTime;

  // Build the result processor chain (pass KNN context for SHARD_K_RATIO optimization)
  buildDistRPChain(r, &xcmd, &us, rpnetNext_Start, knnVq, knnQueryArgIndex);

  if (IsProfile(r)) r->profileClocks.profileParseTime = rs_wall_clock_elapsed_ns(&r->profileClocks.initClock);

  // Create the Search context
  // (notice with cursor, we rely on the existing mechanism of AREQ to free the ctx object when the cursor is exhausted)
  r->sctx = rm_new(RedisSearchCtx);
  *r->sctx = SEARCH_CTX_STATIC(ctx, NULL);
  r->sctx->apiVersion = dialect;
  SearchCtx_UpdateTime(r->sctx, r->reqConfig.queryTimeoutMS);
  // r->sctx->expanded should be received from shards

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
    AREQ_Free(r);
  }
  return REDISMODULE_OK;
}

static void DistAggregateCleanups(RedisModuleCtx *ctx, struct ConcurrentCmdCtx *cmdCtx, IndexSpec *sp,
                          StrongRef *strong_ref, specialCaseCtx *knnCtx, AREQ *r, RedisModule_Reply *reply, QueryError *status) {
  RS_ASSERT(QueryError_HasError(status));
  QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(status), 1, COORD_ERR_WARN);
  QueryError_ReplyAndClear(ctx, status);
  WeakRef_Release(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  if (sp) {
    IndexSpecRef_Release(*strong_ref);
  }
  SpecialCaseCtx_Free(knnCtx);
  if (r) AREQ_Free(r);
  RedisModule_EndReply(reply);
  return;
}

void RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  // CMD, index, expr, args...
  AREQ *r = AREQ_New();
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

/* ======================= DEBUG ONLY ======================= */
void DEBUG_RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  AREQ *r = NULL;
  IndexSpec *sp = NULL;
  specialCaseCtx *knnCtx = NULL;

  // debug_req and &debug_req->r are allocated in the same memory block, so it will be freed
  // when AREQ_Free is called
  QueryError status = QueryError_Default();
  AREQ_Debug *debug_req = AREQ_Debug_New(argv, argc, &status);
  if (!debug_req) {
    goto err;
  }
  // CMD, index, expr, args...
  r = &debug_req->r;

  // Store coordinator start time for dispatch time tracking
  r->profileClocks.coordStartTime = ConcurrentCmdCtx_GetCoordStartTime(cmdCtx);
  AREQ_Debug_params debug_params = debug_req->debug_params;
  // Check if the index still exists, and promote the ref accordingly
  StrongRef strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  sp = StrongRef_Get(strong_ref);
  if (!sp) {
    QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
    goto err;
  }

  int debug_argv_count = debug_params.debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>` strings
  if (prepareForExecution(r, ctx, argv, argc - debug_argv_count, sp, &knnCtx, &status) != REDISMODULE_OK) {
    goto err;
  }

  // rpnet now owns the command
  MRCommand *cmd = &(((RPNet *)AREQ_QueryProcessingCtx(r)->rootProc)->cmd);

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
