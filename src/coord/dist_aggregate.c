/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "result_processor.h"
#include "rmr/rmr.h"
#include "rmutil/util.h"
#include "commands.h"
#include "aggregate/aggregate.h"
#include "dist_plan.h"
#include "module.h"
#include "profile.h"
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

#define CURSOR_EOF 0

// Get cursor command using a cursor id and an existing aggregate command
// Returns true if the cursor is not done (i.e., not depleted)
static bool getCursorCommand(long long cursorId, MRCommand *cmd, MRIteratorCtx *ctx) {
  if (cursorId == CURSOR_EOF) {
    // Cursor was set to 0, end of reply chain. cmd->depleted will be set in `MRIteratorCallback_Done`.
    return false;
  }

  RS_LOG_ASSERT(cmd->num >= 2, "Invalid command?!");

  // Check if the coordinator experienced a timeout or not
  bool timedout = MRIteratorCallback_GetTimedOut(ctx);

  if (cmd->rootCommand == C_AGG) {
    MRCommand newCmd;
    char buf[24]; // enough digits for a long long
    sprintf(buf, "%lld", cursorId);
    // AGGREGATE commands has the index name at position 1
    const char *idx = MRCommand_ArgStringPtrLen(cmd, 1, NULL);
    // If we timed out and not in cursor mode, we want to send the shard a DEL
    // command instead of a READ command (here we know it has more results)
    if (timedout && !cmd->forCursor) {
      newCmd = MR_NewCommand(4, "_FT.CURSOR", "DEL", idx, buf);
      // Mark that the last command was a DEL command
      newCmd.rootCommand = C_DEL;
    } else {
      newCmd = MR_NewCommand(4, "_FT.CURSOR", "READ", idx, buf);
      newCmd.rootCommand = C_READ;
    }

    newCmd.targetSlot = cmd->targetSlot;
    newCmd.protocol = cmd->protocol;
    newCmd.forCursor = cmd->forCursor;
    newCmd.forProfiling = cmd->forProfiling;
    MRCommand_Free(cmd);
    *cmd = newCmd;

  } else {
    // The previous command was a _FT.CURSOR READ command, so we may not need to change anything.
    RS_LOG_ASSERT(cmd->rootCommand == C_READ, "calling `getCursorCommand` after a DEL command");
    RS_ASSERT(cmd->num == 4);
    RS_ASSERT(STR_EQ(cmd->strs[0], cmd->lens[0], "_FT.CURSOR"));
    RS_ASSERT(STR_EQ(cmd->strs[1], cmd->lens[1], "READ"));
    RS_ASSERT(atoll(cmd->strs[3]) == cursorId);

    // If we timed out and not in cursor mode, we want to send the shard a DEL
    // command instead of a READ command (here we know it has more results)
    if (timedout && !cmd->forCursor) {
      MRCommand_ReplaceArg(cmd, 1, "DEL", 3);
      cmd->rootCommand = C_DEL;
    }
  }

  if (timedout && cmd->forCursor) {
    // Reset the `timedOut` value in case it was set (for next iterations, as
    // we're in cursor mode)
    MRIteratorCallback_ResetTimedOut(ctx);
  }

  return true;
}


static void netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  MRCommand *cmd = MRIteratorCallback_GetCommand(ctx);

  // If the root command of this reply is a DEL command, we don't want to
  // propagate it up the chain to the client
  if (cmd->rootCommand == C_DEL) {
    // Discard the response, and return REDIS_OK
    MRIteratorCallback_Done(ctx, MRReply_Type(rep) == MR_REPLY_ERROR);
    MRReply_Free(rep);
    return;
  }

  // Check if an error returned from the shard
  if (MRReply_Type(rep) == MR_REPLY_ERROR) {
    const char* error = MRReply_String(rep, NULL);
    RedisModule_Log(RSDummyContext, "notice", "Coordinator got an error '%.*s' from a shard", GetRedisErrorCodeLength(error), error);
    RedisModule_Log(RSDummyContext, "verbose", "Shard error: %s", error);
    MRIteratorCallback_AddReply(ctx, rep); // to be picked up by getNextReply
    MRIteratorCallback_Done(ctx, 1);
    return;
  }

  // Normal reply from the shard.
  // In any case, the cursor id is the second element in the reply
  RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 1)) == MR_REPLY_INTEGER);
  long long cursorId = MRReply_Integer(MRReply_ArrayElement(rep, 1));

  // Assert that the reply is in the expected format.
#ifdef ENABLE_ASSERT
  if (cmd->protocol == 3) {
    // RESP3 reply structure:
    // [map, cursor] - map contains the results, cursor is the next cursor id
    RS_ASSERT(MRReply_Type(rep) == MR_REPLY_ARRAY);
    RS_ASSERT(MRReply_Length(rep) == 2);
    RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 0)) == MR_REPLY_MAP);
    RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 1)) == MR_REPLY_INTEGER);
    MRReply *map = MRReply_ArrayElement(rep, 0);
    MRReply *Results = MRReply_MapElement(map, "Results");

    if (cmd->forProfiling) {
      // If the command is for profiling, the map at index 0 contains 2 elements:
      // 1. "results" - the results of the command
      // 2. "Profile" - the profile reply, if this is the last reply from this shard
      // If this is the last reply from this shard, the profile reply should set, otherwise it should be NULL
      RS_ASSERT(Results != NULL); // Query reply, nested
      RS_ASSERT(MRReply_Type(Results) == MR_REPLY_MAP);
      RS_ASSERT(MRReply_MapElement(Results, "results") != NULL); // Actual reply results
      if (cursorId == CURSOR_EOF) {
        RS_ASSERT(MRReply_Length(map) == 4); // 2 elements in the map, key and value
        RS_ASSERT(MRReply_MapElement(map, "Profile") != NULL);
        RS_ASSERT(MRReply_Type(MRReply_MapElement(map, "Profile")) == MR_REPLY_MAP);
      } else {
        RS_ASSERT(MRReply_Length(map) == 2); // 1 element in the map, key and value
        RS_ASSERT(MRReply_MapElement(map, "Profile") == NULL); // No profile reply, as this is not the last reply from this shard
      }
    } else {
      // If the command is not for profiling, the map at index 0 is the query reply
      // and contains the results of the command, and additional metadata.
      RS_ASSERT(Results != NULL);
    }
  } else {
    // RESP2 reply structure:
    // [results, cursor] or [results, cursor, profile]
    // results is an array of results, cursor is the next cursor id, and profile is
    // an optional profile reply (if the command was for profiling).
    if (cmd->forProfiling) {
      // If the command is for profiling, the reply should contain 3 elements:
      // [results, cursor, profile]
      RS_ASSERT(MRReply_Length(rep) == 3);
      RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 0)) == MR_REPLY_ARRAY);
      RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 1)) == MR_REPLY_INTEGER);
      // If this is the last reply from this shard, the profile reply should be set, otherwise it should be NULL
      if (cursorId == CURSOR_EOF) {
        RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 2)) == MR_REPLY_ARRAY);
      } else {
        RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 2)) == MR_REPLY_NIL);
      }
    } else {
      // If the command is not for profiling, the reply should contain 2 elements:
      // [results, cursor]
      RS_ASSERT(MRReply_Length(rep) == 2);
      RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 0)) == MR_REPLY_ARRAY);
      RS_ASSERT(MRReply_Type(MRReply_ArrayElement(rep, 1)) == MR_REPLY_INTEGER);
    }
  }
#endif // Reply structure assertions

  // Push the reply down the chain, to be picked up by getNextReply
  MRIteratorCallback_AddReply(ctx, rep); // take ownership of the reply

  // rewrite and resend the cursor command if needed
  // should only be determined based on the cursor and not on the set of results we get
  if (!getCursorCommand(cursorId, cmd, MRIteratorCallback_GetCtx(ctx))) {
    MRIteratorCallback_Done(ctx, 0);
  } else if (cmd->forCursor) {
    MRIteratorCallback_ProcessDone(ctx);
  } else if (MRIteratorCallback_ResendCommand(ctx) == REDIS_ERR) {
    MRIteratorCallback_Done(ctx, 1);
  }
}


static const RLookupKey *keyForField(RPNet *nc, const char *s) {
  for (const RLookupKey *kk = nc->lookup->head; kk; kk = kk->next) {
    if (!strcmp(kk->name, s)) {
      return kk;
    }
  }
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
  MRIterator *it = MR_Iterate(&nc->cmd, netCursorCallback);
  if (!it) {
    return RS_RESULT_ERROR;
  }

  nc->it = it;
  nc->base.Next = rpnetNext;
  return rpnetNext(rp, r);
}

static void buildMRCommand(RedisModuleString **argv, int argc, int profileArgs,
                           AREQDIST_UpstreamInfo *us, MRCommand *xcmd, IndexSpec *sp, specialCaseCtx *knnCtx) {
  // We need to prepend the array with the command, index, and query that
  // we want to use.
  const char **tmparr = array_new(const char *, us->nserialized);

  const char *index_name = RedisModule_StringPtrLen(argv[1], NULL);

  if (profileArgs == 0) {
    array_append(tmparr, RS_AGGREGATE_CMD);                         // Command
    array_append(tmparr, index_name);  // Index name
  } else {
    array_append(tmparr, RS_PROFILE_CMD);
    array_append(tmparr, index_name);  // Index name
    array_append(tmparr, "AGGREGATE");
    if (profileArgs == 3) {
      array_append(tmparr, "LIMITED");
    }
    array_append(tmparr, "QUERY");
  }

  array_append(tmparr, RedisModule_StringPtrLen(argv[2 + profileArgs], NULL));  // Query
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

  int argOffset = RMUtil_ArgIndex("DIALECT", argv + 3 + profileArgs, argc - 3 - profileArgs);
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

  for (size_t ii = 0; ii < us->nserialized; ++ii) {
    array_append(tmparr, us->serialized[ii]);
  }

  *xcmd = MR_NewCommandArgv(array_len(tmparr), tmparr);

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
}

void PrintShardProfile(RedisModule_Reply *reply, void *ctx);

void printAggProfile(RedisModule_Reply *reply, void *ctx) {
  // profileRP replace netRP as end PR
  ProfilePrinterCtx *cCtx = ctx;
  RPNet *rpnet = (RPNet *)AREQ_QueryProcessingCtx(cCtx->req)->rootProc;
  PrintShardProfile_ctx sCtx = {
    .count = array_len(rpnet->shardsProfile),
    .replies = rpnet->shardsProfile,
    .isSearch = false,
  };
  Profile_PrintInFormat(reply, PrintShardProfile, &sCtx, Profile_Print, cCtx);
}

static int parseProfile(RedisModuleString **argv, int argc, AREQ *r) {
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
      QueryError_SetError(AREQ_QueryProcessingCtx(r)->err, QUERY_EPARSEARGS, "No QUERY keyword provided");
      return -1;
    }
  }
  return profileArgs;
}

static int prepareForExecution(AREQ *r, RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         IndexSpec *sp, specialCaseCtx **knnCtx_ptr, QueryError *status) {
  AREQ_QueryProcessingCtx(r)->err = status;
  AREQ_AddRequestFlags(r, QEXEC_F_IS_AGGREGATE | QEXEC_F_BUILDPIPELINE_NO_ROOT);
  rs_wall_clock_init(&r->initClock);

  int profileArgs = parseProfile(argv, argc, r);
  if (profileArgs == -1) return REDISMODULE_ERR;
  int rc = AREQ_Compile(r, argv + 2 + profileArgs, argc - 2 - profileArgs, status);
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
  buildMRCommand(argv , argc, profileArgs, &us, &xcmd, sp, knnCtx);
  xcmd.protocol = is_resp3(ctx) ? 3 : 2;
  xcmd.forCursor = AREQ_RequestFlags(r) & QEXEC_F_IS_CURSOR;
  xcmd.forProfiling = IsProfile(r);
  xcmd.rootCommand = C_AGG;  // Response is equivalent to a `CURSOR READ` response

  // Build the result processor chain
  buildDistRPChain(r, &xcmd, &us, rpnetNext_Start);

  if (IsProfile(r)) r->profileParseTime = rs_wall_clock_elapsed_ns(&r->initClock);

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
  QueryError status = {0};
  specialCaseCtx *knnCtx = NULL;

  // Check if the index still exists, and promote the ref accordingly
  StrongRef strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  IndexSpec *sp = StrongRef_Get(strong_ref);
  if (!sp) {
    QueryError_SetCode(&status, QUERY_EDROPPEDBACKGROUND);
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
  QueryError status = {0};
  AREQ_Debug *debug_req = AREQ_Debug_New(argv, argc, &status);
  if (!debug_req) {
    goto err;
  }
  // CMD, index, expr, args...
  r = &debug_req->r;
  AREQ_Debug_params debug_params = debug_req->debug_params;
  // Check if the index still exists, and promote the ref accordingly
  StrongRef strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  sp = StrongRef_Get(strong_ref);
  if (!sp) {
    QueryError_SetCode(&status, QUERY_EDROPPEDBACKGROUND);
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
