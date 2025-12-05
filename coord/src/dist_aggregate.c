/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdatomic.h>
#include "result_processor.h"
#include "rmr/rmr.h"
#include "rmutil/util.h"
#include "commands.h"
#include "aggregate/aggregate.h"
#include "dist_plan.h"
#include "coord_module.h"
#include "profile.h"
#include "util/timeout.h"
#include "resp3.h"
#include "coord/src/config.h"
#include "util/misc.h"
#include "aggregate/aggregate_debug.h"
#include "util/units.h"
#include "config.h"
#include "shard_window_ratio.h"
#include "rs_wall_clock.h"
#include "rpnet.h"
#include "coord/src/dist_utils.h"

#include <err.h>


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



static void buildMRCommand(RedisModuleString **argv, int argc, int profileArgs,
                           AREQDIST_UpstreamInfo *us, MRCommand *xcmd, specialCaseCtx *knnCtx) {
  // We need to prepend the array with the command, index, and query that
  // we want to use.
  const char **tmparr = array_new(const char *, us->nserialized);

  if (profileArgs == 0) {
    array_append(tmparr, RS_AGGREGATE_CMD);                         // Command
    array_append(tmparr, RedisModule_StringPtrLen(argv[1], NULL));  // Index name
  } else {
    array_append(tmparr, RS_PROFILE_CMD);
    array_append(tmparr, RedisModule_StringPtrLen(argv[1], NULL));  // Index name
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

  int argOffset = 0;
  // Preserve WITHCOUNT flag from the original command
  argOffset  = RMUtil_ArgIndex("WITHCOUNT", argv + 3 + profileArgs, argc - 3 - profileArgs);
  if (argOffset != -1) {
    array_append(tmparr, "WITHCOUNT");
  }

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

  array_free(tmparr);
}

static void buildDistRPChain(AREQ *r, MRCommand *xcmd, AREQDIST_UpstreamInfo *us) {
  // Establish our root processor, which is the distributed processor
  RPNet *rpRoot = RPNet_New(xcmd); // This will take ownership of the command
  rpRoot->base.parent = &r->qiter;
  rpRoot->lookup = us->lookup;
  rpRoot->areq = r;

  ResultProcessor *rpProfile = NULL;
  if (IsProfile(r)) {
    rpProfile = RPProfile_New(&rpRoot->base, &r->qiter);
  }

  RS_ASSERT(!r->qiter.rootProc);
  // Get the deepest-most root:
  int found = 0;
  for (ResultProcessor *rp = r->qiter.endProc; rp; rp = rp->upstream) {
    if (!rp->upstream) {
      rp->upstream = IsProfile(r) ? rpProfile : &rpRoot->base;
      found = 1;
      break;
    }
  }

  // update root and end with RPNet
  r->qiter.rootProc = &rpRoot->base;
  if (!found) {
    r->qiter.endProc = &rpRoot->base;
  }

  // allocate memory for replies and update endProc if necessary
  if (IsProfile(r)) {
    // 2 is just a starting size, as we most likely have more than 1 shard
    rpRoot->shardsProfile = array_new(MRReply*, 2);
    if (!found) {
      r->qiter.endProc = rpProfile;
    }
  }
}

void PrintShardProfile_resp2(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch);
void PrintShardProfile_resp3(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch);

void printAggProfile(RedisModule_Reply *reply, ProfilePrinterCtx *ctx) {
  AREQ *req = ctx->req;

  RedisModule_ReplyKV_Map(reply, "Shards"); // >Shards

  // profileRP replace netRP as end PR
  RPNet *rpnet = (RPNet *)req->qiter.rootProc;
  MRReply *root = rpnet->current.root;
  // The current reply might have profile info in it
  // (For example if the pager stops the query before we deplete the current reply)
  if (root) {
    long long cursorId = MRReply_Integer(MRReply_ArrayElement(root, 1));
    if (cursorId == 0 && rpnet->shardsProfile) {
      array_ensure_append_1(rpnet->shardsProfile, root);
    } else {
      MRReply_Free(root);
    }
  }
  // Calling getNextReply alone is insufficient here, as we might have already encountered EOF from the shards,
  // which caused the call to getNextReply from RPNet to set cond->wait to true.
  // We can't also set cond->wait to false because we might still be waiting for shards' replies containing profile information.

  // Therefore, we loop to drain all remaining replies from the channel.
  // Pending might be zero, but there might still be replies in the channel to read.
  // We may have pulled all the replies from the channel and arrived here due to a timeout,
  // and now we're waiting for the profile results.
  if (MRIterator_GetPending(rpnet->it) || MRIterator_GetChannelSize(rpnet->it)) {
    while (getNextReply(rpnet) != RS_RESULT_EOF) {
      MRReply *root = rpnet->current.root;
      // skip if we get an empty result.
      // This is a bug because we discard the profile info as well
      if (root == NULL) {
        continue;
      }
      long long cursorId = MRReply_Integer(MRReply_ArrayElement(root, 1));
      if (cursorId == 0 && rpnet->shardsProfile) {
        array_ensure_append_1(rpnet->shardsProfile, root);
      } else {
        MRReply_Free(root);
      }
    }
  }

  size_t num_shards = MRIterator_GetNumShards(rpnet->it);
  size_t profile_count = array_len(rpnet->shardsProfile);

  if (profile_count != num_shards) {
    RedisModule_Log(RSDummyContext, "warning", "Profile data received from %zu out of %zu shards",
                    profile_count, num_shards);
  }

  // Print shards profile
  if (reply->resp3) {
    PrintShardProfile_resp3(reply, array_len(rpnet->shardsProfile), rpnet->shardsProfile, false);
  } else {
    PrintShardProfile_resp2(reply, array_len(rpnet->shardsProfile), rpnet->shardsProfile, false);
  }

  RedisModule_Reply_MapEnd(reply); // Shards
  // Print coordinator profile

  RedisModule_ReplyKV_Map(reply, "Coordinator"); // >coordinator

  RedisModule_ReplyKV_Map(reply, "Result processors profile");
  Profile_Print(reply, ctx);
  RedisModule_Reply_MapEnd(reply);

  RedisModule_ReplyKV_Double(reply, "Total Coordinator time", rs_wall_clock_convert_ns_to_ms_d(rs_wall_clock_elapsed_ns(&req->initClock)));

  RedisModule_Reply_MapEnd(reply); // >coordinator
}

static int parseProfile(RedisModuleString **argv, int argc, AREQ *r) {
  // Profile args
  int profileArgs = 0;
  if (RMUtil_ArgIndex("FT.PROFILE", argv, 1) != -1) {
    profileArgs += 2;     // SEARCH/AGGREGATE + QUERY
    r->reqflags |= QEXEC_F_PROFILE;
    if (RMUtil_ArgIndex("LIMITED", argv + 3, 1) != -1) {
      profileArgs++;
      r->reqflags |= QEXEC_F_PROFILE_LIMITED;
    }
    if (RMUtil_ArgIndex("QUERY", argv + 3, 2) == -1) {
      QueryError_SetError(r->qiter.err, QUERY_EPARSEARGS, "No QUERY keyword provided");
      return -1;
    }
  }
  return profileArgs;
}

static int prepareForExecution(AREQ *r, RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         specialCaseCtx **knnCtx_ptr, QueryError *status) {
  r->qiter.err = status;
  r->reqflags |= QEXEC_F_IS_AGGREGATE | QEXEC_F_BUILDPIPELINE_NO_ROOT;
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
      knnCtx = prepareOptionalTopKCase(r->query, argv, argc, status);
      *knnCtx_ptr = knnCtx;
      if (QueryError_HasError(status)) {
        return REDISMODULE_ERR;
      }
      if (knnCtx != NULL) {
        // If we found KNN, add an arange step, so it will be the first step after
        // the root (which is first plan step to be executed after the root).
        AGPLN_AddKNNArrangeStep(&r->ap, knnCtx->knn.k, knnCtx->knn.fieldName);
      }
    }
  }

  // Set the timeout
  updateTimeout(&r->timeoutTime, r->reqConfig.queryTimeoutMS);

  rc = AGGPLN_Distribute(&r->ap, status);
  if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

  AREQDIST_UpstreamInfo us = {NULL};
  rc = AREQ_BuildDistributedPipeline(r, &us, status);
  if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

  // Construct the command string
  MRCommand xcmd;
  buildMRCommand(argv , argc, profileArgs, &us, &xcmd, knnCtx);
  xcmd.protocol = is_resp3(ctx) ? 3 : 2;
  xcmd.forCursor = r->reqflags & QEXEC_F_IS_CURSOR;
  xcmd.forProfiling = IsProfile(r);
  xcmd.rootCommand = C_AGG;  // Response is equivalent to a `CURSOR READ` response

  // Build the result processor chain
  buildDistRPChain(r, &xcmd, &us);

  if (IsProfile(r)) r->profileParseTime = rs_wall_clock_elapsed_ns(&r->initClock);

  // Create the Search context
  // (notice with cursor, we rely on the existing mechanism of AREQ to free the ctx object when the cursor is exhausted)
  r->sctx = rm_new(RedisSearchCtx);
  *r->sctx = SEARCH_CTX_STATIC(ctx, NULL);
  r->sctx->apiVersion = dialect;
  r->sctx->timeout = r->timeoutTime;
  r->qiter.sctx = r->sctx;
  // r->sctx->expanded should be received from shards

  return REDISMODULE_OK;
}

static int executePlan(AREQ *r, struct ConcurrentCmdCtx *cmdCtx, RedisModule_Reply *reply, QueryError *status) {
  if (r->reqflags & QEXEC_F_IS_CURSOR) {
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

static void DistAggregateCleanups(RedisModuleCtx *ctx, specialCaseCtx *knnCtx, AREQ *r, RedisModule_Reply *reply, QueryError *status) {
  RS_ASSERT(QueryError_HasError(status));
  QueryError_ReplyAndClear(ctx, status);
  SpecialCaseCtx_Free(knnCtx);
  if (r) AREQ_Free(r);
  RedisModule_EndReply(reply);
  return;
}

void RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  bool has_map = RedisModule_HasMap(reply);

  // CMD, index, expr, args...
  AREQ *r = AREQ_New();
  QueryError status = {0};
  specialCaseCtx *knnCtx = NULL;

  if (prepareForExecution(r, ctx, argv, argc, &knnCtx, &status) != REDISMODULE_OK) {
    goto err;
  }

  if (executePlan(r, cmdCtx, reply, &status) != REDISMODULE_OK) {
    goto err;
  }

  SpecialCaseCtx_Free(knnCtx);
  RedisModule_EndReply(reply);
  return;

// See if we can distribute the plan...
err:
  DistAggregateCleanups(ctx, knnCtx, r, reply, &status);
  return;
}

/* ======================= DEBUG ONLY ======================= */
void DEBUG_RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  bool has_map = RedisModule_HasMap(reply);

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

  int debug_argv_count = debug_params.debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>` strings
  if (prepareForExecution(r, ctx, argv, argc - debug_argv_count, &knnCtx, &status) != REDISMODULE_OK) {
    goto err;
  }

  // rpnet now owns the command
  MRCommand *cmd = &(((RPNet *)r->qiter.rootProc)->cmd);

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
  RedisModule_EndReply(reply);
  return;

// See if we can distribute the plan...
err:
  DistAggregateCleanups(ctx, knnCtx, r, reply, &status);
  return;
}
