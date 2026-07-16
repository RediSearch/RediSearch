/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <stdatomic.h>
#include "search_result_ffi.h"
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
#include "search_disk_utils.h"
#include "debug_commands.h"
#include "coord_request_ctx.h"
#include "aggregate/reply_empty.h"
#include "aggregate/aggregate_exec_common.h"
#include "cursor.h"

static const RLookupKey *keyForField(RPNet *nc, const char *s) {
  RLOOKUP_FOREACH(kk, nc->lookup, {
    if (!strcmp(RLookupKey_GetName(kk), s)) {
      return kk;
    }
  });

  return NULL;
}

// Context for SHARD_K_RATIO optimization in FT.AGGREGATE.
// Stores information needed to modify the KNN K value in the command
typedef struct {
  size_t queryArgIndex;    // Index of the query argument in the MRCommand
  size_t originalK;        // K value from the parsed query
  double shardWindowRatio; // SHARD_K_RATIO
  size_t kTokenPos;        // Byte offset of K within the query string
  size_t kTokenLen;        // Length of K token in bytes
} AggregateKnnContext;

// Per-iterator context for FT.AGGREGATE, handed to MR_CreateIterator as
// privateData. Holds the running WITHCOUNT total and an optional KNN snapshot
// (for SHARD_K_RATIO).
//
// Ownership: once MR_CreateIterator returns the MRIterator owns this struct and
// frees it via aggregateIteratorContext_Free. The struct in turn owns its
// `knnCtx` allocation, a self-contained scalar snapshot (no borrowed pointers).
//
// `totalResults` is the sum of each shard's reported total_results. It is written
// only on the IO thread (in withCountReplyCb) and read on the coord thread before
// executePlan runs, so no atomics are needed.
//
// `numResponded` counts how many shards have delivered their first response
// (a C_AGG reply via withCountReplyCb, or a no-reply termination via
// withCountErrorCb). Once it reaches the iterator's shard count, the
// WITHCOUNT total is fully known. Written and read only on the IO thread.
//
// `bc`, `areq`, `spec_ref`, and `knnSpecialCtx` carry the deferred-execution
// context on the async WITHCOUNT path. They are set in dispatchAggregateDeferred
// before MR_StartIterator and consumed by executeAggregateDeferred after the
// IO thread posts the handoff. The AREQ's reader ref on the iterator keeps
// iterCtx alive until executeAggregateDeferred calls AREQ_DecrRef (via
// executePlan, or directly on the timeout / spec-dropped branch).
// executeAggregateDeferred copies the fields to locals before releasing AREQ so
// it never reads iterCtx after free.
//
// The aggregate-specific callbacks below (withCountReplyCb / withCountErrorCb)
// stay wired for the iterator's lifetime; they branch on cmd->rootCommand to
// tell a shard's first reply (C_AGG) from subsequent cursor-read replies
// (C_READ / C_DEL / C_PROFILE, rewritten by getCursorCommand). When one of the
// dispatch triggers fires (timeout, error reply on a first reply, no-reply
// termination of a first command, or last first-reply arrives), the callback
// calls MRIterator_SwapCallbacks(it, netCursorCallback, NULL) before
// posting executeAggregateDeferred. That swap is the one-shot gate: subsequent
// IO-thread events never re-enter the aggregate callbacks, so dispatch happens
// exactly once and stragglers cannot observe a partially-freed iterCtx.
//
// `spec_ref` is a WeakRef so the IndexSpec is not pinned across the async wait.
// executeAggregateDeferred re-promotes it; if the spec was dropped meanwhile
// (FT.DROPINDEX, RDB load, server reset) it replies with DROPPED_BACKGROUND
// instead.
typedef struct {
  long long totalResults;           // Sum of each shard's WITHCOUNT total_results
  size_t numResponded;              // How many shards delivered their first response
  AggregateKnnContext *knnCtx;      // May be NULL if no KNN optimization needed
  // Deferred-execution fields (always non-NULL on the WITHCOUNT path; iterator
  // is only created via dispatchAggregateDeferred when HasWithCount(areq)):
  RedisModuleBlockedClient *bc;
  AREQ *areq;
  WeakRef spec_ref;                // weak ref; re-promoted in executeAggregateDeferred
  specialCaseCtx *knnSpecialCtx;   // freed by executeAggregateDeferred
} AggregateIteratorContext;

// Free the AggregateIteratorContext. Deferred-execution fields (bc, areq,
// spec_ref, knnSpecialCtx) are owned by executeAggregateDeferred.
static void aggregateIteratorContext_Free(void *ptr) {
  AggregateIteratorContext *ctx = (AggregateIteratorContext *)ptr;
  RS_ASSERT(ctx);
  rm_free(ctx->knnCtx);
  rm_free(ctx);
}

// Command modifier callback for SHARD_K_RATIO optimization in FT.AGGREGATE
// Called from iterStartCb on IO thread before commands are sent to shards.
static void aggregateKnnCommandModifier(MRCommand *cmd, size_t numShards, void *privateData) {
  RS_ASSERT(privateData && cmd);
  AggregateIteratorContext *ctx = (AggregateIteratorContext *)privateData;
  AggregateKnnContext *knnCtx = ctx->knnCtx;
  RS_ASSERT(knnCtx);
  // Only apply optimization for multi-shard deployments with valid ratio
  if (numShards <= 1 || knnCtx->shardWindowRatio >= MAX_SHARD_WINDOW_RATIO) {
    return;
  }
  size_t effectiveK = calculateEffectiveK(knnCtx->originalK, knnCtx->shardWindowRatio, numShards);
  if (effectiveK == knnCtx->originalK) {
    return;
  }

  // Modify the command to replace KNN k
  modifyKNNCommand(cmd, knnCtx->queryArgIndex, knnCtx->originalK, effectiveK,
                   knnCtx->kTokenPos, knnCtx->kTokenLen);
}

static void executeAggregateDeferred(void *arg);  // forward declaration

// Post executeAggregateDeferred to the coordinator pool. Callers are
// responsible for the one-shot gate: before calling this, they swap every
// shard's success callback to netCursorCallback and clear the iterator's
// errorCB (MRIterator_SwapCallbacks), so any straggler IO-thread event
// takes the steady-state path and cannot re-enter the aggregate callbacks
// after the worker has (possibly) freed iterCtx's deferred-execution fields
// via AREQ_DecrRef. Runs only on the IO thread.
static void dispatchDeferred(AggregateIteratorContext *iterCtx) {
  RS_ASSERT(iterCtx->bc);
  ConcurrentSearch_ThreadPoolRun(executeAggregateDeferred, iterCtx, DIST_THREADPOOL);
}

// Branch on the inbound command type: initial FT.AGGREGATE commands carry
// C_AGG, subsequent FT.CURSOR READ/DEL/PROFILE commands carry C_READ / C_DEL /
// C_PROFILE (rewritten by getCursorCommand after the first reply is pushed).
// Used by the WITHCOUNT callbacks below to tell a shard's first reply from
// its subsequent cursor-read replies without per-shard state.
static inline bool isFirstShardReply(const MRCommand *cmd) {
  return cmd->rootCommand == C_AGG;
}

// WITHCOUNT success callback. Stays wired for the iterator's lifetime; branches
// on the command type to do WITHCOUNT bookkeeping for each shard's first reply
// and just propagate everything else via netCursorCallback. Dispatches the
// deferred execution when a trigger fires (timeout, error reply on a first
// reply, or last first-reply arrives). The dispatch-time
// MRIterator_SwapCallbacks is the one-shot gate: subsequent IO-thread
// events bypass this callback so dispatch happens exactly once.
static void withCountReplyCb(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  MRCommand *cmd = MRIteratorCallback_GetCommand(ctx);
  if (isFirstShardReply(cmd)) {
    AggregateIteratorContext *iterCtx =
        (AggregateIteratorContext *)MRIteratorCallback_GetPrivateData(ctx);
    MRIterator *it = MRIteratorCallback_GetIterator(ctx);
    bool isError = MRReply_Type(rep) == MR_REPLY_ERROR;
    if (!isError) {
      long long shardTotal;
      if (extractTotalResults(rep, cmd, &shardTotal)) {
        iterCtx->totalResults += shardTotal;
      }
    }
    iterCtx->numResponded++;
    bool timedOut = AREQ_TimedOut(iterCtx->areq);
    if (timedOut || isError || iterCtx->numResponded == MRIterator_GetNumShards(it)) {
      if (timedOut) {
        MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(it));
      }
      MRIterator_SwapCallbacks(it, netCursorCallback, NULL);
      dispatchDeferred(iterCtx);
    }
  }
  netCursorCallback(ctx, rep);
}

// WITHCOUNT no-reply termination callback. Stays wired for the iterator's
// lifetime; branches on the command type. For a shard's first command
// (C_AGG) a no-reply termination is treated like an error reply — count the
// shard, swap CBs, dispatch immediately. For subsequent cursor-read no-reply
// terminations there is nothing aggregate-specific to do; MR's
// mrIteratorCallback_Done handles the normal cleanup.
static void withCountErrorCb(MRIteratorCallbackCtx *ctx) {
  MRCommand *cmd = MRIteratorCallback_GetCommand(ctx);
  if (!isFirstShardReply(cmd)) {
    return;
  }
  AggregateIteratorContext *iterCtx =
      (AggregateIteratorContext *)MRIteratorCallback_GetPrivateData(ctx);
  MRIterator *it = MRIteratorCallback_GetIterator(ctx);
  iterCtx->numResponded++;
  // A lost shard (connection drop / send failure) carries no error to surface and
  // is silently omitted, as in the non-WITHCOUNT path. Unlike an error reply there
  // is nothing to propagate, so do not enter Phase B early: that would swap the
  // remaining shards to netCursorCallback and stop accumulating their totals. Wait
  // for the other shards (or a timeout) so their counts are still summed. On a
  // timeout we must dispatch.
  bool timedOut = AREQ_TimedOut(iterCtx->areq);
  if (timedOut || iterCtx->numResponded == MRIterator_GetNumShards(it)) {
    if (timedOut) {
      MRIteratorCallback_SetTimedOut(MRIterator_GetCtx(it));
    }
    MRIterator_SwapCallbacks(it, netCursorCallback, NULL);
    dispatchDeferred(iterCtx);
  }
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

// Create and register the MRIterator for this RPNet without dispatching its
// fan-out (the caller dispatches via MR_StartIterator). Builds the per-iterator
// context (optional WITHCOUNT barrier and optional KNN snapshot), wires the
// abort-wake channel and FT.DEBUG handle, and points base.Next at rpnetNext.
//
// Returns RS_RESULT_OK on success, RS_RESULT_ERROR if the iterator could not be
// created (in which case no callbacks are running and nc->it is left NULL).
static int rpnetCreateIterator(RPNet *nc) {
  // Per-iterator context for privateData. Holds the WITHCOUNT running total and
  // an optional KNN snapshot.
  AggregateIteratorContext *iterCtx = rm_calloc(1, sizeof(AggregateIteratorContext));

  // Mark the WITHCOUNT aggregate path so rpnetNext reports the shard-summed total
  // (accumulated on the IO thread into iterCtx->totalResults) instead of counting
  // rows locally.
  if (HasWithCount(nc->areq) && IsAggregate(nc->areq)) {
    nc->withCount = true;
  }

  // Initialize KNN context if SHARD_K_RATIO optimization is needed.
  if (nc->hasKnnContext) {
    AggregateKnnContext *knnCtx = rm_calloc(1, sizeof(AggregateKnnContext));
    knnCtx->queryArgIndex = nc->knnQueryArgIndex;
    knnCtx->originalK = nc->knnOriginalK;
    knnCtx->shardWindowRatio = nc->knnShardWindowRatio;
    knnCtx->kTokenPos = nc->knnKTokenPos;
    knnCtx->kTokenLen = nc->knnKTokenLen;
    iterCtx->knnCtx = knnCtx;
  }

  // Determine if we need the command modifier callback
  MRCommandModifier cmdModifier = iterCtx->knnCtx ? &aggregateKnnCommandModifier : NULL;

  // Use withCountReplyCb / withCountErrorCb only on the WITHCOUNT path: they
  // accumulate each shard's reported total on the first reply and, once the
  // total is known (last first-reply, error, or timeout), dispatch the deferred
  // plan execution. After dispatch they swap themselves out for netCursorCallback
  // so subsequent cursor-read replies take the steady-state path. For all other
  // paths use netCursorCallback directly; the non-WITHCOUNT path does not wait
  // on a per-shard counter and so does not need an errorCB.
  MRIteratorCallback cb = nc->withCount ? withCountReplyCb : netCursorCallback;
  MRIteratorErrorCallback errCb = nc->withCount ? withCountErrorCb : NULL;

  // The iterator takes ownership of iterCtx and frees it via aggregateIteratorContext_Free.
  MRIterator *it = MR_CreateIterator(&nc->cmd, &(MRIteratorConfig){
    .successCB = cb,
    .errorCB = errCb,
    .cbPrivateData = iterCtx,
    .cbPrivateDataDestructor = aggregateIteratorContext_Free,
    .commandModifier = cmdModifier,
  });

  if (!it) {
    // Iterator never created, so no callbacks are running and it did not take
    // ownership of iterCtx; free it here.
    aggregateIteratorContext_Free(iterCtx);
    return RS_RESULT_ERROR;
  }

  nc->it = it;
  // Register the iterator's channel so the main-thread timeout callback can wake
  // this reader if it blocks in MRIterator_NextWithTimeout after AREQ timed out.
  // Paired with RequestSyncCtx_UnregisterAbortWakeChannel in rpnetFree.
  RequestSyncCtx_RegisterAbortWakeChannel(&nc->areq->syncCtx, MRIterator_GetChannel(it));
#ifdef ENABLE_ASSERT
  // Expose the iterator to FT.DEBUG BG_PENDING_REPLIES; cleared in rpnetFree.
  DebugBgIterator_Set(it);
#endif
  nc->base.Next = rpnetNext;
  return RS_RESULT_OK;
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

  // Lazy (non-deferred) path: create and immediately dispatch the iterator, then
  // pull the first reply. The async WITHCOUNT path creates the iterator eagerly
  // in RSExecDistAggregate before fanning out, so base.Next is already rpnetNext
  // by the time results are pulled and this function is never reached.
  if (rpnetCreateIterator(nc) != RS_RESULT_OK) {
    return RS_RESULT_ERROR;
  }
  MR_StartIterator(nc->it, iterStartCb, NULL);
  return rpnetNext(rp, r);
}

// Deferred execution of the distributed FT.AGGREGATE with WITHCOUNT. Runs on a
// worker thread from the coordinator pool after the first-reply collection
// step has gathered each shard's total_results.
static int executePlan(AREQ *r, struct ConcurrentCmdCtx *cmdCtx, RedisModule_Reply *reply,
                       QueryError *status);

static void executeAggregateDeferred(void *arg) {
  AggregateIteratorContext *iterCtx = (AggregateIteratorContext *)arg;
  AREQ *r = iterCtx->areq;
  RedisModuleBlockedClient *bc = iterCtx->bc;
  WeakRef weak_ref = iterCtx->spec_ref;
  specialCaseCtx *knnCtx = iterCtx->knnSpecialCtx;
  CoordRequestCtx *reqCtx = (CoordRequestCtx *)RedisModule_BlockClientGetPrivateData(bc);

  // Re-promote the WeakRef that the dispatcher stashed in iterCtx. The spec may
  // have been dropped while first-reply collection was running (FT.DROPINDEX,
  // RDB load, server reset); in that case we skip plan execution and reply with
  // DROPPED_BACKGROUND so the client doesn't hang on the blocked reply.
  StrongRef spec_ref = IndexSpecRef_Promote(weak_ref);
  IndexSpec *sp = StrongRef_Get(spec_ref);

  // Capture totalResults before executePlan may free AREQ (and with it nc / iterCtx).
  RS_LOG_ASSERT(AREQ_QueryProcessingCtx(r)->rootProc->type == RP_NETWORK,
                "Expected RP_NETWORK root for distributed aggregate");
  RPNet *nc = (RPNet *)AREQ_QueryProcessingCtx(r)->rootProc;
  long long totalResults = iterCtx->totalResults;

  bool timedOut = CoordRequestCtx_TimedOut(reqCtx);

  if (sp && !timedOut) {
    // Dedicated thread-safe context for this worker. Aliased into sctx->redisCtx
    // so any pipeline step that reads it sees a live ctx on this thread. For
    // cursors the AREQ takes ownership of this ctx (AREQ_Free releases it via
    // the QEXEC_F_IS_CURSOR branch); for non-cursor requests we free it locally
    // after executePlan returns.
    RedisModuleCtx *replyCtx = RedisModule_GetThreadSafeContext(bc);
    r->sctx->redisCtx = replyCtx;

    RedisModule_Reply _reply = RedisModule_NewReply(replyCtx), *reply = &_reply;
    QueryError status = QueryError_Default();
    // Re-point the pipeline's err to this function's local status; the
    // dispatcher's status went out of scope when it returned.
    QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
    qctx->err = &status;

    // Copy the WITHCOUNT total (accumulated on the IO thread by withCountReplyCb)
    // into qctx so the result emitter picks it up.
    if (nc->withCount) {
      qctx->totalResults = totalResults;
    }

    if (AREQ_RequestFlags(r) & QEXEC_F_IS_CURSOR) {
      // Cursor path: AREQ_StartCursor stashes the AREQ on the new Cursor and
      // emits the first chunk via runCursor. We cannot reuse executePlan here
      // because its cursor branch calls ConcurrentCmdCtx_KeepRedisCtx, and we
      // have no ConcurrentCmdCtx (the dispatcher already returned).
      // Cursor ownership of replyCtx: on success the AREQ outlives this
      // function and AREQ_Free frees replyCtx; on failure AREQ_DecrRef below
      // triggers the same AREQ_Free path. Either way, do not free replyCtx locally.
      StrongRef dummy_spec_ref = {.rm = NULL};
      if (AREQ_StartCursor(r, reply, dummy_spec_ref, &status, true) != REDISMODULE_OK) {
        AREQ_ReplyOrStoreError(r, replyCtx, &status);
        AREQ_DecrRef(r);
      }
    } else {
      // Converge on the existing non-cursor path: sendChunk + AREQ_DecrRef.
      // executePlan always returns REDISMODULE_OK in the non-cursor branch.
      executePlan(r, NULL, reply, &status);
      // r is freed by AREQ_DecrRef inside executePlan; do not access r after this.
      RedisModule_FreeThreadSafeContext(replyCtx);
    }
    RedisModule_EndReply(reply);
  } else if (!sp && !timedOut) {
    // Spec dropped between first-reply collection and deferred execution. Reply
    // DROPPED_BACKGROUND and release the AREQ so rpnetFree drops the iterator's
    // reader ref.
    RedisModuleCtx *replyCtx = RedisModule_GetThreadSafeContext(bc);
    QueryError status = QueryError_Default();
    QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
    AREQ_ReplyOrStoreError(r, replyCtx, &status);
    QueryError_ClearError(&status);
    RedisModule_FreeThreadSafeContext(replyCtx);
    AREQ_DecrRef(r);
  } else {
    // Timeout path skipped executePlan; release the AREQ ourselves so rpnetFree
    // drops the iterator's reader ref and the iterator (and iterCtx) is freed.
    AREQ_DecrRef(r);
  }

  if (sp) {
    CurrentThread_ClearIndexSpec();
    IndexSpecRef_Release(spec_ref);
  }
  SpecialCaseCtx_Free(knnCtx);
  WeakRef_Release(weak_ref);
  RedisModule_BlockedClientMeasureTimeEnd(bc);
  void *privdata = RedisModule_BlockClientGetPrivateData(bc);
  RedisModule_UnblockClient(bc, privdata);
}

// Helper to calculate the number of profile arguments
static inline int getProfileArgs(ProfileOptions profileOptions) {
  int profileArgs = 0;
  if (profileOptions != EXEC_NO_FLAGS) {
    profileArgs += 2; // SEARCH/AGGREGATE + QUERY
    if (profileOptions & EXEC_WITH_PROFILE_LIMITED) {
      profileArgs++;
    }
  }
  return profileArgs;
}

// Extract a scalar KNN snapshot for SHARD_K_RATIO optimization if applicable.
// Reads the parsed VectorQuery on the main thread (while specialCaseCtx is
// still alive) and copies the fields the IO-thread command modifier needs
// into outSnapshot. Returns true if the snapshot is populated.
static bool extractKnnOptimizationContext(specialCaseCtx *knnCtx, ProfileOptions profileOptions,
                                          AggregateKnnContext *outSnapshot) {
  RS_ASSERT(outSnapshot != NULL);

  const KNNVectorQuery *knn_query = &knnCtx->knn.queryNode->vn.vq->knn;
  double ratio = knn_query->shardWindowRatio;

  if (ratio >= MAX_SHARD_WINDOW_RATIO) {
    return false;
  }

  int profileArgs = getProfileArgs(profileOptions);
  outSnapshot->queryArgIndex    = 2 + profileArgs;  // Query is at index 2 + profileArgs
  outSnapshot->originalK        = knn_query->k;
  outSnapshot->shardWindowRatio = ratio;
  outSnapshot->kTokenPos        = knn_query->k_token_pos;
  outSnapshot->kTokenLen        = knn_query->k_token_len;
  return true;
}

// Build the distributed MR command for FT.AGGREGATE
static void buildMRCommand(RedisModuleString **argv, int argc, ProfileOptions profileOptions,
                           AREQDIST_UpstreamInfo *us, MRCommand *xcmd, IndexSpec *sp) {
  // We need to prepend the array with the command, index, and query that
  // we want to use.
  const char **tmparr = array_new(const char *, array_len(us->serialized));

  const char *index_name = RedisModule_StringPtrLen(argv[1], NULL);

  int profileArgs = getProfileArgs(profileOptions);
  if (profileOptions == EXEC_NO_FLAGS) {
    array_append(tmparr, RS_AGGREGATE_CMD);                         // Command
    array_append(tmparr, index_name);  // Index name
  } else {
    array_append(tmparr, RS_PROFILE_CMD);
    array_append(tmparr, index_name);  // Index name
    array_append(tmparr, "AGGREGATE");
    if (profileOptions & EXEC_WITH_PROFILE_LIMITED) {
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


static void buildDistRPChain(AREQ *r, MRCommand *xcmd, AREQDIST_UpstreamInfo *us,
                             int (*nextFunc)(ResultProcessor *, SearchResult *),
                             const AggregateKnnContext *knnSnapshot) {
  // Establish our root processor, which is the distributed processor
  RPNet *rpRoot = RPNet_New(xcmd, nextFunc); // This will take ownership of the command
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(r);
  rpRoot->base.parent = qctx;
  rpRoot->lookup = us->lookup;
  rpRoot->areq = r;

  // Store KNN scalar snapshot for SHARD_K_RATIO optimization (used by
  // rpnetNext_Start to build the iterator-owned AggregateKnnContext)
  if (knnSnapshot) {
    rpRoot->hasKnnContext      = true;
    rpRoot->knnQueryArgIndex   = knnSnapshot->queryArgIndex;
    rpRoot->knnOriginalK       = knnSnapshot->originalK;
    rpRoot->knnShardWindowRatio = knnSnapshot->shardWindowRatio;
    rpRoot->knnKTokenPos       = knnSnapshot->kTokenPos;
    rpRoot->knnKTokenLen       = knnSnapshot->kTokenLen;
  }

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

  AREQ_SetCanYieldPartialResults(r);
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

// Pin the request's config + tail-pipeline policy to the dispatch-time value
// (CoordRequestCtx), undoing AREQ_New/AREQ_Compile's RSGlobalConfig re-read on
// the BG thread. Avoids a TOCTOU with a concurrent FT.CONFIG SET (mirrors hybrid).
static void applyCoordReqConfigTimeoutPolicy(AREQ *r, RSTimeoutPolicy policy) {
  r->reqConfig.timeoutPolicy = policy;
  r->pipeline.qctx.timeoutPolicy = policy;
}

static int prepareForExecution(AREQ *r, RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               IndexSpec *sp, specialCaseCtx **knnCtx_ptr, size_t numShards,
                               RSTimeoutPolicy requestTimeoutPolicy, QueryError *status) {
  AREQ_QueryProcessingCtx(r)->err = status;
  AREQ_AddRequestFlags(r, QEXEC_F_IS_AGGREGATE | QEXEC_F_IS_COORDINATOR);
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

  // User-facing cursors are unsupported on disk (flex). Reject before fan-out.
  // The coordinator's own shard fan-out (which always adds WITHCURSOR to the
  // shard `_FT.AGGREGATE`) does not pass through here.
  if ((AREQ_RequestFlags(r) & QEXEC_F_IS_CURSOR) &&
      !SearchDisk_MarkUnsupportedArgumentIfDiskEnabled("WITHCURSOR", status)) {
    return REDISMODULE_ERR;
  }

  // Pin back to the dispatch-time policy before skipTimeoutChecks / the pipeline
  // ctx are derived from it below.
  applyCoordReqConfigTimeoutPolicy(r, requestTimeoutPolicy);

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

  // The coordinator merges shard-local groups, so allow one configured cap per shard.
  AggregationPipelineParams aggregationParams = AREQ_MakeAggregationPipelineParams(
      r, GroupByLimits_ForCoordinator(RSGlobalConfig.maxAggregateGroups, numShards));

  AREQDIST_UpstreamInfo us = {NULL};
  rc = AREQ_BuildDistributedPipeline(r, &us, &aggregationParams, status);
  if (rc != REDISMODULE_OK) return REDISMODULE_ERR;

  // Construct the command string
  MRCommand xcmd;
  AggregateKnnContext knnSnapshot;
  bool hasKnnSnapshot = false;
  buildMRCommand(argv, argc, profileOptions, &us, &xcmd, sp);

  if (knnCtx) {
    hasKnnSnapshot = extractKnnOptimizationContext(knnCtx, profileOptions, &knnSnapshot);
  }

  xcmd.protocol = is_resp3(ctx) ? 3 : 2;
  xcmd.forCursor = AREQ_RequestFlags(r) & QEXEC_F_IS_CURSOR;
  xcmd.forProfiling = IsProfile(r);
  xcmd.rootCommand = C_AGG;  // Response is equivalent to a `CURSOR READ` response
  xcmd.coordStartTime = r->profileClocks.coordStartTime;

  // Build the result processor chain (pass KNN snapshot for SHARD_K_RATIO optimization)
  buildDistRPChain(r, &xcmd, &us, rpnetNext_Start, hasKnnSnapshot ? &knnSnapshot : NULL);

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

// Eagerly create and start the MR iterator for a WITHCOUNT request, transfer
// ownership of (areq, knnCtx, weak spec ref, blocked client) into the iterator
// context, and return. The pipeline runs later on the coordinator pool via
// executeAggregateDeferred, once withCountReplyCb has summed every shard's
// total_results.
//
// On success: releases strong_ref and ends reply; ownership of r, knnCtx, and
// the dispatcher's weak ref is transferred to iterCtx. On failure: status is
// set and the caller cleans up via DistAggregateCleanups (which still owns r,
// knnCtx, and the refs).
static int dispatchAggregateDeferred(AREQ *r, struct ConcurrentCmdCtx *cmdCtx,
                                     specialCaseCtx *knnCtx, StrongRef strong_ref,
                                     RedisModule_Reply *reply, QueryError *status) {
  RS_LOG_ASSERT(AREQ_QueryProcessingCtx(r)->rootProc->type == RP_NETWORK,
                "Expected RP_NETWORK root for distributed aggregate");
  RPNet *nc = (RPNet *)AREQ_QueryProcessingCtx(r)->rootProc;

  if (rpnetCreateIterator(nc) != RS_RESULT_OK) {
    QueryError_SetCode(status, QUERY_ERROR_CODE_GENERIC);
    return REDISMODULE_ERR;
  }
  // Wire deferred-execution fields into the iterator's private data before
  // MR_StartIterator so the IO thread sees them when withCountReplyCb fires.
  AggregateIteratorContext *iterCtx =
      (AggregateIteratorContext *)MRIterator_GetPrivateData(nc->it);
  iterCtx->bc = ConcurrentCmdCtx_GetBlockedClient(cmdCtx);
  iterCtx->areq = r;
  // Transfer the dispatcher's WeakRef into iterCtx; executeAggregateDeferred
  // promotes it back to a StrongRef (or treats the spec as dropped).
  iterCtx->spec_ref = ConcurrentCmdCtx_TakeWeakRef(cmdCtx);
  iterCtx->knnSpecialCtx = knnCtx;

  // Drop the dispatcher's ctx alias; executeAggregateDeferred creates its own
  // thread-safe ctx on the worker thread.
  r->sctx->redisCtx = NULL;
  // Defer the unblock to executeAggregateDeferred.
  ConcurrentCmdCtx_KeepBlockedClient(cmdCtx);
  // Start the fan-out; withCountReplyCb posts executeAggregateDeferred once all
  // shard first-replies are in.
  MR_StartIterator(nc->it, iterStartCb, NULL);
  // Release the dispatcher's StrongRef; the spec is no longer pinned across
  // the async wait. executeAggregateDeferred re-promotes iterCtx->spec_ref.
  IndexSpecRef_Release(strong_ref);
  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
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

  // The global timeout policy may change before this background job is picked up.
  // Use the policy captured from the original request.
  RSTimeoutPolicy requestTimeoutPolicy = CoordRequestCtx_GetTimeoutPolicy(reqCtx);
  if (requestTimeoutPolicy == TimeoutPolicy_ReturnStrict) {
    r->syncCtx.requiresAggregateResultsSync = true;
  }
  CoordRequestCtx_SetRequest(reqCtx, r);
  CoordRequestCtx_UnlockSetRequest(reqCtx);

  QueryError status = QueryError_Default();
  specialCaseCtx *knnCtx = NULL;

  // Store coordinator start time for dispatch time tracking
  r->profileClocks.coordStartTime = ConcurrentCmdCtx_GetCoordStartTime(cmdCtx);
  size_t numShards = ConcurrentCmdCtx_GetNumShards(cmdCtx);

  // Check if the index still exists, and promote the ref accordingly
  StrongRef strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  IndexSpec *sp = StrongRef_Get(strong_ref);
  if (!sp) {
    QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
    goto err;
  }

  if (prepareForExecution(r, ctx, argv, argc, sp, &knnCtx, numShards, requestTimeoutPolicy,
                          &status) != REDISMODULE_OK) {
    goto err;
  }

  // WITHCOUNT: eagerly create the MR iterator, hand off to deferred execution
  // (runs on the coordinator pool after all shard first-replies have been
  // collected, then converges on executePlan's non-cursor branch). Non-WITHCOUNT
  // requests stay on the synchronous executePlan path below.
  if (HasWithCount(r)) {
    if (dispatchAggregateDeferred(r, cmdCtx, knnCtx, strong_ref, reply, &status)
        != REDISMODULE_OK) {
      goto err;
    }
    return;
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
int DistAggregateTimeoutFailCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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
// thread after the background pipeline has aborted. Flips RPNet to drainOnly
// mode so the post-abort drain only pulls already-buffered shard replies, then
// delegates the actual loop to the shared helper.
static void drainPartialResultsAfterTimeout(AREQ *req) {
  QueryProcessingCtx *qctx = AREQ_QueryProcessingCtx(req);
  if (!qctx->canYieldPartialResults) {
    return;
  }

  RS_ASSERT(qctx->rootProc->type == RP_NETWORK);
  ((RPNet *)qctx->rootProc)->drainOnly = true;

  AREQ_DrainStoredResultsAfterTimeout(req);
}

// Timeout callback for Coordinator AREQ execution
// Called on the main thread when the blocking client times out (RETURN-STRICT policy only).
int DistAggregateTimeoutReturnStrictCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

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
    // Intentionally claim as worker-owned here: query-level coord aggregate timeouts do not use
    // the cursor-read timeout-owner cleanup path, and the worker must still observe a claimed
    // aggregation phase so it stores/signals the timed-out state for partial-result handling.
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
  if (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) {
    req->storedReplyState.rc = RS_RESULT_TIMEDOUT;
  }

  // Harvest any shard replies that landed in the channel before the deadline.
  // No-op for already-complete runs.
  drainPartialResultsAfterTimeout(req);

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

  // Under RETURN-STRICT, a shard's TIMEDOUT warning does not abort the coord
  // pipeline (see processWarningsAndCleanup in src/coord/rpnet.c): RPNet keeps
  // draining the remaining shards and the warning is surfaced via the
  // QEXEC_S_SHARD_TIMED_OUT_WARNING flag. The only RETURN-STRICT path that
  // still produces rc=TIMEDOUT is the coord's own deadline firing, which
  // routes through DistAggregateTimeoutReturnStrictCallback -- not this
  // callback. Under FAIL, a shard timeout still bails the coord pipeline
  // early; the BG thread stores the resulting error in storedReplyState.err
  // and the early-error branch above replies with it.
  AREQ_ReplyWithStoredResults(ctx, req);

  // Note: No AREQ_DecrRef here - CoordRequestCtx_Free releases the context's reference.
  return REDISMODULE_OK;
}

// Coordinator FT.CURSOR READ timeout callback for the RETURN_STRICT policy.
// Runs on the main thread when the BC times out. Unlike the FT.AGGREGATE
// RETURN_STRICT path, no TryClaim here: BG's existing `(!TryClaim || TimedOut)`
// check at startPipelineCommon handles pipeline-side bails, and pre-pipeline
// bails are signaled via AREQ_ReplyOrStoreError. The timer waits and branches
// on `hasStoredResults`.
int DistCursorReadTimeoutReturnStrictCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  CoordRequestCtx *reqCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!reqCtx) {
    return RedisModule_ReplyWithError(ctx, "Internal error: timeout with no context");
  }
  RS_ASSERT(reqCtx->type == COMMAND_AGGREGATE);

  // Read `req` under the same lock that gates BG's TakeForExecution +
  // SetRequest, so `req == NULL` reliably proxies "BG has not yet taken
  // the cursor".
  CoordRequestCtx_LockSetRequest(reqCtx);
  CoordRequestCtx_SetTimedOut(reqCtx);
  AREQ *req = (AREQ *)CoordRequestCtx_GetRequest(reqCtx);
  if (req) {
    CoordRequestCtx_UnlockSetRequest(reqCtx);
  } else {
    // BG never took the cursor (or hit the early TimedOut check and bailed
    // before taking). No condvar signal will arrive; reply directly with
    // a depleted cursor. Cid was validated by CursorCommand on the main thread
    // before BC arming, so argv[3] is trusted. Purge while still holding
    // setRequestLock: BG cannot pass its TimedOut check and take the cursor
    // concurrently.
    long long cid;
    int rc = RedisModule_StringToLongLong(argv[3], &cid);
    RS_ASSERT(rc == REDISMODULE_OK);
    Cursors_PurgeForDb(GetGlobalCursor((uint64_t)cid), (uint64_t)cid, RedisModule_GetSelectedDb(ctx));
    CoordRequestCtx_UnlockSetRequest(reqCtx);
    return coord_cursor_read_empty_reply_timeout(ctx, 0);
  }

  // BG has taken the cursor. Wake the abort channel — unblocks BG from
  // MRIterator_NextWithTimeout if it's mid-pipeline; no-op otherwise.
  RequestSyncCtx_WakeAbortChannel(&req->syncCtx);

  // Sync with BG.
  AREQ_WaitForAggregateResultsComplete(req);

  if (req->storedReplyState.hasStoredResults) {
    // Drain anything queued before the deadline, then serialize and dispose
    // the stashed cursor (Pause if more rows remain, Free on EOF) inside
    // AREQ_ReplyWithStoredResults.
    req->storedReplyState.rc = RS_RESULT_TIMEDOUT;
    drainPartialResultsAfterTimeout(req);
    AREQ_ReplyWithStoredResults(ctx, req);
  } else {
    // Pre-pipeline bail through AREQ_ReplyOrStoreError. Currently unreachable
    // on coord+RETURN_STRICT (coordinator cursors have a NULL spec_ref so the
    // only such bail in cursorRead can't fire); kept for forward-compat.
    QueryError *err = &req->storedReplyState.err;
    RS_ASSERT(QueryError_HasError(err));
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(err), 1, COORD_ERR_WARN);
    QueryError_ReplyAndClear(ctx, err);
  }
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
  size_t numShards = 0;
  RPNet *rpnet = NULL;
  // The global timeout policy may change before this background job is picked up.
  // Use the policy captured from the original request. Declared here, before any
  // `goto err`, to avoid jumping over its initialization.
  RSTimeoutPolicy requestTimeoutPolicy = CoordRequestCtx_GetTimeoutPolicy(reqCtx);

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

  if (requestTimeoutPolicy == TimeoutPolicy_ReturnStrict) {
    r->syncCtx.requiresAggregateResultsSync = true;
  }
  CoordRequestCtx_SetRequest(reqCtx, r);
  CoordRequestCtx_UnlockSetRequest(reqCtx);

  // Store coordinator start time for dispatch time tracking
  r->profileClocks.coordStartTime = ConcurrentCmdCtx_GetCoordStartTime(cmdCtx);
  numShards = ConcurrentCmdCtx_GetNumShards(cmdCtx);
  debug_params = debug_req->debug_params;
  // Check if the index still exists, and promote the ref accordingly
  strong_ref = IndexSpecRef_Promote(ConcurrentCmdCtx_GetWeakRef(cmdCtx));
  sp = StrongRef_Get(strong_ref);
  if (!sp) {
    QueryError_SetCode(&status, QUERY_ERROR_CODE_DROPPED_BACKGROUND);
    goto err;
  }

  debug_argv_count = debug_params.debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>` strings
  if (prepareForExecution(r, ctx, argv, argc - debug_argv_count, sp, &knnCtx, numShards,
                          requestTimeoutPolicy, &status) != REDISMODULE_OK) {
    goto err;
  }

  // rpnet now owns the command
  rpnet = (RPNet *)AREQ_QueryProcessingCtx(r)->rootProc;
  cmd = &rpnet->cmd;

  MRCommand_Insert(cmd, 0, "_FT.DEBUG", sizeof("_FT.DEBUG") - 1);
  // The _FT.DEBUG prefix shifts every existing argument by one; adjust the
  // saved KNN query argument index so the SHARD_K_RATIO modifier rewrites the
  // right slot.
  if (rpnet->hasKnnContext) rpnet->knnQueryArgIndex += 1;
  // insert also debug params at the end
  for (size_t i = 0; i < debug_argv_count; i++) {
    size_t n;
    const char *arg = RedisModule_StringPtrLen(debug_params.debug_argv[i], &n);
    MRCommand_Append(cmd, arg, n);
  }

  if (parseAndCompileDebug(debug_req, &status) != REDISMODULE_OK) {
    goto err;
  }

  // WITHCOUNT follows the same eager-async path as the non-debug entry point.
  if (HasWithCount(r)) {
    if (dispatchAggregateDeferred(r, cmdCtx, knnCtx, strong_ref, reply, &status)
        != REDISMODULE_OK) {
      goto err;
    }
    return;
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
