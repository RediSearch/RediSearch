/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hybrid_cursor_mappings.h"

#include <string.h>

#include "hybrid/hybrid_exec.h"
#include "query_error_ffi.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "shard_window_ratio.h"
#include "rmr/reply.h"
#include "rmr/io_runtime_ctx.h"
#include "rmr/rmr.h"

#ifdef ENABLE_ASSERT
#include "debug_commands.h"
#endif

void HybridArmingCtx_Free(void *p) {
  HybridArmingCtx *ctx = (HybridArmingCtx *)p;
  for (size_t i = 0; i < ctx->numArms; i++) {
    rm_free(ctx->suffixes[i]);
  }
  rm_free(ctx->knnCtx);
  rm_free(ctx);
}

static inline MRReply *clusterQueryErrorReply(void) {
  return MRReply_CreateError(CLUSTER_QUERY_ERROR, strlen(CLUSTER_QUERY_ERROR));
}

// Push `reply` into every arm's read stream. Each channel's reader frees its
// replies independently, so the streams cannot share one reply: all but the
// last get a clone, made before the original is pushed, since a push hands
// ownership to a reader that may free it concurrently.
static void pushToEveryArm(HybridArmingCtx *ctx, MRReply *reply) {
  for (size_t i = 0; i + 1 < ctx->numArms; i++) {
    MRIterator_PushReply(ctx->its[i], MRReply_Clone(reply));
  }
  MRIterator_PushReply(ctx->its[ctx->numArms - 1], reply);
}

// Forward mapping-stage warnings into the read streams, where each subquery's
// RPNet folds them (see processHybridMappingWarning in rpnet.c). Pushed one
// bare string reply per warning — a top-level string is unambiguous in a read
// stream, whose other replies are arrays or errors. Max-prefix warnings are
// suffix-tagged with the subquery they belong to and routed to that stream
// alone; the rest (timeout / shard OOM) are whole-shard conditions and go to
// every stream.
static void forwardWarnings(HybridArmingCtx *ctx, MRReply *warnings) {
  for (size_t i = 0; i < MRReply_Length(warnings); i++) {
    MRReply *warning = MRReply_TakeArrayElement(warnings, i);
    const char *warning_str = MRReply_String(warning, NULL);
    if (!strncmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS, strlen(QUERY_WMAXPREFIXEXPANSIONS))) {
      // The warning ends with its arm's suffix. The first arm takes one that matches no suffix,
      // as SEARCH did before arms were labelled.
      size_t target = 0;
      const size_t len = strlen(warning_str);
      for (size_t a = 0; a < ctx->numArms; a++) {
        const size_t suffixLen = strlen(ctx->suffixes[a]);
        if (len >= suffixLen && !strcmp(warning_str + len - suffixLen, ctx->suffixes[a])) {
          target = a;
          break;
        }
      }
      MRIterator_PushReply(ctx->its[target], warning);
      continue;
    }
    pushToEveryArm(ctx, warning);
  }
}

// Arm (or retire) one shard's placeholders on every read iterator. The shard
// reserves every subquery cursor before publishing anything
// (HybridRequest_ReserveSubCursors), so a mapping's ids are either all nonzero
// or all 0 (the shard bailed and published nothing). Published cursors of a
// request that already timed out are deleted instead of read; either way the id
// ends up in a live command and the standard teardown covers any abort from
// here on.
static void armShardReads(HybridArmingCtx *ctx, uint16_t shardIdx, const long long *cids) {
  for (size_t i = 0; i < ctx->numArms; i++) {
    RS_ASSERT((cids[i] == 0) == (cids[0] == 0));
    if (cids[i] == 0) {
      MRIterator_ResolveShard(ctx->its[i], shardIdx, 0);
    } else {
      MRIterator_ArmShardCursorRead(ctx->its[i], shardIdx, cids[i]);
    }
  }
}

// Surface a shard-level failure to every read stream and retire the shard's
// placeholders. The error is pushed before the placeholders are resolved so
// the resolving side's channel unblock cannot beat the error into the reader.
// Consumes the error reply.
static void failShardReads(HybridArmingCtx *ctx, uint16_t shardIdx, MRReply *error) {
  pushToEveryArm(ctx, error);
  for (size_t i = 0; i < ctx->numArms; i++) {
    MRIterator_ResolveShard(ctx->its[i], shardIdx, 1);
  }
}

// Whole-fan-out failure before anything was dispatched (pre-fanout connection
// validation failed, see hybridArmingStartCb): every iterator still holds its
// single initial placeholder — surface one error per read stream and retire
// those placeholders. No shard was sent anything, so no callback ever fires.
static void failReadsBeforeExpansion(HybridArmingCtx *ctx) {
  for (size_t i = 0; i < ctx->numArms; i++) {
    RS_ASSERT(MRIterator_GetNumShards(ctx->its[i]) == 1);
    MRIterator_PushReply(ctx->its[i], clusterQueryErrorReply());
    MRIterator_ResolveShard(ctx->its[i], 0, 1);
  }
}

// Parse a shard's cursor-mapping reply. Both protocols carry the same layout —
// a RESP2 array or a RESP3 map, stored either way as a flat element array: one
// label/cursor-id pair per arm, in slot order, then "warnings" and its array
// (the emission order of replyWithCursors in hybrid_exec.c). With SEARCH and one
// VSIM that is ["SEARCH", <cid>, "VSIM", <cid>, "warnings", [...]]. A shard that
// bails before parsing (common_hybrid_query_reply_empty) cannot know the arm
// count and always sends SEARCH and VSIM with id 0, so the pairs are read until
// "warnings" and arms with no pair read as 0. Consumes the reply.
static void processHybridMapping(HybridArmingCtx *ctx, MRReply *rep, uint16_t shardIdx) {
  // Quietly read any unexpected layout as "shard published no cursors" —
  // e.g. a profile-wrapped envelope from a shard build that still wrapped its
  // early-bail reply. All of that shard's streams end empty; nothing leaks
  // (it published no cursor ids to begin with).
  long long cids[HYBRID_NUM_ARM_SLOTS] = {0};
  RS_ASSERT(MRReply_Type(rep) == MR_REPLY_ARRAY || MRReply_Type(rep) == MR_REPLY_MAP);
  const size_t len = MRReply_Length(rep);
  size_t arm = 0;
  for (size_t i = 0; i + 1 < len; i += 2) {
    MRReply *key = MRReply_ArrayElement(rep, i);
    MRReply *value = MRReply_ArrayElement(rep, i + 1);
    if (MRReply_StringEquals(key, "warnings", true)) {
      forwardWarnings(ctx, value);
      break;
    }
    if (arm < ctx->numArms) {
      MRReply_ToInteger(value, &cids[arm]);
    }
    arm++;
  }
  // Either one id per arm, or a shard that published none.
  if (arm != ctx->numArms) {
    for (size_t i = 0; i < ctx->numArms; i++) {
      RS_ASSERT(cids[i] == 0);
      cids[i] = 0;
    }
  }
  armShardReads(ctx, shardIdx, cids);
  MRReply_Free(rep);
}

void hybridArmingCallback(MRIteratorCallbackCtx *ctx, MRReply *rep) {
#ifdef ENABLE_ASSERT
  // Sync point (debug): park the IO thread with a shard's cursor mapping in
  // hand, before its reads are armed — the deterministic spot to stage a
  // coordinator timeout between cursor publication and cursor consumption.
  SyncPoint_Wait(SYNC_POINT_BEFORE_HYBRID_ARM_READS);
#endif
  HybridArmingCtx *cb_ctx = (HybridArmingCtx *)MRIteratorCallback_GetPrivateData(ctx);
  RS_ASSERT(cb_ctx);
  const uint16_t shardIdx = MRIteratorCallback_GetShardIdx(ctx);
  bool isError = MRReply_Type(rep) == MR_REPLY_ERROR;

  if (isError) {
    failShardReads(cb_ctx, shardIdx, rep);
  } else {
    processHybridMapping(cb_ctx, rep, shardIdx);
  }

  MRIteratorCallback_Done(ctx, isError);
}

void hybridArmingErrorCallback(MRIteratorCallbackCtx *ctx) {
  HybridArmingCtx *cb_ctx = (HybridArmingCtx *)MRIteratorCallback_GetPrivateData(ctx);
  RS_ASSERT(cb_ctx);
  failShardReads(cb_ctx, MRIteratorCallback_GetShardIdx(ctx), clusterQueryErrorReply());
}

void hybridArmingStartCb(void *p) {
  MRIterator *hybridIt = (MRIterator *)p;
  HybridArmingCtx *ctx = (HybridArmingCtx *)MRIterator_GetPrivateData(hybridIt);
  // The read iterators complete as independent logical requests (each calls
  // IORuntimeCtx_RequestCompleted when it drains) but share this one
  // scheduled job — register them so the queue's pending accounting balances.
  IORuntimeCtx *ioRuntime = MRIterator_GetIORuntime(hybridIt);
  for (size_t i = 0; i < ctx->numArms; i++) {
    IORuntimeCtx_RequestStarted(ioRuntime);
  }
  // Validate connections before expanding anything, so a failure retires
  // exactly one placeholder per iterator. iterStartCb re-validates internally,
  // but connection state only changes via jobs on this same IO loop, so its
  // check cannot disagree with this one.
  if (!MRIterator_AllShardsConnected(hybridIt)) {
    failReadsBeforeExpansion(ctx);
    MRIterator_ResolveShard(hybridIt, 0, 1);
    return;
  }
  for (size_t i = 0; i < ctx->numArms; i++) {
    iterExpandShellsCb(ctx->its[i]);
  }
  iterStartCb(hybridIt);
}

void HybridKnnApplyShardKRatio(MRCommand *cmd, size_t numShards, const HybridKnnContext *knnCtx) {
  RS_ASSERT(cmd && knnCtx && knnCtx->kArgIndex >= 0);
  // Only apply optimization for multi-shard deployments with valid ratio
  if (numShards <= 1 || knnCtx->shardWindowRatio >= MAX_SHARD_WINDOW_RATIO) {
    return;
  }
  size_t effectiveK = calculateEffectiveK(knnCtx->originalK, knnCtx->shardWindowRatio, numShards);
  modifyVsimKNN(cmd, knnCtx->kArgIndex, effectiveK, knnCtx->originalK);
}

void HybridKnnCommandModifier(MRCommand *cmd, size_t numShards, void *privateData) {
  RS_ASSERT(privateData && cmd);
  const HybridArmingCtx *ctx = (const HybridArmingCtx *)privateData;
  HybridKnnApplyShardKRatio(cmd, numShards, ctx->knnCtx);
}
