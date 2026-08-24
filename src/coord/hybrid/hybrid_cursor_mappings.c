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

#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "shard_window_ratio.h"
#include "rmr/reply.h"
#include "rmr/io_runtime_ctx.h"
#include "rmr/rmr.h"

#ifdef ENABLE_ASSERT
#include "debug_commands.h"
#endif

// Flattened element count of a shard's cursor-mapping reply (3 key/value
// pairs); identical in both protocols. See processHybridMapping.
#define HYBRID_MAPPING_REPLY_LENGTH 6

void HybridArmingCtx_Free(void *p) {
  HybridArmingCtx *ctx = (HybridArmingCtx *)p;
  rm_free(ctx->knnCtx);
  rm_free(ctx);
}

// Forward mapping-stage warnings into both read streams, where each subquery's
// RPNet folds the ones addressed to it (see processHybridMappingWarning in
// rpnet.c). Pushed one bare string reply per warning — a top-level string is
// unambiguous in a read stream, whose other replies are arrays or errors.
// Each channel's reader frees its replies independently, so the two streams
// cannot share one reply: the original is taken out of `warnings` for one
// stream and a single clone is made for the other.
static void forwardWarnings(HybridArmingCtx *ctx, MRReply *warnings) {
  if (!warnings) {
    return;
  }
  for (size_t i = 0; i < MRReply_Length(warnings); i++) {
    MRReply *warning = MRReply_TakeArrayElement(warnings, i);
    // Clone before pushing the original: a push hands ownership to the
    // reader, which may free it concurrently.
    MRReply *clone = MRReply_Clone(warning);
    MRIterator_PushReply(ctx->searchIt, clone);
    MRIterator_PushReply(ctx->vsimIt, warning);
  }
}

// Arm (or retire) one shard's placeholders on both read iterators. A cursor id
// of 0 means the shard published no cursor for that stream (e.g. it bailed on
// a strict timeout). Published cursors of an abandoned request — the request
// timed out, or the paired stream is absent so no merge can happen — are
// deleted instead of read; either way the id ends up in a live command and the
// standard teardown covers any abort from here on.
static void armShardReads(HybridArmingCtx *ctx, uint16_t shardIdx, long long searchCid,
                          long long vsimCid) {
  const bool partialPair = (searchCid == 0) != (vsimCid == 0);
  if (searchCid == 0) {
    MRIterator_ResolveShard(ctx->searchIt, shardIdx, 0);
  } else {
    MRIterator_ArmShardCursorRead(
        ctx->searchIt, shardIdx, searchCid,
        partialPair || MRIteratorCallback_GetTimedOut(MRIterator_GetCtx(ctx->searchIt)));
  }
  if (vsimCid == 0) {
    MRIterator_ResolveShard(ctx->vsimIt, shardIdx, 0);
  } else {
    MRIterator_ArmShardCursorRead(
        ctx->vsimIt, shardIdx, vsimCid,
        partialPair || MRIteratorCallback_GetTimedOut(MRIterator_GetCtx(ctx->vsimIt)));
  }
}

// Surface a shard-level failure to both read streams and retire the shard's
// placeholders. The error is pushed before the placeholders are resolved so
// the resolving side's channel unblock cannot beat the error into the reader.
static void failShardReads(HybridArmingCtx *ctx, uint16_t shardIdx, MRReply *error) {
  MRIterator_PushReply(ctx->searchIt, MRReply_Clone(error));
  MRIterator_PushReply(ctx->vsimIt, MRReply_Clone(error));
  MRIterator_ResolveShard(ctx->searchIt, shardIdx, 1);
  MRIterator_ResolveShard(ctx->vsimIt, shardIdx, 1);
}

// Whole-fan-out failure before anything was dispatched (pre-fanout connection
// validation failed, see hybridArmingStartCb): every iterator still holds its
// single initial placeholder — surface one error per read stream and retire
// those placeholders. No shard was sent anything, so no callback ever fires.
static void failReadsBeforeExpansion(HybridArmingCtx *ctx, const char *msg) {
  MRIterator *its[2] = {ctx->searchIt, ctx->vsimIt};
  for (int j = 0; j < 2; j++) {
    RS_ASSERT(MRIterator_GetNumShards(its[j]) == 1);
    MRIterator_PushReply(its[j], MRReply_CreateError(msg, strlen(msg)));
    MRIterator_ResolveShard(its[j], 0, 1);
  }
}

// Parse a shard's cursor-mapping reply. Both protocols carry the same fixed
// layout — a RESP2 array or a RESP3 map, stored either way as a flat element
// array: ["SEARCH", <cid>, "VSIM", <cid>, "warnings", [...]] (the emission
// order of replyWithCursors in hybrid_exec.c). The structure is asserted in
// debug builds; production extracts the values by offset.
static void processHybridMapping(HybridArmingCtx *ctx, MRReply *rep, uint16_t shardIdx) {
  RS_ASSERT(MRReply_Length(rep) == HYBRID_MAPPING_REPLY_LENGTH);
  RS_ASSERT(MRReply_StringEquals(MRReply_ArrayElement(rep, 0), "SEARCH", true));
  RS_ASSERT(MRReply_StringEquals(MRReply_ArrayElement(rep, 2), "VSIM", true));
  RS_ASSERT(MRReply_StringEquals(MRReply_ArrayElement(rep, 4), "warnings", true));
  long long searchCid = 0, vsimCid = 0;
  MRReply_ToInteger(MRReply_ArrayElement(rep, 1), &searchCid);
  MRReply_ToInteger(MRReply_ArrayElement(rep, 3), &vsimCid);
  forwardWarnings(ctx, MRReply_ArrayElement(rep, 5));
  armShardReads(ctx, shardIdx, searchCid, vsimCid);
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
  const int replyType = MRReply_Type(rep);
  bool isError = replyType == MR_REPLY_ERROR;

  if (isError) {
    failShardReads(cb_ctx, shardIdx, rep);
  } else if (replyType == MR_REPLY_MAP || replyType == MR_REPLY_ARRAY) {
    processHybridMapping(cb_ctx, rep, shardIdx);
  } else {
    MRReply *error = MRReply_CreateError(CLUSTER_QUERY_ERROR, strlen(CLUSTER_QUERY_ERROR));
    failShardReads(cb_ctx, shardIdx, error);
    MRReply_Free(error);
    isError = true;
  }

  MRIteratorCallback_Done(ctx, isError);
  MRReply_Free(rep);
}

void hybridArmingErrorCallback(MRIteratorCallbackCtx *ctx) {
  HybridArmingCtx *cb_ctx = (HybridArmingCtx *)MRIteratorCallback_GetPrivateData(ctx);
  RS_ASSERT(cb_ctx);
  MRReply *error = MRReply_CreateError(CLUSTER_QUERY_ERROR, strlen(CLUSTER_QUERY_ERROR));
  failShardReads(cb_ctx, MRIteratorCallback_GetShardIdx(ctx), error);
  MRReply_Free(error);
}

void hybridArmingStartCb(void *p) {
  MRIterator *hybridIt = (MRIterator *)p;
  HybridArmingCtx *ctx = (HybridArmingCtx *)MRIterator_GetPrivateData(hybridIt);
  // The read iterators complete as independent logical requests (each calls
  // IORuntimeCtx_RequestCompleted when it drains) but share this one
  // scheduled job — register them so the queue's pending accounting balances.
  IORuntimeCtx *ioRuntime = MRIterator_GetIORuntime(hybridIt);
  IORuntimeCtx_RequestStarted(ioRuntime);
  IORuntimeCtx_RequestStarted(ioRuntime);
  // Validate connections before expanding anything, so a failure retires
  // exactly one placeholder per iterator. iterStartCb re-validates internally,
  // but connection state only changes via jobs on this same IO loop, so its
  // check cannot disagree with this one.
  if (!MRIterator_AllShardsConnected(hybridIt)) {
    failReadsBeforeExpansion(ctx, CLUSTER_QUERY_ERROR);
    MRIterator_ResolveShard(hybridIt, 0, 1);
    return;
  }
  iterExpandShellsCb(ctx->searchIt);
  iterExpandShellsCb(ctx->vsimIt);
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
