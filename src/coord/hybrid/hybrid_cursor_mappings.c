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
#include "rmr/rmr.h"

#ifdef ENABLE_ASSERT
#include "debug_commands.h"
#endif

#define INTERNAL_HYBRID_RESP3_LENGTH 6
#define INTERNAL_HYBRID_RESP2_LENGTH 6

void HybridArmingCtx_Free(void *p) {
  HybridArmingCtx *ctx = (HybridArmingCtx *)p;
  rm_free(ctx->knnCtx);
  rm_free(ctx);
}

// Forward mapping-stage warnings into both read streams, where each subquery's
// RPNet folds the ones addressed to it (see processHybridMappingWarning in
// rpnet.c). Pushed one bare string reply per warning — a top-level string is
// unambiguous in a read stream, whose other replies are arrays or errors.
// `warnings` stays owned by the enclosing reply; clones are pushed.
static void forwardWarnings(HybridArmingCtx *ctx, MRReply *warnings) {
  if (!warnings) {
    return;
  }
  for (size_t i = 0; i < MRReply_Length(warnings); i++) {
    MRReply *warning = MRReply_ArrayElement(warnings, i);
    MRIterator_PushReply(ctx->searchIt, MRReply_Clone(warning));
    MRIterator_PushReply(ctx->vsimIt, MRReply_Clone(warning));
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

// Whole-fan-out failure: pre-fanout connection validation failed (no shard was
// sent anything), or the three iterators disagree on the shard count (topology
// changed between their expansion jobs). Retire every placeholder on both read
// iterators and surface one error per stream. One-shot via `readsFailed`.
static void failAllReads(HybridArmingCtx *ctx, const char *msg) {
  ctx->readsFailed = true;
  MRIterator *its[2] = {ctx->searchIt, ctx->vsimIt};
  for (int j = 0; j < 2; j++) {
    MRIterator_PushReply(its[j], MRReply_CreateError(msg, strlen(msg)));
    const size_t n = MRIterator_GetNumShards(its[j]);
    for (size_t i = 0; i < n; i++) {
      MRIterator_ResolveShard(its[j], i, 1);
    }
  }
}

// True when this fan-out reply may be resolved per shard: nobody already
// retired all placeholders, and the three iterators agree on the shard count
// so `targetShardIdx` addresses the same shard everywhere.
static bool shardsAligned(const HybridArmingCtx *ctx, const MRIterator *hybridIt) {
  const size_t n = MRIterator_GetNumShards((MRIterator *)hybridIt);
  return n == MRIterator_GetNumShards(ctx->searchIt) && n == MRIterator_GetNumShards(ctx->vsimIt);
}

// Parse a RESP3 cursor-mapping reply: {"SEARCH": <cid>, "VSIM": <cid>, "warnings": [...]}
static void processHybridResp3(HybridArmingCtx *ctx, MRReply *rep, uint16_t shardIdx) {
  RS_ASSERT(MRReply_Length(rep) == INTERNAL_HYBRID_RESP3_LENGTH);
  forwardWarnings(ctx, MRReply_MapElement(rep, "warnings"));
  long long searchCid = 0, vsimCid = 0;
  MRReply *searchReply = MRReply_MapElement(rep, "SEARCH");
  MRReply *vsimReply = MRReply_MapElement(rep, "VSIM");
  RS_ASSERT(searchReply && vsimReply);
  MRReply_ToInteger(searchReply, &searchCid);
  MRReply_ToInteger(vsimReply, &vsimCid);
  armShardReads(ctx, shardIdx, searchCid, vsimCid);
}

// Parse a RESP2 cursor-mapping reply: ["SEARCH", <cid>, "VSIM", <cid>, "warnings", [...]]
static void processHybridResp2(HybridArmingCtx *ctx, MRReply *rep, uint16_t shardIdx) {
  RS_ASSERT(MRReply_Length(rep) == INTERNAL_HYBRID_RESP2_LENGTH);
  long long searchCid = 0, vsimCid = 0;
  for (size_t i = 0; i + 1 < MRReply_Length(rep); i += 2) {
    const char *key = MRReply_String(MRReply_ArrayElement(rep, i), NULL);
    MRReply *value = MRReply_ArrayElement(rep, i + 1);
    if (strcmp(key, "SEARCH") == 0) {
      MRReply_ToInteger(value, &searchCid);
    } else if (strcmp(key, "VSIM") == 0) {
      MRReply_ToInteger(value, &vsimCid);
    } else if (strcmp(key, "warnings") == 0) {
      forwardWarnings(ctx, value);
    }
  }
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
  MRIterator *hybridIt = MRIteratorCallback_GetIterator(ctx);
  const uint16_t shardIdx = MRIteratorCallback_GetCommand(ctx)->targetShardIdx;
  const int replyType = MRReply_Type(rep);
  bool isError = replyType == MR_REPLY_ERROR;

  if (cb_ctx->readsFailed) {
    // Placeholders were already retired wholesale; nothing left to resolve.
    // A late shard that still published cursors leaves them to the idle sweep.
  } else if (!shardsAligned(cb_ctx, hybridIt)) {
    // Pre-fanout connection validation failed (single synthetic error, the
    // fan-out was never expanded), or the expansion jobs saw different
    // topologies. Per-shard resolution is not addressable — fail everything.
    failAllReads(cb_ctx, isError ? MRReply_String(rep, NULL) : CLUSTER_QUERY_ERROR);
    isError = true;
  } else if (isError) {
    failShardReads(cb_ctx, shardIdx, rep);
  } else if (replyType == MR_REPLY_MAP) {
    processHybridResp3(cb_ctx, rep, shardIdx);
  } else if (replyType == MR_REPLY_ARRAY) {
    processHybridResp2(cb_ctx, rep, shardIdx);
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
  if (cb_ctx->readsFailed) {
    return;
  }
  MRIterator *hybridIt = MRIteratorCallback_GetIterator(ctx);
  if (!shardsAligned(cb_ctx, hybridIt)) {
    failAllReads(cb_ctx, CLUSTER_QUERY_ERROR);
    return;
  }
  const uint16_t shardIdx = MRIteratorCallback_GetCommand(ctx)->targetShardIdx;
  MRReply *error = MRReply_CreateError(CLUSTER_QUERY_ERROR, strlen(CLUSTER_QUERY_ERROR));
  failShardReads(cb_ctx, shardIdx, error);
  MRReply_Free(error);
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
