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

// Flattened element count of a shard's cursor-mapping reply (3 key/value
// pairs); identical in both protocols. See processHybridMapping.
#define HYBRID_MAPPING_REPLY_LENGTH 6

void HybridArmingCtx_Free(void *p) {
  HybridArmingCtx *ctx = (HybridArmingCtx *)p;
  rm_free(ctx->knnCtx);
  rm_free(ctx);
}

static inline MRReply *clusterQueryErrorReply(void) {
  return MRReply_CreateError(CLUSTER_QUERY_ERROR, strlen(CLUSTER_QUERY_ERROR));
}

// Forward mapping-stage warnings into the read streams, where each subquery's
// RPNet folds them (see processHybridMappingWarning in rpnet.c). Pushed one
// bare string reply per warning — a top-level string is unambiguous in a read
// stream, whose other replies are arrays or errors. Max-prefix warnings are
// suffix-tagged with the subquery they belong to and routed to that stream
// alone; the rest (timeout / shard OOM) are whole-shard conditions and go to
// both. Each channel's reader frees its replies independently, so the two
// streams cannot share one reply — the second stream gets a clone.
static void forwardWarnings(HybridArmingCtx *ctx, MRReply *warnings) {
  for (size_t i = 0; i < MRReply_Length(warnings); i++) {
    MRReply *warning = MRReply_TakeArrayElement(warnings, i);
    const char *warning_str = MRReply_String(warning, NULL);
    if (!strncmp(warning_str, QUERY_WMAXPREFIXEXPANSIONS, strlen(QUERY_WMAXPREFIXEXPANSIONS))) {
      MRIterator *target = strstr(warning_str, VSIM_SUFFIX) ? ctx->vsimIt : ctx->searchIt;
      MRIterator_PushReply(target, warning);
      continue;
    }
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
    const bool searchTimedOut = MRIteratorCallback_GetTimedOut(MRIterator_GetCtx(ctx->searchIt));
    MRIterator_ArmShardCursorRead(ctx->searchIt, shardIdx, searchCid, partialPair || searchTimedOut);
  }
  if (vsimCid == 0) {
    MRIterator_ResolveShard(ctx->vsimIt, shardIdx, 0);
  } else {
    const bool vsimTimedOut = MRIteratorCallback_GetTimedOut(MRIterator_GetCtx(ctx->vsimIt));
    MRIterator_ArmShardCursorRead(ctx->vsimIt, shardIdx, vsimCid, partialPair || vsimTimedOut);
  }
}

// Surface a shard-level failure to both read streams and retire the shard's
// placeholders. The error is pushed before the placeholders are resolved so
// the resolving side's channel unblock cannot beat the error into the reader.
// Consumes the error reply.
static void failShardReads(HybridArmingCtx *ctx, uint16_t shardIdx, MRReply *error) {
  MRIterator_PushReply(ctx->searchIt, MRReply_Clone(error));
  MRIterator_PushReply(ctx->vsimIt, error);
  MRIterator_ResolveShard(ctx->searchIt, shardIdx, 1);
  MRIterator_ResolveShard(ctx->vsimIt, shardIdx, 1);
}

// Whole-fan-out failure before anything was dispatched (pre-fanout connection
// validation failed, see hybridArmingStartCb): every iterator still holds its
// single initial placeholder — surface one error per read stream and retire
// those placeholders. No shard was sent anything, so no callback ever fires.
static void failReadsBeforeExpansion(HybridArmingCtx *ctx) {
  MRIterator *its[2] = {ctx->searchIt, ctx->vsimIt};
  for (int j = 0; j < 2; j++) {
    RS_ASSERT(MRIterator_GetNumShards(its[j]) == 1);
    MRIterator_PushReply(its[j], clusterQueryErrorReply());
    MRIterator_ResolveShard(its[j], 0, 1);
  }
}

// Parse a shard's cursor-mapping reply. Both protocols carry the same fixed
// layout — a RESP2 array or a RESP3 map, stored either way as a flat element
// array: ["SEARCH", <cid>, "VSIM", <cid>, "warnings", [...]] (the emission
// order of replyWithCursors in hybrid_exec.c). The structure is asserted in
// debug builds; production extracts the values by offset.
// Consumes the reply.
static void processHybridMapping(HybridArmingCtx *ctx, MRReply *rep, uint16_t shardIdx) {
  // Quietly read any unexpected layout as "shard published no cursors" —
  // e.g. a profile-wrapped envelope from a shard build that still wrapped its
  // early-bail reply. Both of that shard's streams end empty; nothing leaks
  // (it published no cursor ids to begin with).
  long long searchCid = 0, vsimCid = 0;
  RS_ASSERT(MRReply_Type(rep) == MR_REPLY_ARRAY || MRReply_Type(rep) == MR_REPLY_MAP);
  RS_ASSERT(MRReply_Length(rep) == HYBRID_MAPPING_REPLY_LENGTH);
  RS_ASSERT(MRReply_StringEquals(MRReply_ArrayElement(rep, 0), "SEARCH", true));
  RS_ASSERT(MRReply_StringEquals(MRReply_ArrayElement(rep, 2), "VSIM", true));
  RS_ASSERT(MRReply_StringEquals(MRReply_ArrayElement(rep, 4), "warnings", true));
  if (MRReply_Length(rep) == HYBRID_MAPPING_REPLY_LENGTH) {
    MRReply_ToInteger(MRReply_ArrayElement(rep, 1), &searchCid);
    MRReply_ToInteger(MRReply_ArrayElement(rep, 3), &vsimCid);
    forwardWarnings(ctx, MRReply_ArrayElement(rep, 5));
  }
  armShardReads(ctx, shardIdx, searchCid, vsimCid);
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
  IORuntimeCtx_RequestStarted(ioRuntime);
  IORuntimeCtx_RequestStarted(ioRuntime);
  // Validate connections before expanding anything, so a failure retires
  // exactly one placeholder per iterator. iterStartCb re-validates internally,
  // but connection state only changes via jobs on this same IO loop, so its
  // check cannot disagree with this one.
  if (!MRIterator_AllShardsConnected(hybridIt)) {
    failReadsBeforeExpansion(ctx);
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
