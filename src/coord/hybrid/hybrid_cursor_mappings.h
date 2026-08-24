/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "rmr/rmr.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Context for SHARD_K_RATIO optimization in FT.HYBRID commands.
 * Contains information needed to calculate and apply effectiveK.
 */
typedef struct {
  size_t originalK;         // Original K value from query
  double shardWindowRatio;  // Ratio for shard window optimization
  int kArgIndex;            // Index of the K value argument in the MRCommand
} HybridKnnContext;

/**
 * Shared state of the FT.HYBRID arming fan-out (the `cbPrivateData` of the
 * `_FT.HYBRID` iterator). The fan-out's reply callback arms and dispatches
 * each shard's cursor-read placeholders on the two sibling read iterators, so
 * every published shard cursor id lives in a live iterator command from the
 * moment it is known — cleanup on abort is then the standard rmr teardown
 * (DEL-swap / timed-out arming), with no coordinator-side cursor bookkeeping.
 *
 * All fields are touched only on the iterators' shared IO thread. The sibling
 * iterators are safe to dereference without references of our own: each keeps
 * its writers' reference until every placeholder is armed or resolved, which
 * only this fan-out's callbacks do.
 */
typedef struct {
  MRIterator *searchIt;
  MRIterator *vsimIt;
  HybridKnnContext *knnCtx;  // KNN context for SHARD_K_RATIO optimization (may be NULL)
} HybridArmingCtx;

/** Destructor for HybridArmingCtx (the fan-out iterator's cbPrivateDataDestructor). */
void HybridArmingCtx_Free(void *p);

/**
 * Per-reply callback of the `_FT.HYBRID` arming fan-out. Parses the shard's
 * cursor mapping, forwards mapping-stage warnings into both read streams, and
 * arms (or resolves) this shard's placeholder on each read iterator. Shard
 * errors are cloned into both read streams, where rpnetNext applies the
 * timeout/OOM policies.
 */
void hybridArmingCallback(MRIteratorCallbackCtx *ctx, MRReply *rep);

/**
 * No-reply counterpart of hybridArmingCallback (dead connection mid-flight):
 * surfaces a cluster error into both read streams and resolves the shard's
 * placeholders. Notify-only per the MRIteratorErrorCallback contract.
 */
void hybridArmingErrorCallback(MRIteratorCallbackCtx *ctx);

/**
 * Start callback of the `_FT.HYBRID` arming fan-out: validates shard
 * connections, expands both sibling read iterators' placeholders, and
 * dispatches the fan-out — all within one scheduled IO job, i.e. one topology
 * snapshot, so the three iterators cannot observe different shard counts and
 * a per-shard index addresses the same shard on all of them.
 */
void hybridArmingStartCb(void *p);

/**
 * Apply SHARD_K_RATIO optimization to an MRCommand based on the provided
 * HybridKnnContext. Computes the effective per-shard K and rewrites the K
 * argument in the command in-place. No-op for single-shard deployments or
 * when the ratio disables the optimization.
 *
 * Exposed primarily as the inner logic of HybridKnnCommandModifier so that
 * it can be unit-tested without replicating the callback context layout.
 */
void HybridKnnApplyShardKRatio(MRCommand *cmd, size_t numShards, const HybridKnnContext *knnCtx);

/**
 * Command modifier for the arming fan-out (privateData is the HybridArmingCtx).
 * Called from iterStartCb on the IO thread before commands are sent to shards.
 */
void HybridKnnCommandModifier(MRCommand *cmd, size_t numShards, void *privateData);

#ifdef __cplusplus
}
#endif
