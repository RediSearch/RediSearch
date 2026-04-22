/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include "endpoint.h"
#include "node.h"
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A "shard" represents a slot set of the cluster, with its associated node (we keep a single node per shard) */
typedef struct {
  MRClusterNode node;
  RedisModuleSlotRangeArray *slotRanges;
} MRClusterShard;

/* Create a new cluster shard to be added to a topology */
MRClusterShard MR_NewClusterShard(MRClusterNode *node, RedisModuleSlotRangeArray *slotRanges);

/* A topology is the mapping of slots to shards and nodes
 * Currently, the shards order is arbitrary, and may also change when the topology refreshed,
 * even if the actual mapping didn't change.
 */
typedef struct MRClusterTopology {
  uint32_t numShards;
  uint32_t capShards;
  MRClusterShard *shards;
} MRClusterTopology;

MRClusterTopology *MR_NewTopology(uint32_t numShards);
void MRClusterTopology_AddShard(MRClusterTopology *topo, MRClusterShard *sh);

/**
 * Frees resources held by an MRClusterNode.
 *
 * This function releases any memory and resources associated with the given MRClusterNode,
 * including its endpoint and node ID string. It does not free the MRClusterNode struct itself
 * (if it was stack-allocated), only its internal allocations.
 *
 * Usage:
 *   - Call this function when you are done using an MRClusterNode and want to release its resources.
 *   - The pointer 'n' must be valid and must not have already been freed.
 *   - Do not use the MRClusterNode after calling this function unless it is re-initialized.
 */
void MRClusterNode_Free(MRClusterNode *n);

void MRClusterTopology_Free(MRClusterTopology *t);

MRClusterTopology *MRClusterTopology_Clone(MRClusterTopology *t);

/* Return true if `a` and `b` describe the same set of shards with identical
 * connectivity (node id, host, port). Slot range differences are ignored.
 * Shard order is ignored, as the topology provider does not guarantee a stable
 * order across refreshes. Two NULL topologies are treated as equal.
 *
 * This predicate is used to decide whether a topology refresh requires
 * re-running the connection validation handshake: only connectivity changes do.
 * Slot-range-only updates still need the topology pointer to be refreshed so
 * the fanout path annotates commands with current slot info, but they do not
 * require reconnecting or rearming the validation timers. */
bool MRClusterTopology_ConnectivityEqual(const MRClusterTopology *a, const MRClusterTopology *b);

#ifdef __cplusplus
}
#endif
