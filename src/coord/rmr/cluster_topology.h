/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

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

/* A topology is the mapping of slots to shards and nodes */
typedef struct MRClusterTopology {
  uint32_t numShards;
  uint32_t capShards;
  MRClusterShard *shards;
} MRClusterTopology;

MRClusterTopology *MR_NewTopology(uint32_t numShards);
void MRClusterTopology_AddShard(MRClusterTopology *topo, MRClusterShard *sh);

// Sort shards by the port of their node
// We want to sort by some node value and not by slots, as the nodes in the cluster may be
// stable while slots can migrate between them
void MRClusterTopology_SortShards(MRClusterTopology *topo);

void MRClusterNode_Free(MRClusterNode *n);

void MRClusterTopology_Free(MRClusterTopology *t);

MRClusterTopology *MRClusterTopology_Clone(MRClusterTopology *t);

#ifdef __cplusplus
}
#endif
