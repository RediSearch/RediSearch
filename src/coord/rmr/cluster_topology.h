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

#ifdef __cplusplus
extern "C" {
#endif

/* A "shard" represents a slot set of the cluster, with its associated nodes */
typedef struct {
  uint32_t numNodes;
  uint32_t capNodes;
  MRClusterNode *nodes;
} MRClusterShard;

/* Create a new cluster shard to be added to a topology */
MRClusterShard MR_NewClusterShard(void);
void MRClusterShard_AddNode(MRClusterShard *sh, MRClusterNode *n);

#define MRHASHFUNC_CRC12_STR "CRC12"
#define MRHASHFUNC_CRC16_STR "CRC16"

/* A topology is the mapping of slots to shards and nodes */
typedef struct MRClusterTopology {
  size_t numSlots;
  size_t numShards;
  size_t capShards;
  MRClusterShard *shards;
} MRClusterTopology;

MRClusterTopology *MR_NewTopology(size_t numShards, size_t numSlots);
void MRClusterTopology_AddShard(MRClusterTopology *topo, MRClusterShard *sh);

void MRClusterTopology_Free(MRClusterTopology *t);

void MRClusterNode_Free(MRClusterNode *n);

MRClusterTopology *MRClusterTopology_Clone(MRClusterTopology *t);

#ifdef __cplusplus
}
#endif
