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

typedef uint16_t mr_slot_t;
typedef struct {
  mr_slot_t start;
  mr_slot_t end;
} mr_slot_range_t;

/* A "shard" represents a slot range of the cluster, with its associated nodes. For each sharding
 * key, we select the slot based on the hash function, and then look for the shard in the cluster's
 * shard array */
typedef struct {
  uint32_t numRanges;
  uint32_t capRanges;
  mr_slot_range_t *ranges;
  uint32_t numNodes;
  uint32_t capNodes;
  MRClusterNode *nodes;
} MRClusterShard;

/* Create a new cluster shard to be added to a topology */
MRClusterShard MR_NewClusterShard(uint32_t capNodes);
void MRClusterShard_AddNode(MRClusterShard *sh, MRClusterNode *n);
void MRClusterShard_AddRange(MRClusterShard *sh, mr_slot_t start, mr_slot_t end);

#define MRHASHFUNC_CRC12_STR "CRC12"
#define MRHASHFUNC_CRC16_STR "CRC16"

typedef enum {
  MRHashFunc_None = 0,
  MRHashFunc_CRC12,
  MRHashFunc_CRC16,
} MRHashFunc;

/* A topology is the mapping of slots to shards and nodes */
typedef struct MRClusterTopology {
  size_t numSlots;
  MRHashFunc hashFunc;
  size_t numShards;
  size_t capShards;
  MRClusterShard *shards;
} MRClusterTopology;

MRClusterTopology *MR_NewTopology(size_t numShards, size_t numSlots, MRHashFunc hashFunc);
void MRClusterTopology_AddShard(MRClusterTopology *topo, MRClusterShard *sh);

void MRClusterTopology_Free(MRClusterTopology *t);

void MRClusterNode_Free(MRClusterNode *n);

MRClusterTopology *MRClusterTopology_Clone(MRClusterTopology *t);

#ifdef __cplusplus
}
#endif
