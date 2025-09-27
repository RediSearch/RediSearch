/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "cluster_topology.h"
#include "endpoint.h"
#include "rmalloc.h"

void MRClusterShard_AddNode(MRClusterShard *sh, MRClusterNode *n) {
  if (sh->capNodes == sh->numNodes) {
    sh->capNodes += 1;
    sh->nodes = rm_realloc(sh->nodes, sh->capNodes * sizeof(MRClusterNode));
  }
  sh->nodes[sh->numNodes++] = *n;
}


void MRClusterShard_AddRange(MRClusterShard *sh, mr_slot_t start, mr_slot_t end) {
  if (sh->capRanges == sh->numRanges) {
    sh->capRanges++;
    sh->ranges = rm_realloc(sh->ranges, sh->capRanges * sizeof(mr_slot_range_t));
  }
  sh->ranges[sh->numRanges].start = start;
  sh->ranges[sh->numRanges].end = end;
  sh->numRanges++;
}

RedisModuleString *MRClusterShard_HoldMasterID(MRClusterShard *sh) {
  for (int i = 0; i < sh->numNodes; i++) {
    if (sh->nodes[i].flags & MRNode_Master) {
      return RedisModule_HoldString(NULL, sh->nodes[i].id);
    }
  }
  return NULL;
}

MRClusterShard MR_NewClusterShard(mr_slot_t startSlot, mr_slot_t endSlot, uint32_t capNodes) {
  mr_slot_range_t *ranges = rm_malloc(sizeof(mr_slot_range_t));
  ranges[0].start = startSlot;
  ranges[0].end = endSlot;
  MRClusterShard ret = (MRClusterShard){
      .numRanges = 1,
      .capRanges = 1,
      .ranges = ranges,
      .numNodes = 0,
      .capNodes = capNodes,
      .nodes = rm_calloc(capNodes, sizeof(MRClusterNode)),
  };
  return ret;
}

MRClusterTopology *MR_NewTopology(size_t numShards, size_t numSlots, MRHashFunc hashFunc) {
  MRClusterTopology *topo = rm_new(MRClusterTopology);
  topo->numSlots = numSlots;
  topo->hashFunc = hashFunc;
  topo->numShards = 0;
  topo->capShards = numShards;
  topo->shards = rm_calloc(topo->capShards, sizeof(MRClusterShard));
  return topo;
}

void MRClusterTopology_AddShard(MRClusterTopology *topo, MRClusterShard *sh) {
  if (topo->capShards == topo->numShards) {
    topo->capShards++;
    topo->shards = rm_realloc(topo->shards, topo->capShards * sizeof(MRClusterShard));
  }
  topo->shards[topo->numShards++] = *sh;
}

MRClusterTopology *MRClusterTopology_Clone(MRClusterTopology *t) {
  if (!t) {
    return NULL;
  }
  MRClusterTopology *topo = MR_NewTopology(t->numShards, t->numSlots, t->hashFunc);
  for (int s = 0; s < t->numShards; s++) {
    MRClusterShard *original_shard = &t->shards[s];
    MRClusterShard new_shard = MR_NewClusterShard(original_shard->ranges[0].start, original_shard->ranges[0].end, original_shard->numNodes);
    if (original_shard->numRanges > 1) {
      new_shard.capRanges = original_shard->numRanges;
      new_shard.ranges = rm_realloc(new_shard.ranges, new_shard.capRanges * sizeof(mr_slot_range_t));
      for (size_t r = 1; r < original_shard->numRanges; r++) {
        new_shard.ranges[r] = original_shard->ranges[r];
      }
      new_shard.numRanges = original_shard->numRanges;
    }
    for (int n = 0; n < original_shard->numNodes; n++) {
      MRClusterNode *node = &original_shard->nodes[n];
      MRClusterShard_AddNode(&new_shard, node);
    }
    for (int n = 0; n < new_shard.numNodes; n++) {
      // Take an  actual copy of the node ID string, as it's going to be handled by another thread (HoldString and FreeString are not thread-safe)
      new_shard.nodes[n].id = RedisModule_CreateStringFromString(NULL, original_shard->nodes[n].id);
      RedisModule_TrimStringAllocation(new_shard.nodes[n].id);
      MREndpoint_Copy(&new_shard.nodes[n].endpoint, &original_shard->nodes[n].endpoint);
      new_shard.nodes[n].endpoint.port = original_shard->nodes[n].endpoint.port;
      new_shard.nodes[n].flags = 0;
      new_shard.nodes[n].flags = original_shard->nodes[n].flags;
    }
    MRClusterTopology_AddShard(topo, &new_shard);
  }
  return topo;
}

void MRClusterNode_Free(MRClusterNode *n) {
  MREndpoint_Free(&n->endpoint);
  RedisModule_FreeString(NULL, n->id);
}

void MRClusterTopology_Free(MRClusterTopology *t) {
  for (int s = 0; s < t->numShards; s++) {
    for (int n = 0; n < t->shards[s].numNodes; n++) {
      MRClusterNode_Free(&t->shards[s].nodes[n]);
    }
    rm_free(t->shards[s].nodes);
    rm_free(t->shards[s].ranges);
  }
  rm_free(t->shards);
  rm_free(t);
}
