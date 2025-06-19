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

MRClusterShard MR_NewClusterShard(mr_slot_t startSlot, mr_slot_t endSlot, size_t capNodes) {
  MRClusterShard ret = (MRClusterShard){
      .startSlot = startSlot,
      .endSlot = endSlot,
      .capNodes = capNodes,
      .numNodes = 0,
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
  MRClusterTopology *topo = MR_NewTopology(t->numShards, t->numSlots, t->hashFunc);
  for (int s = 0; s < t->numShards; s++) {
    MRClusterShard *original_shard = &t->shards[s];
    MRClusterShard new_shard = MR_NewClusterShard(original_shard->startSlot, original_shard->endSlot, original_shard->numNodes);
    for (int n = 0; n < original_shard->numNodes; n++) {
      MRClusterNode *node = &original_shard->nodes[n];
      MRClusterShard_AddNode(&new_shard, node);
    }
    for (int n = 0; n < new_shard.numNodes; n++) {
      new_shard.nodes[n].id = rm_strdup(original_shard->nodes[n].id);
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
  rm_free((char *)n->id);
}

void MRClusterTopology_Free(MRClusterTopology *t) {
  for (int s = 0; s < t->numShards; s++) {
    for (int n = 0; n < t->shards[s].numNodes; n++) {
      MRClusterNode_Free(&t->shards[s].nodes[n]);
    }
    rm_free(t->shards[s].nodes);
  }
  rm_free(t->shards);
  rm_free(t);
}
