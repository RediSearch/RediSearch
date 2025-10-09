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

  if (n->flags & MRNode_Master && sh->numNodes > 0) {
    // Ensure that master nodes are always at the front of the list
    sh->nodes[sh->numNodes] = sh->nodes[0];
    sh->nodes[0] = *n;
  } else {
    // Otherwise, just append
    sh->nodes[sh->numNodes] = *n;
  }
  sh->numNodes++;
}

MRClusterShard MR_NewClusterShard() {
  MRClusterShard ret = (MRClusterShard){
      .numNodes = 0,
      .capNodes = 1,
      .nodes = rm_calloc(1, sizeof(MRClusterNode)),
  };
  return ret;
}

MRClusterTopology *MR_NewTopology(size_t numShards, size_t numSlots) {
  MRClusterTopology *topo = rm_new(MRClusterTopology);
  topo->numSlots = numSlots;
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
  MRClusterTopology *topo = MR_NewTopology(t->numShards, t->numSlots);
  for (int s = 0; s < t->numShards; s++) {
    MRClusterShard *original_shard = &t->shards[s];
    MRClusterShard new_shard = MR_NewClusterShard();
    if (new_shard.capNodes < original_shard->numNodes) {
      new_shard.capNodes = original_shard->numNodes;
      rm_free(new_shard.nodes);
      new_shard.nodes = rm_calloc(new_shard.capNodes, sizeof(MRClusterNode));
    }

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
