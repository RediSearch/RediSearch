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
#include "rmutil/rm_assert.h"

MRClusterShard MR_NewClusterShard(MRClusterNode *node, RedisModuleSlotRangeArray *slotRanges) {
  MRClusterShard ret = (MRClusterShard){
      .node = *node,
      .slotRanges = slotRanges,
  };
  return ret;
}

void MRClusterShard_Free(MRClusterShard *sh) {
  MRClusterNode_Free(&sh->node);
  rm_free(sh->slotRanges);
}

MRClusterTopology *MR_NewTopology(uint32_t numShards) {
  MRClusterTopology *topo = rm_new(MRClusterTopology);
  topo->numShards = 0;
  topo->capShards = numShards;
  topo->shards = rm_calloc(topo->capShards, sizeof(MRClusterShard));
  return topo;
}

void MRClusterTopology_AddShard(MRClusterTopology *topo, MRClusterShard *sh) {
  RS_LOG_ASSERT(topo->numShards < topo->capShards, "Expected to have enough capacity for all shards");
  topo->shards[topo->numShards++] = *sh;
}

MRClusterTopology *MRClusterTopology_Clone(MRClusterTopology *t) {
  if (!t) {
    return NULL;
  }
  MRClusterTopology *topo = MR_NewTopology(t->numShards);
  for (int s = 0; s < t->numShards; s++) {
    MRClusterShard *original_shard = &t->shards[s];
    RedisModuleSlotRangeArray *slot_ranges = NULL;
    if (original_shard->slotRanges != NULL) {
      size_t total_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * original_shard->slotRanges->num_ranges;
      slot_ranges = (RedisModuleSlotRangeArray*)rm_malloc(total_size);
      memcpy(slot_ranges, original_shard->slotRanges, total_size);
    }
    MRClusterShard new_shard = MR_NewClusterShard(&original_shard->node, slot_ranges);

    new_shard.node.id = rm_strdup(original_shard->node.id);
    MREndpoint_Copy(&new_shard.node.endpoint, &original_shard->node.endpoint);
    new_shard.node.endpoint.port = original_shard->node.endpoint.port;

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
    MRClusterShard_Free(&t->shards[s]);
  }
  rm_free(t->shards);
  rm_free(t);
}
