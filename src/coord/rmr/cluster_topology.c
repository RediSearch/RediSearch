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
#include "slot_ranges.h"
#include "rmutil/rm_assert.h"

#include <string.h>

MRClusterShard MR_NewClusterShard(MRClusterNode *node, RedisModuleSlotRangeArray *slotRanges) {
  MRClusterShard ret = (MRClusterShard){
      .node = *node,
      .slotRanges = slotRanges,
  };
  return ret;
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

    RedisModuleSlotRangeArray *slot_ranges = SlotRangeArray_Clone(original_shard->slotRanges);
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

static void MRClusterShard_Free(MRClusterShard *sh) {
  MRClusterNode_Free(&sh->node);
  rm_free(sh->slotRanges);
}

void MRClusterTopology_Free(MRClusterTopology *t) {
  for (int s = 0; s < t->numShards; s++) {
    MRClusterShard_Free(&t->shards[s]);
  }
  rm_free(t->shards);
  rm_free(t);
}

static const MRClusterShard *findShardByNodeId(const MRClusterTopology *t, const char *id) {
  for (uint32_t i = 0; i < t->numShards; i++) {
    if (t->shards[i].node.id && strcmp(t->shards[i].node.id, id) == 0) {
      return &t->shards[i];
    }
  }
  return NULL;
}

bool MRClusterTopology_ConnectivityEqual(const MRClusterTopology *a, const MRClusterTopology *b) {
  if (a == b) return true;
  if (!a || !b) return false;
  if (a->numShards != b->numShards) return false;
  for (uint32_t i = 0; i < a->numShards; i++) {
    const MRClusterShard *sa = &a->shards[i];
    if (!sa->node.id) return false;
    const MRClusterShard *sb = findShardByNodeId(b, sa->node.id);
    if (!sb) return false;
    if (sa->node.endpoint.port != sb->node.endpoint.port) return false;
    if (!sa->node.endpoint.host || !sb->node.endpoint.host ||
        strcmp(sa->node.endpoint.host, sb->node.endpoint.host) != 0) {
      return false;
    }
  }
  return true;
}
