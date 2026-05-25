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

#include <arpa/inet.h>

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

MRClusterTopology *MRClusterTopology_FromAPI(RedisModuleCtx *ctx, const char *auth, size_t auth_len, uint32_t *my_shard_idx) {
  *my_shard_idx = UINT32_MAX;

  size_t numNodes = 0;
  char **node_ids = RedisModule_GetClusterNodesList(ctx, &numNodes);
  if (!node_ids || numNodes == 0) {
    if (node_ids) RedisModule_FreeClusterNodesList(node_ids);
    RedisModule_Log(ctx, "warning", "Failed to get cluster nodes list");
    return NULL;
  }

  bool saw_myself = false;

  // Topology can contain at most one entry per node; replicas and slot-less
  // masters will be skipped, so this is an upper bound on the final size.
  MRClusterTopology *topo = MR_NewTopology(numNodes);

  for (size_t i = 0; i < numNodes; i++) {
    const char *node_id = node_ids[i];

    char ip[INET6_ADDRSTRLEN] = {0};
    int port = 0;
    int flags = 0;
    if (RedisModule_GetClusterNodeInfo(ctx, node_id, ip, NULL, &port, &flags) != REDISMODULE_OK) {
      RedisModule_Log(ctx, "notice", "Failed to get info for cluster node `%.*s`", REDISMODULE_NODE_ID_LEN, node_id);
      continue;
    }

    if (flags & REDISMODULE_NODE_MYSELF) saw_myself = true;

    // Skip replicas, unreachable nodes, and nodes with no valid endpoint
    if (!(flags & REDISMODULE_NODE_MASTER) || port <= 0 || ip[0] == '\0') {
      continue;
    }

    // Fetch the slot ranges owned by this master node. Slot-less masters
    // (e.g. fresh nodes not yet assigned slots) are excluded from the topology.
    // The module API hands us ownership; we must explicitly free.
    RedisModuleSlotRangeArray *node_slots = RedisModule_GetClusterNodeSlotRanges(ctx, node_id);
    if (!node_slots || node_slots->num_ranges <= 0) {
      if (node_slots) RedisModule_ClusterFreeSlotRanges(ctx, node_slots);
      continue;
    }

    MRClusterNode node = {
      .id = rm_strndup(node_id, REDISMODULE_NODE_ID_LEN),
      .endpoint = (MREndpoint){
        .host = rm_strdup(ip),
        .port = port,
        .unixSock = NULL,
        .password = (auth && auth_len > 0) ? rm_strndup(auth, auth_len) : NULL,
      },
    };

    // The topology owns its slot range arrays and frees them with rm_free,
    // so we must clone the module-owned array before freeing it.
    RedisModuleSlotRangeArray *cloned_slots = SlotRangeArray_Clone(node_slots);
    RedisModule_ClusterFreeSlotRanges(ctx, node_slots);

    if (flags & REDISMODULE_NODE_MYSELF) *my_shard_idx = topo->numShards;
    MRClusterShard shard = MR_NewClusterShard(&node, cloned_slots);
    MRClusterTopology_AddShard(topo, &shard);
  }

  RedisModule_FreeClusterNodesList(node_ids);

  if (topo->numShards == 0) {
    RedisModule_Log(ctx, "warning", "Got no valid shards from cluster API");
    MRClusterTopology_Free(topo);
    return NULL;
  }

  // If we never saw ourselves in the node list, the topology is unusable for
  // this node. A known replica or slot-less master is fine and leaves
  // *my_shard_idx at UINT32_MAX (matching RedisEnterprise_ParseTopology).
  if (!saw_myself) {
    RedisModule_Log(ctx, "warning", "Local node not found in cluster nodes list");
    MRClusterTopology_Free(topo);
    return NULL;
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
