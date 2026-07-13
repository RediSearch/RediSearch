/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "../config.h"
#include "cluster.h"
#include "redismodule.h"
#include "rmr.h"
#include "module.h"

#include <stdbool.h>

void UpdateTopology(RedisModuleCtx *ctx) {
  uint32_t my_shard_idx = UINT32_MAX;
  MRClusterTopology *topo = MRClusterTopology_FromAPI(ctx, NULL, 0, &my_shard_idx);
  if (!topo) {
    // If we didn't get a topology, do nothing. MRClusterTopology_FromAPI logged the reason.
    return;
  }

  static const RedisModuleSlotRangeArray empty_slots = {0, {{0, 0}}};
  const RedisModuleSlotRangeArray *local_slots = &empty_slots;
  RedisModuleSlotRangeArray *local_slots_from_api = NULL;
  if (my_shard_idx != UINT32_MAX) {
    RS_ASSERT(my_shard_idx < topo->numShards);
    MR_SetLocalNodeId(topo->shards[my_shard_idx].node.id);
    local_slots = topo->shards[my_shard_idx].slotRanges;
  } else {
    // Valid topology, but this node is not part of it (e.g. a replica or slot-less master).
    MR_SetLocalNodeId(RedisModule_GetMyClusterID());
    local_slots_from_api = RedisModule_ClusterGetLocalSlotRanges(ctx);
    if (local_slots_from_api) {
      local_slots = local_slots_from_api;
    }
  }

  MR_UpdateTopology(topo, local_slots);
  if (local_slots_from_api) {
    RedisModule_ClusterFreeSlotRanges(ctx, local_slots_from_api);
  }
}

// True while the topology updater is active: refreshing the topology on every cluster topology
// change event. Only set on an OSS cluster; may be cleared temporarily by the debug commands.
static bool topologyUpdaterRunning = false;

void RedisTopologyUpdater_OnTopologyChanged(RedisModuleCtx *ctx) {
  if (!topologyUpdaterRunning) return;  // Paused, or not an OSS cluster
  UpdateTopology(ctx);
}

int InitRedisTopologyUpdater(RedisModuleCtx *ctx) {
  if (topologyUpdaterRunning || clusterConfig.type != ClusterType_RedisOSS) return REDISMODULE_ERR;
  topologyUpdaterRunning = true;
  // The topology change event only fires on changes, so fetch the current topology ourselves.
  // This may fail if the cluster is not ready yet (e.g. at server startup); the events fired
  // while the cluster forms will trigger the next refresh.
  UpdateTopology(ctx);
  return REDISMODULE_OK;
}

int StopRedisTopologyUpdater(RedisModuleCtx *ctx) {
  REDISMODULE_NOT_USED(ctx);
  if (!topologyUpdaterRunning) {
    return REDISMODULE_ERR;  // Already stopped (or never started - enterprise)
  }
  topologyUpdaterRunning = false;
  return REDISMODULE_OK;
}
