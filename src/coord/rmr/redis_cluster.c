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

#define REFRESH_PERIOD 1000 // 1 second
RedisModuleTimerID topologyRefreshTimer = 0;

static void UpdateTopology_Periodic(RedisModuleCtx *ctx, void *p) {
  REDISMODULE_NOT_USED(p);
  topologyRefreshTimer = RedisModule_CreateTimer(ctx, REFRESH_PERIOD, UpdateTopology_Periodic, NULL);
  UpdateTopology(ctx);
}

void RedisTopologyUpdater_StopAndRescheduleImmediately(RedisModuleCtx *ctx) {
  RedisModule_StopTimer(ctx, topologyRefreshTimer, NULL);
  topologyRefreshTimer = RedisModule_CreateTimer(ctx, 0, UpdateTopology_Periodic, NULL);
}

int InitRedisTopologyUpdater(RedisModuleCtx *ctx) {
  if (topologyRefreshTimer || clusterConfig.type != ClusterType_RedisOSS) return REDISMODULE_ERR;
  topologyRefreshTimer = RedisModule_CreateTimer(ctx, REFRESH_PERIOD, UpdateTopology_Periodic, NULL);
  return REDISMODULE_OK;
}

int StopRedisTopologyUpdater(RedisModuleCtx *ctx) {
  int rc = RedisModule_StopTimer(ctx, topologyRefreshTimer, NULL);
  topologyRefreshTimer = 0;
  return rc; // OK if we stopped the timer, ERR if it was already stopped (or never started - enterprise)
}
