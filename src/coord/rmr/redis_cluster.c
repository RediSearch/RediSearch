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
#include "util/strconv.h"
#include "slot_ranges.h"

#ifndef ENABLE_ASSERT
#define ASSERT_KEY(reply, idx, expected)
#else
#define ASSERT_KEY(reply, idx, expected)                                        \
  do {                                                                          \
    RedisModuleCallReply *key = RedisModule_CallReplyArrayElement(reply, idx);  \
    RS_ASSERT(RedisModule_CallReplyType(key) == REDISMODULE_REPLY_STRING);      \
    size_t key_len;                                                             \
    const char *key_str = RedisModule_CallReplyStringPtr(key, &key_len);        \
    RS_ASSERT(STR_EQ(key_str, key_len, expected));                              \
  } while (0)
#endif

static bool parseNode(RedisModuleCallReply *node, MRClusterNode *n) {
  const size_t len = RedisModule_CallReplyLength(node);
  RS_ASSERT(len % 2 == 0);
  char *id = NULL;
  char *host = NULL;
  int port = 0; // Use 0 to indicate "not set"

  for (size_t i = 0; i < len / 2; i++) {
    size_t key_len, val_len;
    RedisModuleCallReply *key = RedisModule_CallReplyArrayElement(node, i * 2);
    const char *key_str = RedisModule_CallReplyStringPtr(key, &key_len);
    RedisModuleCallReply *val = RedisModule_CallReplyArrayElement(node, i * 2 + 1);

    if (STR_EQ(key_str, key_len, "id")) {
      const char *val_str = RedisModule_CallReplyStringPtr(val, &val_len);
      if (val_str && val_len == REDISMODULE_NODE_ID_LEN) {
        id = rm_strndup(val_str, REDISMODULE_NODE_ID_LEN);
      }
    } else if (STR_EQ(key_str, key_len, "endpoint")) {
      const char *val_str = RedisModule_CallReplyStringPtr(val, &val_len);
      if (val_str && !STR_EQ(val_str, val_len, "") && !STR_EQ(val_str, val_len, "?")) {
        host = rm_strndup(val_str, val_len);
      }
    } else if (STR_EQ(key_str, key_len, "tls-port")) {
      int port_val = (int)RedisModule_CallReplyInteger(val);
      if (port_val > 0) {
        port = port_val; // Prefer tls-port if available (but only if valid)
      }
    } else if (STR_EQ(key_str, key_len, "port") && port == 0) {
      int port_val = (int)RedisModule_CallReplyInteger(val);
      if (port_val > 0) {
        port = port_val; // Only set if tls-port wasn't set and port is valid
      }
    }
  }
  // Verify we have the required fields
  if (id && host && port > 0) {
    n->id = id;
    n->endpoint.host = host;
    n->endpoint.port = port;
    return true;
  }

  // Invalid node. Cleanup and return `false`
  rm_free(id);
  rm_free(host);
  return false;
}

static bool parseMasterNode(RedisModuleCallReply *nodes, MRClusterNode *n) {
  const size_t numNodes = RedisModule_CallReplyLength(nodes);

  for (size_t i = 0; i < numNodes; i++) {
    RedisModuleCallReply *node = RedisModule_CallReplyArrayElement(nodes, i);
    RS_ASSERT(RedisModule_CallReplyType(node) == REDISMODULE_REPLY_ARRAY);
    size_t node_len = RedisModule_CallReplyLength(node);
    RS_ASSERT(node_len % 2 == 0);

    // Find the "role" key
    size_t j = 0;
    for (; j < node_len; j += 2) {
      RedisModuleCallReply *key = RedisModule_CallReplyArrayElement(node, j);
      RS_ASSERT(RedisModule_CallReplyType(key) == REDISMODULE_REPLY_STRING);
      size_t key_len;
      const char *key_str = RedisModule_CallReplyStringPtr(key, &key_len);
      if (STR_EQ(key_str, key_len, "role")) {
        break;
      }
    }
    // Check if this is a master node
    ASSERT_KEY(node, j, "role");
    RedisModuleCallReply *val = RedisModule_CallReplyArrayElement(node, j + 1);
    size_t val_len;
    const char *val_str = RedisModule_CallReplyStringPtr(val, &val_len);
    if (STR_EQ(val_str, val_len, "master")) {
      if (parseNode(node, n)) {
        return true;
      }
    }
  }
  return false;
}

static bool parseSlots(RedisModuleCallReply *slots, MRClusterShard *sh) {
  size_t len = RedisModule_CallReplyLength(slots);
  RS_ASSERT(len % 2 == 0);
  if (!len) return false;
  size_t buffer_size = SlotRangeArray_SizeOf(len / 2);
  sh->slotRanges = rm_malloc(buffer_size);
  sh->slotRanges->num_ranges = (int32_t)(len / 2);
  for (size_t r = 0; r < sh->slotRanges->num_ranges; r++) {
    sh->slotRanges->ranges[r].start = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(slots, r * 2));
    sh->slotRanges->ranges[r].end = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(slots, r * 2 + 1));
  }
  return true;
}

static bool hasSlots(RedisModuleCallReply *shard) {
  ASSERT_KEY(shard, 0, "slots");
  RedisModuleCallReply *slots = RedisModule_CallReplyArrayElement(shard, 1);
  RS_ASSERT(RedisModule_CallReplyType(slots) == REDISMODULE_REPLY_ARRAY);
  return RedisModule_CallReplyLength(slots) > 0;
}

// Sort shards by the port of their node
// We want to sort by some node value and not by slots, as the nodes in the cluster may be
// stable while slots can migrate between them
static void sortShards(MRClusterTopology *topo) {
  // Simple insertion sort, we don't expect many shards
  for (size_t i = 1; i < topo->numShards; i++) {
    MRClusterShard key = topo->shards[i];
    size_t j = i;
    while (j > 0 && topo->shards[j - 1].node.endpoint.port > key.node.endpoint.port) {
      topo->shards[j] = topo->shards[j - 1];
      j--;
    }
    topo->shards[j] = key;
  }
}

// Assumes auto memory was enabled on ctx
static MRClusterTopology *RedisCluster_GetTopology(RedisModuleCtx *ctx) {

  RedisModuleCallReply *cluster_shards = RedisModule_Call(ctx, "CLUSTER", "c", "SHARDS");
  if (cluster_shards == NULL || RedisModule_CallReplyType(cluster_shards) != REDISMODULE_REPLY_ARRAY) {
    RedisModule_Log(ctx, "warning", "Error calling CLUSTER SHARDS");
    return NULL;
  }
  /*
1) 1# "slots" =>
      1) (integer) 0
      2) (integer) 4095
      3) (integer) 8192
      4) (integer) 12287
   2# "nodes" =>
      1)  1# "id" => "e10b7051d6bf2d5febd39a2be297bbaea6084111"
          2# "port" => (integer) 30001
          3# "tls-port" => (integer) 40001
          4# "ip" => "127.0.0.1"
          5# "endpoint" => "localhost"
          6# "role" => "master"
      2)  1# "id" => "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf"
          2# "port" => (integer) 30004
          3# "tls-port" => (integer) 40004
          4# "ip" => "127.0.0.1"
          5# "endpoint" => "localhost"
          6# "role" => "replica"
2) 1# "slots" =>
      1) (integer) 4096
      2) (integer) 8191
      3) (integer) 12288
      4) (integer) 16383
   2# "nodes" =>
      1)  1# "id" => "fd20502fe1b32fc32c15b69b0a9537551f162f1f"
          2# "port" => (integer) 30003
          3# "tls-port" => (integer) 40003
          4# "ip" => "127.0.0.1"
          5# "endpoint" => "localhost"
          6# "role" => "master"
      2)  1# "id" => "6daa25c08025a0c7e4cc0d1ab255949ce6cee902"
          2# "port" => (integer) 30005
          3# "tls-port" => (integer) 40005
          4# "ip" => "127.0.0.1"
          5# "endpoint" => "localhost"
          6# "role" => "replica"
  */

  size_t numShards = RedisModule_CallReplyLength(cluster_shards);
  if (numShards == 0 || (numShards == 1 && !hasSlots(RedisModule_CallReplyArrayElement(cluster_shards, 0)))) {
    RedisModule_Log(ctx, "warning", "Got no valid shards in CLUSTER SHARDS");
    return NULL;
  }

  MRClusterTopology *topo = rm_calloc(1, sizeof(MRClusterTopology));
  topo->shards = rm_calloc(numShards, sizeof(MRClusterShard));

  // Parse each shard, filter badly formatted or not ready shards
  // A shard is valid if:
  // 1. It has some slots associated with
  // 2. It has a valid master node with a valid endpoint:
  //    i. Valid node id
  //   ii. Non-zero port
  //  iii. Valid endpoint (not missing or a special invalid value)
  for (size_t i = 0; i < numShards; i++) {
    RedisModuleCallReply *currShard = RedisModule_CallReplyArrayElement(cluster_shards, i);
    RS_ASSERT(RedisModule_CallReplyType(currShard) == REDISMODULE_REPLY_ARRAY);
    RS_ASSERT(RedisModule_CallReplyLength(currShard) == 4); // We expect 4 elements: "slots", <array>, "nodes", <array>

    // Handle slots
    ASSERT_KEY(currShard, 0, "slots");
    RedisModuleCallReply *slots = RedisModule_CallReplyArrayElement(currShard, 1);
    if (!parseSlots(slots, &topo->shards[topo->numShards])) continue;

    // Handle nodes
    ASSERT_KEY(currShard, 2, "nodes");
    RedisModuleCallReply *nodes = RedisModule_CallReplyArrayElement(currShard, 3);
    RS_ASSERT(RedisModule_CallReplyType(nodes) == REDISMODULE_REPLY_ARRAY);
    // parse and store the master
    if (!parseMasterNode(nodes, &topo->shards[topo->numShards].node)) {
      rm_free(topo->shards[topo->numShards].slotRanges);
      continue;
    }
    // Successfully parsed this shard
    topo->numShards++;
  }

  if (!topo->numShards) {
    RedisModule_Log(ctx, "warning", "Got no valid shards in CLUSTER SHARDS");
    rm_free(topo->shards);
    rm_free(topo);
    return NULL;
  }

  // Sort shards by the port of their node (master), to have a stable order while the topology is stable
  sortShards(topo);
  return topo;
}

void UpdateTopology(RedisModuleCtx *ctx) {
  RS_AutoMemory(ctx);
  MRClusterTopology *topo = RedisCluster_GetTopology(ctx);
  if (topo) { // if we didn't get a topology, do nothing. Log was already printed
    // Pass the local slots info directly from the RedisModule API, as we enabled auto memory
    MR_UpdateTopology(topo, RedisModule_ClusterGetLocalSlotRanges(ctx));
    Slots_DropCachedLocalSlots(); // Local slots may have changed, drop the cache
  }
}

#define REFRESH_PERIOD 1000 // 1 second
RedisModuleTimerID topologyRefreshTimer = 0;

static void UpdateTopology_Periodic(RedisModuleCtx *ctx, void *p) {
  REDISMODULE_NOT_USED(p);
  topologyRefreshTimer = RedisModule_CreateTimer(ctx, REFRESH_PERIOD, UpdateTopology_Periodic, NULL);
  UpdateTopology(ctx);
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
