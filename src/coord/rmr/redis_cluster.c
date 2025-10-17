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

static void parseSlots(RedisModuleCallReply *slots, MRClusterShard *sh) {
  size_t len = RedisModule_CallReplyLength(slots);
  RS_ASSERT(len % 2 == 0);
  sh->numRanges = len / 2;
  sh->capRanges = sh->numRanges;
  sh->ranges = rm_malloc(sh->numRanges * sizeof(mr_slot_range_t));
  for (size_t r = 0; r < sh->numRanges; r++) {
    sh->ranges[r].start = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(slots, r * 2));
    sh->ranges[r].end = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(slots, r * 2 + 1));
  }
}

static void parseNode(RedisModuleCallReply *node, MRClusterNode *n) {
  const size_t len = RedisModule_CallReplyLength(node);
  RS_ASSERT(len % 2 == 0);
  n->endpoint.port = -1; // Use -1 to indicate "not set"

  for (size_t i = 0; i < len / 2; i++) {
    size_t key_len, val_len;
    RedisModuleCallReply *key = RedisModule_CallReplyArrayElement(node, i * 2);
    const char *key_str = RedisModule_CallReplyStringPtr(key, &key_len);
    RedisModuleCallReply *val = RedisModule_CallReplyArrayElement(node, i * 2 + 1);

    if (STR_EQ(key_str, key_len, "id")) {
      const char *val_str = RedisModule_CallReplyStringPtr(val, &val_len);
      n->id = rm_strndup(val_str, val_len);
    } else if (STR_EQ(key_str, key_len, "endpoint")) {
      const char *val_str = RedisModule_CallReplyStringPtr(val, &val_len);
      n->endpoint.host = rm_strndup(val_str, val_len);
    } else if (STR_EQ(key_str, key_len, "role")) {
      const char *val_str = RedisModule_CallReplyStringPtr(val, &val_len);
      if (STR_EQ(val_str, val_len, "master")) {
        n->flags |= MRNode_Master;
      }
    } else if (STR_EQ(key_str, key_len, "tls-port")) {
      n->endpoint.port = (int)RedisModule_CallReplyInteger(val); // Prefer tls-port if available
    } else if (STR_EQ(key_str, key_len, "port") && n->endpoint.port == -1) {
      n->endpoint.port = (int)RedisModule_CallReplyInteger(val); // Only set if tls-port wasn't set
    }
  }
  // Basic sanity - verify we have the required fields
  RS_ASSERT(n->id != NULL);
  RS_ASSERT(n->endpoint.host != NULL);
  RS_ASSERT(n->endpoint.port != -1);
}

static bool hasSlots(RedisModuleCallReply *shard) {
  ASSERT_KEY(shard, 0, "slots");
  RedisModuleCallReply *slots = RedisModule_CallReplyArrayElement(shard, 1);
  RS_ASSERT(RedisModule_CallReplyType(slots) == REDISMODULE_REPLY_ARRAY);
  return RedisModule_CallReplyLength(slots) > 0;
}

// Sort shards by the port of their first node
static void sortShards(MRClusterTopology *topo) {
  // Simple insertion sort, we don't expect many shards
  for (size_t i = 1; i < topo->numShards; i++) {
    MRClusterShard key = topo->shards[i];
    size_t j = i;
    while (j > 0 && topo->shards[j - 1].nodes[0].endpoint.port > key.nodes[0].endpoint.port) {
      topo->shards[j] = topo->shards[j - 1];
      j--;
    }
    topo->shards[j] = key;
  }
}

static MRClusterTopology *RedisCluster_GetTopology(RedisModuleCtx *ctx) {
  RS_AutoMemory(ctx);
  RedisModuleCallReply *myID_reply = RedisModule_Call(ctx, "CLUSTER", "c", "MYID");
  if (myID_reply == NULL || RedisModule_CallReplyType(myID_reply) != REDISMODULE_REPLY_STRING) {
    RedisModule_Log(ctx, "warning", "Error calling CLUSTER MYID");
    return NULL;
  }

  size_t idlen;
  const char *myID = RedisModule_CallReplyStringPtr(myID_reply, &idlen);

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
    RedisModule_Log(ctx, "warning", "Got no slots in CLUSTER SHARDS");
    return NULL;
  }
  MRClusterTopology *topo = rm_calloc(1, sizeof(MRClusterTopology));
  topo->hashFunc = MRHashFunc_CRC16;

  topo->numSlots = 16384;
  topo->numShards = numShards;
  topo->shards = rm_calloc(numShards, sizeof(MRClusterShard));

  // iterate each nested array
  for (size_t i = 0; i < numShards; i++) {
    RedisModuleCallReply *currShard = RedisModule_CallReplyArrayElement(cluster_shards, i);
    RS_ASSERT(RedisModule_CallReplyType(currShard) == REDISMODULE_REPLY_ARRAY);
    RS_ASSERT(RedisModule_CallReplyLength(currShard) == 4); // We expect 4 elements: "slots", <array>, "nodes", <array>

    // Handle slots
    ASSERT_KEY(currShard, 0, "slots");
    parseSlots(RedisModule_CallReplyArrayElement(currShard, 1), &topo->shards[i]);

    // Handle nodes
    ASSERT_KEY(currShard, 2, "nodes");
    RedisModuleCallReply *nodes = RedisModule_CallReplyArrayElement(currShard, 3);
    RS_ASSERT(RedisModule_CallReplyType(nodes) == REDISMODULE_REPLY_ARRAY);
    topo->shards[i].capNodes = RedisModule_CallReplyLength(nodes);
    topo->shards[i].numNodes = topo->shards[i].capNodes; // we expect all nodes to be valid
    topo->shards[i].nodes = rm_calloc(topo->shards[i].capNodes, sizeof(MRClusterNode));
    // parse each node
    for (size_t n = 0; n < topo->shards[i].numNodes; n++) {
      parseNode(RedisModule_CallReplyArrayElement(nodes, n), &topo->shards[i].nodes[n]);
      // Mark the node as self if its ID matches our ID
      if (STR_EQ(myID, idlen, topo->shards[i].nodes[n].id)) {
        topo->shards[i].nodes[n].flags |= MRNode_Self;
      }
    }
    // Make sure the first node is the master
    if (!(topo->shards[i].nodes[0].flags & MRNode_Master)) {
      for (size_t n = 1; n < topo->shards[i].numNodes; n++) {
        if (topo->shards[i].nodes[n].flags & MRNode_Master) {
          MRClusterNode tmp = topo->shards[i].nodes[n];
          topo->shards[i].nodes[n] = topo->shards[i].nodes[0];
          topo->shards[i].nodes[0] = tmp;
          break;
        }
      }
    }
    RS_ASSERT(topo->shards[i].nodes[0].flags & MRNode_Master);
  }

  // Sort shards by the port of their first node (master), to have a stable order
  sortShards(topo);
  return topo;
}

extern size_t NumShards;
void UpdateTopology(RedisModuleCtx *ctx) {
  MRClusterTopology *topo = RedisCluster_GetTopology(ctx);
  if (topo) { // if we didn't get a topology, do nothing. Log was already printed
    RedisModule_Log(ctx, "debug", "UpdateTopology: Setting number of partitions to %ld", topo->numShards);
    NumShards = topo->numShards;
    MR_UpdateTopology(topo);
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
