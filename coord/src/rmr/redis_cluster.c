/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "../config.h"
#include "cluster.h"
#include "conn.h"
#include "libuv/include/uv.h"
#include "redismodule.h"
#include "rmutil/periodic.h"
#include "version.h"

#define REDIS_CLUSTER_REFRESH_TIMEOUT 1000

MRClusterTopology *RedisCluster_GetTopology(RedisModuleCtx *ctx) {

  const char *myId = NULL;
  RedisModuleCallReply *r = RedisModule_Call(ctx, "CLUSTER", "c", "MYID");
  if (r == NULL || RedisModule_CallReplyType(r) != REDISMODULE_REPLY_STRING) {
    RedisModule_Log(ctx, "error", "Error calling CLUSTER MYID§");
    return NULL;
  }
  size_t idlen;
  myId = RedisModule_CallReplyStringPtr(r, &idlen);

  r = RedisModule_Call(ctx, "CLUSTER", "c", "SLOTS");
  if (r == NULL || RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ARRAY) {
    RedisModule_Log(ctx, "error", "Error calling CLUSTER SLOTS");
    return NULL;
  }

  /*1) 1) (integer) 0
   2) (integer) 5460
   3) 1) "127.0.0.1"
      2) (integer) 30001
      3) "09dbe9720cda62f7865eabc5fd8857c5d2678366"
   4) 1) "127.0.0.1"
      2) (integer) 30004
      3) "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf"*/

  size_t len = RedisModule_CallReplyLength(r);
  if (len < 1) {
    RedisModule_Log(ctx, "warning", "Got no slots in CLUSTER SLOTS");
    return NULL;
  }
  // printf("Creating a topology of %zd slots\n", len);
  MRClusterTopology *topo = rm_calloc(1, sizeof(MRClusterTopology));
  topo->hashFunc = MRHashFunc_CRC16;

  topo->numSlots = 16384;
  topo->numShards = 0;
  topo->shards = rm_calloc(len, sizeof(MRClusterShard));

  // iterate each nested array
  for (size_t i = 0; i < len; i++) {
    // e is slot range entry
    RedisModuleCallReply *e = RedisModule_CallReplyArrayElement(r, i);
    if (RedisModule_CallReplyLength(e) < 3) {
      printf("Invalid reply object for slot %zd, type %d. len %d\n", i,
             RedisModule_CallReplyType(e), (int)RedisModule_CallReplyLength(e));
      goto err;
    }
    // parse the start and end slots
    MRClusterShard sh;
    sh.startSlot = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(e, 0));
    sh.endSlot = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(e, 1));
    int numNodes = RedisModule_CallReplyLength(e) - 2;
    sh.numNodes = 0;
    sh.nodes = rm_calloc(numNodes, sizeof(MRClusterNode));
    // printf("Parsing slot %zd, %d nodes", i, numNodes);
    // parse the nodes
    for (size_t n = 0; n < numNodes; n++) {
      RedisModuleCallReply *nd = RedisModule_CallReplyArrayElement(e, n + 2);
      // the array node must be 3 elements (future versions may add more...)
      if (RedisModule_CallReplyLength(nd) < 3) {
        goto err;
      }

      // Parse the node information (host, port, id)
      size_t hostlen, idlen;
      const char *host =
          RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(nd, 0), &hostlen);
      const char *id =
          RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(nd, 2), &idlen);

      const char *id_str = rm_strndup(id, idlen);

      // We need to get the port using the `RedisModule_GetClusterNodeInfo` API because on 7.2
      // invoking `cluster slot` from RM_Call will always return the none tls port.
      // For for information refer to: https://github.com/redis/redis/pull/12233
      int port = 0;
      RedisModule_GetClusterNodeInfo(ctx, id_str, NULL, NULL, &port, NULL);

      MRClusterNode node = {
          .endpoint =
              (MREndpoint){
                  .host = rm_strndup(host, hostlen), .port = port, .auth = (clusterConfig.globalPass ? rm_strdup(clusterConfig.globalPass) : NULL) , .unixSock = NULL},
          .id = id_str,
          .flags = MRNode_Coordinator,
      };
      // the first node in every shard is the master
      if (n == 0) node.flags |= MRNode_Master;

      // compare the node id to our id
      if (!strncmp(node.id, myId, idlen)) {
        // printf("Found myself %s!\n", myId);
        node.flags |= MRNode_Self;
      }
      sh.nodes[sh.numNodes++] = node;

      // printf("Added node id %s, %s:%d master? %d\n", sh.nodes[n].id, sh.nodes[n].endpoint.host,
      //        sh.nodes[n].endpoint.port, sh.nodes[n].flags & MRNode_Master);
    }
    // printf("Added shard %d..%d with %d nodes\n", sh.startSlot, sh.endSlot, numNodes);
    topo->shards[topo->numShards++] = sh;
  }

  return topo;
err:
  RedisModule_Log(ctx, "error", "Error parsing cluster topology");
  MRClusterTopology_Free(topo);
  return NULL;
}

static struct RMUtilTimer *updateTopoTimer;

static int updateTopoCB(RedisModuleCtx *ctx, void *p) {
  RedisModule_ThreadSafeContextLock(ctx);
  RS_AutoMemory(ctx);

  RedisModuleCallReply *r = RedisModule_Call(ctx, REDISEARCH_MODULE_NAME".CLUSTERREFRESH", "");
  if (RedisModule_CallReplyType(r) == REDIS_REPLY_ERROR) {
    fprintf(stderr, "Error running CLUSTERREFRESH: %s\n", RedisModule_CallReplyStringPtr(r, NULL));
  }
  if (r) RedisModule_FreeCallReply(r);
  RedisModule_ThreadSafeContextUnlock(ctx);
  return 1;
}

int InitRedisTopologyUpdater() {
  updateTopoTimer =
      RMUtil_NewPeriodicTimer(updateTopoCB, NULL, NULL, (struct timespec){.tv_sec = 1});
  return REDIS_OK;
}
