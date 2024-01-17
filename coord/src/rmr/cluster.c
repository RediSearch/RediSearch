/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "cluster.h"
#include "hiredis/adapters/libuv.h"
#include "crc16.h"
#include "crc12.h"
#include "rmutil/vector.h"
#include "node_map.h"

#include <stdlib.h>

void _MRClsuter_UpdateNodes(MRCluster *cl) {
  if (cl->topo) {

    /* Reallocate the cluster's node map */
    if (cl->nodeMap) {
      MRNodeMap_Free(cl->nodeMap);
    }
    cl->nodeMap = MR_NewNodeMap();

    /* Get all the current node ids from the connection manager.  We will remove all the nodes
     * that are in the new topology, and after the update, delete all the nodes that are in this map
     * and not in the new topology */
    dict *nodesToDisconnect = dictCreate(&dictTypeHeapStrings, NULL);
    dictIterator *it = dictGetIterator(cl->mgr.map);
    dictEntry *de;
    while ((de = dictNext(it))) {
      dictAdd(nodesToDisconnect, dictGetKey(de), NULL);
    }
    dictReleaseIterator(it);

    /* Walk the topology and add all nodes in it to the connection manager */
    for (int sh = 0; sh < cl->topo->numShards; sh++) {
      for (int n = 0; n < cl->topo->shards[sh].numNodes; n++) {
        MRClusterNode *node = &cl->topo->shards[sh].nodes[n];
        // printf("Adding node %s:%d to cluster\n", node->endpoint.host, node->endpoint.port);
        MRConnManager_Add(&cl->mgr, node->id, &node->endpoint, 0);

        /* Add the node to the node map */
        MRNodeMap_Add(cl->nodeMap, node);

        /* This node is still valid, remove it from the nodes to delete list */
        dictDelete(nodesToDisconnect, node->id);

        /* See if this is us - if so we need to update the cluster's host and current id */
        if (node->flags & MRNode_Self) {
          cl->myNode = node;
          cl->myshard = &cl->topo->shards[sh];
        }
      }
    }

    // if we didn't remove the node from the original nodes map copy, it means it's not in the new topology,
    // we need to disconnect the node's connections
    it = dictGetIterator(nodesToDisconnect);
    while ((de = dictNext(it))) {
      MRConnManager_Disconnect(&cl->mgr, dictGetKey(de));
    }
    dictReleaseIterator(it);
    dictRelease(nodesToDisconnect);
  }
}

MRCluster *MR_NewCluster(MRClusterTopology *initialTopology, size_t conn_pool_size, ShardFunc sf,
                         long long minTopologyUpdateInterval) {
  MRCluster *cl = rm_new(MRCluster);
  cl->sf = sf;
  cl->topologyUpdateMinInterval = minTopologyUpdateInterval;
  cl->lastTopologyUpdate = 0;
  cl->topo = initialTopology;
  cl->nodeMap = NULL;
  cl->myNode = NULL;  // tODO: discover local ip/port
  cl->myshard = NULL;
  MRConnManager_Init(&cl->mgr, conn_pool_size);

  if (cl->topo) {
    _MRClsuter_UpdateNodes(cl);
  }
  return cl;
}

/* Find the shard responsible for a given slot */
MRClusterShard *_MRCluster_FindShard(MRCluster *cl, uint slot) {
  // TODO: Switch to binary search
  for (int i = 0; i < cl->topo->numShards; i++) {
    if (cl->topo->shards[i].startSlot <= slot && cl->topo->shards[i].endSlot >= slot) {
      return &cl->topo->shards[i];
    }
  }
  return NULL;
}

/* Select a node from the shard according to the coordination strategy */
MRClusterNode *_MRClusterShard_SelectNode(MRClusterShard *sh, MRClusterNode *myNode,
                                          MRCoordinationStrategy strategy) {

  switch (strategy & ~MRCluster_MastersOnly) {

    case MRCluster_LocalCoordination:
      for (int i = 0; i < sh->numNodes; i++) {
        MRClusterNode *n = &sh->nodes[i];
        // skip slaves if this is a master only request
        if (strategy & MRCluster_MastersOnly && !(n->flags & MRNode_Master)) {
          continue;
        }
        if (MRNode_IsSameHost(n, myNode)) {
          return n;
        }
      }
      // Not found...
      return NULL;

    case MRCluster_RemoteCoordination:
      for (int i = 0; i < sh->numNodes; i++) {
        MRClusterNode *n = &sh->nodes[i];
        // skip slaves if this is a master only request
        if (strategy & MRCluster_MastersOnly && !(n->flags & MRNode_Master)) {
          continue;
        }
        if (!MRNode_IsSameHost(n, myNode)) {
          return n;
        }
      }
      // Not found...
      return NULL;

    case MRCluster_FlatCoordination:
      // if we only want masters - find the master of this shard
      if (strategy & MRCluster_MastersOnly) {
        for (int i = 0; i < sh->numNodes; i++) {
          if (sh->nodes[i].flags & MRNode_Master) {
            return &sh->nodes[i];
          }
        }
        return NULL;
      }
      // if we don't care - select a random node
      return &sh->nodes[rand() % sh->numNodes];
  }
  return NULL;
}

/* Send a single command to the right shard in the cluster, with an optional control over node
 * selection */
int MRCluster_SendCommand(MRCluster *cl, MRCoordinationStrategy strategy, MRCommand *cmd,
                          redisCallbackFn *fn, void *privdata) {

  if (!cl || !cl->topo) {
    return REDIS_ERR;
  }

  /* Get the cluster slot from the sharder */
  uint slot = cl->sf(cmd, cl->topo->numSlots);

  /* Get the shard from the slotmap */
  MRClusterShard *sh = _MRCluster_FindShard(cl, slot);
  if (!sh) {
    return REDIS_ERR;
  }

  MRClusterNode *node = _MRClusterShard_SelectNode(sh, cl->myNode, strategy);
  if (!node) return REDIS_ERR;

  MRConn *conn = MRConn_Get(&cl->mgr, node->id);
  if (!conn) return REDIS_ERR;
  return MRConn_SendCommand(conn, cmd, fn, privdata);
}

/* Multiplex a command to all coordinators, using a specific coordination strategy. Returns the
 * number of sent commands */
int MRCluster_FanoutCommand(MRCluster *cl, MRCoordinationStrategy strategy, MRCommand *cmd,
                            redisCallbackFn *fn, void *privdata) {
  if (!cl->nodeMap) {
    return 0;
  }

  int cmd_proto = cmd->protocol;

  MRNodeMapIterator it;
  switch (strategy & ~(MRCluster_MastersOnly)) {
    case MRCluster_LocalCoordination:
      it = MRNodeMap_IterateHost(cl->nodeMap, cl->myNode->endpoint.host);
      break;
    default:
      it = MRNodeMap_IterateAll(cl->nodeMap);
  }

  int ret = 0;
  MRClusterNode *n;
  while (NULL != (n = it.Next(&it))) {
    if ((strategy & MRCluster_MastersOnly) && !(n->flags & MRNode_Master)) {
      continue;
    }
    MRConn *conn = MRConn_Get(&cl->mgr, n->id);
    // printf("Sending fanout command to %s:%d\n", conn->ep.host, conn->ep.port);
    if (conn) {
      //@@TODO: maybe not required
      if (!conn->protocol || cmd_proto != conn->protocol) {
        MRCommand hello = MR_NewCommand(2, "HELLO", cmd_proto == 3 ? "3" : "2");
        int rc = MRConn_SendCommand(conn, &hello, NULL, privdata);
        MRCommand_Free(&hello);
        conn->protocol = cmd_proto;
      }

      if (MRConn_SendCommand(conn, cmd, fn, privdata) != REDIS_ERR) {
        ret++;
      }
    }
  }
  if(cmd->cmd) {
    sdsfree(cmd->cmd);
    cmd->cmd = NULL;
  }
  MRNodeMapIterator_Free(&it);

  return ret;
}

/* Initialize the connections to all shards */
int MRCluster_ConnectAll(MRCluster *cl) {

  return MRConnManager_ConnectAll(&cl->mgr);
}

void MRKey_Parse(MRKey *mk, const char *src, size_t srclen) {
  mk->shard = mk->base = src;
  mk->shardLen = mk->baseLen = srclen;

  const char *endBrace = src + srclen - 1;
  if (srclen < 3 || !*endBrace || *endBrace != '}') {
    // printf("No closing brace found!\n");
    return;
  }

  const char *beginBrace = endBrace;
  while (beginBrace >= src && *beginBrace != '{') {
    beginBrace--;
  }

  if (*beginBrace != '{') {
    // printf("No open brace found!\n");
    return;
  }

  mk->baseLen = beginBrace - src;
  mk->shard = beginBrace + 1;
  mk->shardLen = endBrace - mk->shard;
  // printf("Shard key: %.*s\n", (int)mk->shardLen, mk->shard);
}

static const char *MRGetShardKey(MRCommand *cmd, size_t *len) {
  int pos = MRCommand_GetShardingKey(cmd);
  if (pos < 0 || pos >= cmd->num) {
    // printf("Returning NULL.. pos=%d, num=%d\n", pos, cmd->num);
    return NULL;
  }

  size_t klen;
  const char *k = MRCommand_ArgStringPtrLen(cmd, pos, &klen);
  MRKey mk;
  MRKey_Parse(&mk, k, klen);
  *len = mk.shardLen;
  return mk.shard;
}

mr_slot_t CRC16ShardFunc(MRCommand *cmd, mr_slot_t numSlots) {

  size_t len;

  if(cmd->targetSlot >= 0){
    return cmd->targetSlot;
  }

  const char *k = MRGetShardKey(cmd, &len);
  if (!k) return 0;
  uint16_t crc = crc16(k, len);
  return crc % numSlots;
}

mr_slot_t CRC12ShardFunc(MRCommand *cmd, mr_slot_t numSlots) {
  size_t len;

  if(cmd->targetSlot >= 0){
    return cmd->targetSlot;
  }

  const char *k = MRGetShardKey(cmd, &len);
  if (!k) return 0;
  uint16_t crc = crc12(k, len);
  return crc % numSlots;
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

int MRClusterTopology_IsValid(MRClusterTopology *t) {
  if (!t || t->numShards <= 0 || t->numSlots <= 0) {
    return 0;
  }

  mr_slot_t sum = 0;
  for (size_t s = 0; s < t->numShards; s++) {
    sum += 1 + t->shards[s].endSlot - t->shards[s].startSlot;
  }

  return sum >= t->numSlots;
}

size_t MRCluster_NumShards(MRCluster *cl) {
  if (cl->topo) {
    return cl->topo->numShards;
  }
  return 0;
}

void MRClusterNode_Free(MRClusterNode *n) {
  MREndpoint_Free(&n->endpoint);
  rm_free((char *)n->id);
}

static ShardFunc selectHashFunc(MRHashFunc f) {
  switch (f) {
    case MRHashFunc_CRC12:
      return CRC12ShardFunc;
    case MRHashFunc_CRC16:
      return CRC16ShardFunc;
    default:
      break;
  }
  return NULL;
}
int MRCLuster_UpdateTopology(MRCluster *cl, MRClusterTopology *newTopo) {

  if (!newTopo) return REDIS_ERR;
  time_t now = time(NULL);
  cl->lastTopologyUpdate = now;

  // if the topology has updated, we update to the new one
  if (newTopo->hashFunc != MRHashFunc_None) {
    cl->sf = selectHashFunc(newTopo->hashFunc);
  } else if (cl->topo) {
    newTopo->hashFunc = cl->topo->hashFunc;
  }

  MRClusterTopology *old = cl->topo;
  cl->topo = newTopo;
  if (cl->topo) {
    _MRClsuter_UpdateNodes(cl);

    MRCluster_ConnectAll(cl);
  }
  if (old) {
    MRClusterTopology_Free(old);
  }
  return REDIS_OK;
}

size_t MRCluster_NumHosts(MRCluster *cl) {
  return cl->nodeMap ? MRNodeMap_NumHosts(cl->nodeMap) : 0;
}

size_t MRCluster_NumNodes(MRCluster *cl) {
  return cl->nodeMap ? MRNodeMap_NumNodes(cl->nodeMap) : 0;
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

void MRClusterShard_AddNode(MRClusterShard *sh, MRClusterNode *n) {
  if (sh->capNodes == sh->numNodes) {
    sh->capNodes += 1;
    sh->nodes = rm_realloc(sh->nodes, sh->capNodes * sizeof(MRClusterNode));
  }
  sh->nodes[sh->numNodes++] = *n;
}

MRClusterTopology *MR_NewTopology(size_t numShards, size_t numSlots) {
  MRClusterTopology *topo = rm_calloc(1, sizeof(*topo));
  topo->capShards = numShards;
  topo->numShards = 0;
  topo->numSlots = numSlots;
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

void MRClust_Free(MRCluster *cl) {
  if (cl) {
    if (cl->topo)
      MRClusterTopology_Free(cl->topo);
    if (cl->nodeMap)
      MRNodeMap_Free(cl->nodeMap);
    if (cl->mgr.map)
      MRConnManager_Free(&cl->mgr);
    rm_free(cl);
  }
}
