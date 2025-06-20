/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "cluster.h"
#include "crc16.h"
#include "crc12.h"
#include "rmutil/rm_assert.h"

#include <stdlib.h>
#include "rq.h"

/* Initialize the MapReduce engine with a node provider */
MRCluster *MR_NewCluster(MRClusterTopology *initialTopology, size_t conn_pool_size, size_t num_io_threads) {
  MRCluster *cl = rm_new(MRCluster);
  RS_ASSERT(num_io_threads > 0);
  cl->num_io_threads = num_io_threads;
  cl->io_runtimes_pool_size = num_io_threads - 1;
  cl->current_round_robin = 0;  // Initialize round-robin counter
  if (num_io_threads <= 1) {
    cl->control_plane_io_runtime = IORuntimeCtx_Create(conn_pool_size, initialTopology, 0, true);
    cl->io_runtimes_pool = NULL;
  } else {
    cl->io_runtimes_pool = rm_malloc(cl->io_runtimes_pool_size * sizeof(IORuntimeCtx*));
    cl->control_plane_io_runtime = IORuntimeCtx_Create(conn_pool_size, initialTopology, 0, true);
    for (size_t i = 0; i < cl->io_runtimes_pool_size; i++) {
      cl->io_runtimes_pool[i] = IORuntimeCtx_Create(conn_pool_size, initialTopology, i + 1, false);
    }
  }
  return cl;
}

/* Find the shard responsible for a given slot */
MRClusterShard *_MRCluster_FindShard(MRClusterTopology *topo, unsigned slot) {
  // TODO: Switch to binary search
  for (int i = 0; i < topo->numShards; i++) {
    if (topo->shards[i].startSlot <= slot && topo->shards[i].endSlot >= slot) {
      return &topo->shards[i];
    }
  }
  return NULL;
}

/* Select a node from the shard according to the coordination strategy */
MRClusterNode *_MRClusterShard_SelectNode(MRClusterShard *sh, bool mastersOnly) {
  // if we only want masters - find the master of this shard
  if (mastersOnly) {
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

typedef struct {
  const char *base;
  size_t baseLen;
  const char *shard;
  size_t shardLen;
} MRKey;

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

static const char *MRGetShardKey(const MRCommand *cmd, size_t *len) {
  int pos = MRCommand_GetShardingKey(cmd);
  if (pos >= cmd->num) {
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


static mr_slot_t getSlotByCmd(const MRCommand *cmd, const MRClusterTopology *topo) {

  if(cmd->targetSlot >= 0){
    return cmd->targetSlot;
  }

  size_t len;
  const char *k = MRGetShardKey(cmd, &len);
  if (!k) return 0;
  // Default to crc16
  uint16_t crc = (topo->hashFunc == MRHashFunc_CRC12) ? crc12(k, len) : crc16(k, len);
  return crc % topo->numSlots;
}

MRConn* MRCluster_GetConn(IORuntimeCtx *ioRuntime, bool mastersOnly, MRCommand *cmd) {

  if (!ioRuntime->topo) return NULL;

  /* Get the cluster slot from the sharder */
  unsigned slot = getSlotByCmd(cmd, ioRuntime->topo);

  /* Get the shard from the slot map */
  MRClusterShard *sh = _MRCluster_FindShard(ioRuntime->topo, slot);
  if (!sh) return NULL;

  MRClusterNode *node = _MRClusterShard_SelectNode(sh, mastersOnly);
  if (!node) return NULL;

  return MRConn_Get(ioRuntime->conn_mgr, node->id);
}

/* Send a single command to the right shard in the cluster, with an optional control over node
 * selection */
int MRCluster_SendCommand(IORuntimeCtx *ioRuntime,
                          bool mastersOnly,
                          MRCommand *cmd,
                          redisCallbackFn *fn,
                          void *privdata) {
  MRConn *conn = MRCluster_GetConn(ioRuntime, mastersOnly, cmd);
  if (!conn) return REDIS_ERR;
  return MRConn_SendCommand(conn, cmd, fn, privdata);
}

int MRCluster_CheckConnections(IORuntimeCtx *ioRuntime,
                              bool mastersOnly) {
  struct MRClusterTopology *topo = ioRuntime->topo;
  for (size_t i = 0; i < topo->numShards; i++) {
    MRClusterShard *sh = &topo->shards[i];
    for (size_t j = 0; j < sh->numNodes; j++) {
      if (mastersOnly && !(sh->nodes[j].flags & MRNode_Master)) {
        continue;
      }
      if (!MRConn_Get(ioRuntime->conn_mgr, sh->nodes[j].id)) {
        return REDIS_ERR;
      }
    }
  }
  return REDIS_OK;
}

/* Multiplex a command to all coordinators, using a specific coordination strategy. Returns the
 * number of sent commands */
int MRCluster_FanoutCommand(IORuntimeCtx *ioRuntime,
                           bool mastersOnly,
                           MRCommand *cmd,
                           redisCallbackFn *fn,
                           void *privdata) {
  struct MRClusterTopology *topo = ioRuntime->topo;
  int ret = 0;
  for (size_t i = 0; i < topo->numShards; i++) {
    MRClusterShard *sh = &topo->shards[i];
    for (size_t j = 0; j < sh->numNodes; j++) {
      if (mastersOnly && !(sh->nodes[j].flags & MRNode_Master)) {
        continue;
      }
      MRConn *conn = MRConn_Get(ioRuntime->conn_mgr, sh->nodes[j].id);
      if (conn) {
        if (MRConn_SendCommand(conn, cmd, fn, privdata) != REDIS_ERR) {
          ret++;
        }
      }
    }
  }
  return ret;
}

void MRCluster_UpdateConnPerShard(IORuntimeCtx *ioRuntime, size_t new_conn_pool_size) {
  RS_ASSERT(new_conn_pool_size > 0);
  size_t old_conn_pool_size = ioRuntime->conn_mgr->nodeConns;
  if (old_conn_pool_size > new_conn_pool_size) {
    MRConnManager_Shrink(ioRuntime->conn_mgr, new_conn_pool_size, IORuntimeCtx_GetLoop(ioRuntime));
  } else if (old_conn_pool_size < new_conn_pool_size) {
    MRConnManager_Expand(ioRuntime->conn_mgr, new_conn_pool_size, IORuntimeCtx_GetLoop(ioRuntime));
  }
}

void MRClust_Free(MRCluster *cl) {
  if (cl) {
    // First, fire the shutdown event for all runtimes
    if (cl->io_runtimes_pool) {
      for (size_t i = 0; i < cl->io_runtimes_pool_size; i++) {
        IORuntimeCtx_FireShutdown(cl->io_runtimes_pool[i]);
      }
    }
    IORuntimeCtx_FireShutdown(cl->control_plane_io_runtime);

    // Then free the RuntimeCtx, it will join the threads
    if (cl->io_runtimes_pool) {
      for (size_t i = 0; i < cl->io_runtimes_pool_size; i++) {
        IORuntimeCtx_Free(cl->io_runtimes_pool[i]);
      }
      rm_free(cl->io_runtimes_pool);
    }
    IORuntimeCtx_Free(cl->control_plane_io_runtime);
    rm_free(cl);
  }
}

size_t MRCluster_AssignRoundRobinIORuntimeIdx(MRCluster *cl) {
  // Idea is to skip the control plane runtime
  if (cl->io_runtimes_pool_size == 0) {
    return 0; // default to the control plane one
  }
  size_t idx = cl->current_round_robin + 1;
  cl->current_round_robin = (cl->current_round_robin  + 1) % cl->io_runtimes_pool_size;
  return idx;
}

IORuntimeCtx *MRCluster_GetControlPlaneIORuntimeCtx(const MRCluster *cl) {
  return cl->control_plane_io_runtime;
}

IORuntimeCtx *MRCluster_GetIORuntimeCtx(const MRCluster *cl, size_t idx) {
  if (idx == 0) {
    return cl->control_plane_io_runtime;
  }
  return cl->io_runtimes_pool[idx - 1];
}
