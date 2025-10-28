/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "cluster.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"

#include <stdlib.h>
#include "rq.h"

/* Initialize the MapReduce engine with a node provider */
MRCluster *MR_NewCluster(MRClusterTopology *initialTopology, size_t conn_pool_size, size_t num_io_threads) {
  MRCluster *cl = rm_new(MRCluster);
  RS_ASSERT(num_io_threads > 0);
  cl->num_io_threads = num_io_threads;
  cl->current_round_robin = 0;  // Initialize round-robin counter
  cl->io_runtimes_pool = rm_malloc(cl->num_io_threads * sizeof(IORuntimeCtx*));
  for (size_t i = 0; i < cl->num_io_threads; i++) {
    cl->io_runtimes_pool[i] = IORuntimeCtx_Create(conn_pool_size, initialTopology, i + 1, i == 0);
  }

  return cl;
}

static MRConn* MRCluster_GetConn(IORuntimeCtx *ioRuntime, MRCommand *cmd) {
  RS_LOG_ASSERT(cmd->targetShard != INVALID_SHARD, "Command must know its target shard");
  RS_LOG_ASSERT(ioRuntime->topo != NULL, "IORuntimeCtx must have a valid topology here");

  // No target shard, or out of range (may happen during topology updates)
  if (cmd->targetShard >= ioRuntime->topo->numShards) {
    RedisModule_Log(RSDummyContext, "warning", "Command targetShard %d is out of bounds (numShards=%u)", cmd->targetShard, ioRuntime->topo->numShards);
    return NULL;
  }

  /* Get the shard directly by the targetShard field */
  MRClusterShard *sh = &ioRuntime->topo->shards[cmd->targetShard];

  return MRConn_Get(&ioRuntime->conn_mgr, sh->node.id);
}

/* Send a single command to the right shard in the cluster, with an optional control over node
 * selection */
int MRCluster_SendCommand(IORuntimeCtx *ioRuntime,
                          MRCommand *cmd,
                          redisCallbackFn *fn,
                          void *privdata) {
  MRConn *conn = MRCluster_GetConn(ioRuntime, cmd);
  if (!conn) return REDIS_ERR;
  return MRConn_SendCommand(conn, cmd, fn, privdata);
}

/* Multiplex a command to all coordinators, using a specific coordination strategy. Returns the
 * number of sent commands */
int MRCluster_FanoutCommand(IORuntimeCtx *ioRuntime,
                           MRCommand *cmd,
                           redisCallbackFn *fn,
                           void *privdata) {
  struct MRClusterTopology *topo = ioRuntime->topo;
  int ret = 0;
  for (size_t i = 0; i < topo->numShards; i++) {
    MRConn *conn = MRConn_Get(&ioRuntime->conn_mgr, topo->shards[i].node.id);
    if (conn) {
      if (MRConn_SendCommand(conn, cmd, fn, privdata) != REDIS_ERR) {
        ret++;
      }
    }
  }
  return ret;
}

void MRCluster_Free(MRCluster *cl) {
  if (cl) {
    // First, fire the shutdown event for all runtimes
    if (cl->io_runtimes_pool) {
      for (size_t i = 0; i < cl->num_io_threads; i++) {
        IORuntimeCtx_FireShutdown(cl->io_runtimes_pool[i]);
      }
    }

    // Then free the RuntimeCtx, it will join the threads
    if (cl->io_runtimes_pool) {
      for (size_t i = 0; i < cl->num_io_threads; i++) {
        IORuntimeCtx_Free(cl->io_runtimes_pool[i]);
      }
      rm_free(cl->io_runtimes_pool);
    }
    rm_free(cl);
  }
}

size_t MRCluster_AssignRoundRobinIORuntimeIdx(MRCluster *cl) {
  size_t idx = cl->current_round_robin;
  cl->current_round_robin = (cl->current_round_robin + 1) % cl->num_io_threads;
  return idx;
}

IORuntimeCtx *MRCluster_GetIORuntimeCtx(const MRCluster *cl, size_t idx) {
  RS_ASSERT(idx < cl->num_io_threads);
  return cl->io_runtimes_pool[idx];
}
