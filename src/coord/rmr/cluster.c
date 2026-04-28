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
#include <string.h>
#include "rq.h"

extern RedisModuleCtx *RSDummyContext;

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

static inline MRConn* MRCluster_GetConn(IORuntimeCtx *ioRuntime, MRCommand *cmd) {
  RS_LOG_ASSERT(cmd->targetShard != NULL, "Command must know its target shard");
  RS_LOG_ASSERT(ioRuntime->topo != NULL, "IORuntimeCtx must have a valid topology here");

  return MRConn_Get(&ioRuntime->conn_mgr, cmd->targetShard);
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

/* Multiplex a non-sharding command to all coordinators, using a specific coordination strategy.  Returns the
 * number of sent commands.
 * If validateConnections is true, the function will validate that all connections are up before sending the command */
int MRCluster_FanoutCommand(IORuntimeCtx *ioRuntime,
                           MRCommand *cmd,
                           redisCallbackFn *fn,
                           void *privdata,
                           bool validateConnections) {
  struct MRClusterTopology *topo = ioRuntime->topo;
  uint32_t slotsInfoPos = cmd->slotsInfoArgIndex; // 0 if not set, which means slot info is not needed
  uint32_t dispatchTimePos = cmd->dispatchTimeArgIndex; // 0 if not set, which means dispatch time is not needed
  if (dispatchTimePos) {
    // Update dispatch time for this command
    MRCommand_SetDispatchTime(cmd);
  }

  // MOD-13322 DEBUG (DO NOT MERGE): force the FIRST _FT.SEARCH / _FT.AGGREGATE
  // fanout to take the validateConnections failure path, deterministically
  // reproducing the "one query lost across all shards" CI symptom.
  // Filtered to the test's commands so setup-time fanouts (FT.CREATE etc.) are unaffected.
  static int debug_remaining_fails = 1;
  if (debug_remaining_fails > 0 && cmd->num > 0 && cmd->strs[0] &&
      ((cmd->lens[0] == 10 && memcmp(cmd->strs[0], "_FT.SEARCH", 10) == 0) ||
       (cmd->lens[0] == 13 && memcmp(cmd->strs[0], "_FT.AGGREGATE", 13) == 0))) {
    debug_remaining_fails--;
    RedisModule_Log(RSDummyContext, "warning",
                    "MOD-13322 DEBUG: forcing fanout to fail for '%.*s'", (int)cmd->lens[0], cmd->strs[0]);
    return 0;
  }

  // Pre-fanout connection validation
  if (validateConnections) {
    for (size_t i = 0; i < topo->numShards; i++) {
      MRConn *conn = MRConn_Get(&ioRuntime->conn_mgr, topo->shards[i].node.id);
      if (!conn) {
        // MOD-13322: surface why a fanout was aborted before any shard was contacted.
        // MRConn_Get returns NULL when no connection in the pool is in `Connected` state,
        // which can happen transiently after MR_UpdateConnPoolSize has expanded the pool
        // but the new connections have not finished their async TCP+AUTH+HELLO handshake.
        const char *state = MRConnManager_GetNodeState(&ioRuntime->conn_mgr, topo->shards[i].node.id);
        RedisModule_Log(RSDummyContext, "warning",
                        "Fanout aborted by validateConnections: no Connected conn for shard %zu (node id %s, first-conn state %s, pool size %d)",
                        i, topo->shards[i].node.id, state ? state : "<no pool>", ioRuntime->conn_mgr.nodeConns);
        return 0;
      }
    }
  }

  int ret = 0;
  for (size_t i = 0; i < topo->numShards; i++) {
    MRConn *conn = MRConn_Get(&ioRuntime->conn_mgr, topo->shards[i].node.id);
    if (conn) {
      if (slotsInfoPos) {
        // Update slot info for this command
        MRCommand_SetSlotInfo(cmd, topo->shards[i].slotRanges);
      }
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
