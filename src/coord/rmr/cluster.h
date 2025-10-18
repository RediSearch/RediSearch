/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "triemap.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "conn.h"
#include "endpoint.h"
#include "command.h"
#include "node.h"
#include "io_runtime_ctx.h"
#include "cluster_topology.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A cluster has nodes and connections that can be used by the engine to send requests */
typedef struct {
  /* The connection manager holds a connection to each node, indexed by node id */
  /* An MRCluster holds an array of Connection Managers (one per each I/O thread)*/
  IORuntimeCtx **io_runtimes_pool;
  size_t num_io_threads; // Number of threads in the pool (including the control plane)
  size_t current_round_robin;
} MRCluster;

/* Multiplex a non-sharding command to all coordinators, using a specific coordination strategy. The
 * return value is the number of nodes we managed to successfully send the command to */
int MRCluster_FanoutCommand(IORuntimeCtx *ioRuntime, MRCommand *cmd, redisCallbackFn *fn, void *privdata);

/* Send a command to its appropriate shard, selecting a node based on the coordination strategy.
 * Returns REDIS_OK on success, REDIS_ERR on failure. Notice that that send is asynchronous so even
 * though we signal for success, the request may fail */
int MRCluster_SendCommand(IORuntimeCtx *ioRuntime, MRCommand *cmd, redisCallbackFn *fn, void *privdata);

/* Create a new cluster using a node provider */
MRCluster *MR_NewCluster(MRClusterTopology *topology, size_t conn_pool_size, size_t num_io_threads);

void MRCluster_Free(MRCluster *cl);

size_t MRCluster_AssignRoundRobinIORuntimeIdx(MRCluster *cl);

IORuntimeCtx *MRCluster_GetIORuntimeCtx(const MRCluster *cl, size_t idx);

#ifdef __cplusplus
}
#endif
