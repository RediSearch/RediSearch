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

typedef uint16_t mr_slot_t;

/* A "shard" represents a slot range of the cluster, with its associated nodes. For each sharding
 * key, we select the slot based on the hash function, and then look for the shard in the cluster's
 * shard array */
typedef struct {
  mr_slot_t startSlot;
  mr_slot_t endSlot;
  size_t numNodes;
  size_t capNodes;
  MRClusterNode *nodes;
} MRClusterShard;

/* Create a new cluster shard to be added to a topology */
MRClusterShard MR_NewClusterShard(mr_slot_t startSlot, mr_slot_t endSlots, size_t capNodes);
void MRClusterShard_AddNode(MRClusterShard *sh, MRClusterNode *n);

#define MRHASHFUNC_CRC12_STR "CRC12"
#define MRHASHFUNC_CRC16_STR "CRC16"

typedef enum {
  MRHashFunc_None = 0,
  MRHashFunc_CRC12,
  MRHashFunc_CRC16,
} MRHashFunc;

/* A topology is the mapping of slots to shards and nodes */
typedef struct MRClusterTopology {
  size_t numSlots;
  MRHashFunc hashFunc;
  size_t numShards;
  size_t capShards;
  MRClusterShard *shards;
} MRClusterTopology;

MRClusterTopology *MR_NewTopology(size_t numShards, size_t numSlots, MRHashFunc hashFunc);
void MRClusterTopology_AddShard(MRClusterTopology *topo, MRClusterShard *sh);

void MRClusterTopology_Free(MRClusterTopology *t);

void MRClusterNode_Free(MRClusterNode *n);

/* A cluster has nodes and connections that can be used by the engine to send requests */
typedef struct {
  /* The connection manager holds a connection to each node, indexed by node id */
  /* An MRCluster holds an array of Connection Managers (one per each I/O thread)*/
  IORuntimeCtx **io_runtimes_pool;
  IORuntimeCtx *control_plane_io_runtime;
  size_t num_io_threads; // Number of threads in the pool (including the control plane)
  size_t io_runtimes_pool_size;
  size_t current_round_robin;
  /* The latest topology of the cluster */
  MRClusterTopology *topo;
} MRCluster;

int MRCluster_CheckConnections(MRClusterTopology *topo, IORuntimeCtx *ioRuntime, bool mastersOnly);

/* Multiplex a non-sharding command to all coordinators, using a specific coordination strategy. The
 * return value is the number of nodes we managed to successfully send the command to */
int MRCluster_FanoutCommand(MRClusterTopology *topo, IORuntimeCtx *ioRuntime, bool mastersOnly, MRCommand *cmd, redisCallbackFn *fn,
                            void *privdata);

/* Get a connected connection according to the cluster, strategy and command.
 * Returns NULL if no fitting connection exists at the moment */
MRConn *MRCluster_GetConn(MRClusterTopology *topo, IORuntimeCtx *ioRuntime, bool mastersOnly, MRCommand *cmd);

/* Send a command to its appropriate shard, selecting a node based on the coordination strategy.
 * Returns REDIS_OK on success, REDIS_ERR on failure. Notice that that send is asynchronous so even
 * though we signal for success, the request may fail */
int MRCluster_SendCommand(MRClusterTopology *topo, IORuntimeCtx *ioRuntime, bool mastersOnly, MRCommand *cmd, redisCallbackFn *fn,
                          void *privdata);

/* Asynchronously connect to all nodes in the cluster. This must be called before the io loop is
 * started */
int MRCluster_ConnectAll(IORuntimeCtx *ioRuntime);

/* Create a new cluster using a node provider */
MRCluster *MR_NewCluster(MRClusterTopology *topology, size_t conn_pool_size, size_t num_io_threads);

/* Update the number of connections per shard */
void MRCluster_UpdateConnPerShard(IORuntimeCtx *ioRuntime, size_t new_conn_pool_size);

/* Update the topology by calling the topology provider explicitly with ctx. If ctx is NULL, the
 * provider's current context is used. Otherwise, we call its function with the given context */
int MRCluster_UpdateTopology(MRCluster *cl, MRClusterTopology *newTopology);

void MRClust_Free(MRCluster *cl);

size_t IORuntimePool_AssignRoundRobinIdx(MRCluster *cl);

IORuntimeCtx *IORuntimePool_GetCtx(MRCluster *cl, size_t idx);
