/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "triemap/triemap.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "conn.h"
#include "endpoint.h"
#include "command.h"
#include "node.h"

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
// MR_NewToplogy
// MRTopology_AddShard MRShard_AddNode

#define MRHASHFUNC_CRC12_STR "CRC12"
#define MRHASHFUNC_CRC16_STR "CRC16"

typedef enum {
  MRHashFunc_None = 0,
  MRHashFunc_CRC12,
  MRHashFunc_CRC16,
} MRHashFunc;

/* A topology is the mapping of slots to shards and nodes */
typedef struct {
  size_t numSlots;
  MRHashFunc hashFunc;
  size_t numShards;
  size_t capShards;
  MRClusterShard *shards;

} MRClusterTopology;

MRClusterTopology *MR_NewTopology(size_t numShards, size_t numSlots);
void MRClusterTopology_AddShard(MRClusterTopology *topo, MRClusterShard *sh);

void MRClusterTopology_Free(MRClusterTopology *t);

void MRClusterNode_Free(MRClusterNode *n);

/* Check the validity of the topology. A topology is considered valid if we have shards, and the
 * slot coverage is complete */
int MRClusterTopology_IsValid(MRClusterTopology *t);

/* A function that tells the cluster which shard to send a command to. should return -1 if not
 * applicable */
typedef mr_slot_t (*ShardFunc)(MRCommand *cmd, mr_slot_t numSlots);

/* A cluster has nodes and connections that can be used by the engine to send requests */
typedef struct {
  /* The connection manager holds a connection to each node, indexed by node id */
  MRConnManager mgr;
  /* The latest topology of the cluster */
  MRClusterTopology *topo;
  /* the current node, detected when updating the topology */
  MRClusterNode *myNode;
  MRClusterShard *myshard;
  /* The sharding functino, responsible for transforming keys into slots */
  ShardFunc sf;

  /* map of nodes by ip:port */
  MRNodeMap *nodeMap;

  // the time we last updated the topology
  // TODO: use millisecond precision time here
  time_t lastTopologyUpdate;
  // the minimum allowed interval between topology updates
  long long topologyUpdateMinInterval;
} MRCluster;

/* Define the coordination strategy of a coordination command */
typedef enum {
  /* Send the coordination command to all nodes */
  MRCluster_FlatCoordination,
  /* Send the command to one coordinator per physical machine (identified by its IP address) */
  MRCluster_RemoteCoordination,
  /* Send the command to local nodes only - i.e. nodes working on the same physical host */
  MRCluster_LocalCoordination,
  /* If this is set, we only wish to talk to masters.
   * NOTE: This is a flag that should be added to the strategy along with one of the above */
  MRCluster_MastersOnly = 0x08,

} MRCoordinationStrategy;

/* Multiplex a non-sharding command to all coordinators, using a specific coordination strategy. The
 * return value is the number of nodes we managed to successfully send the command to */
int MRCluster_FanoutCommand(MRCluster *cl, MRCoordinationStrategy strategy, MRCommand *cmd,
                            redisCallbackFn *fn, void *privdata);

/* Send a command to its approrpriate shard, selecting a node based on the coordination strategy.
 * Returns REDIS_OK on success, REDIS_ERR on failure. Notice that that send is asynchronous so even
 * thuogh we signal for success, the request may fail */
int MRCluster_SendCommand(MRCluster *cl, MRCoordinationStrategy strategy, MRCommand *cmd,
                          redisCallbackFn *fn, void *privdata);

/* The number of individual hosts (by IP adress) in the cluster */
size_t MRCluster_NumHosts(MRCluster *cl);

/* The number of nodes in the cluster */
size_t MRCluster_NumNodes(MRCluster *cl);

/* The number of shard instances in the cluster */
size_t MRCluster_NumShards(MRCluster *cl);

/* Asynchronously connect to all nodes in the cluster. This must be called before the io loop is
 * started */
int MRCluster_ConnectAll(MRCluster *cl);

/* Create a new cluster using a node provider */
MRCluster *MR_NewCluster(MRClusterTopology *topology, size_t conn_pool_size, ShardFunc sharder,
                         long long minTopologyUpdateInterval);

/* Update the topology by calling the topology provider explicitly with ctx. If ctx is NULL, the
 * provider's current context is used. Otherwise, we call its function with the given context */
int MRCLuster_UpdateTopology(MRCluster *cl, MRClusterTopology *newTopology);

mr_slot_t CRC16ShardFunc(MRCommand *cmd, mr_slot_t numSlots);
mr_slot_t CRC12ShardFunc(MRCommand *cmd, mr_slot_t numSlots);

typedef struct {
  const char *base;
  size_t baseLen;
  const char *shard;
  size_t shardLen;
} MRKey;

void MRKey_Parse(MRKey *key, const char *srckey, size_t srclen);

void MRClust_Free(MRCluster *cl);
