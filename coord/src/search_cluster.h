/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "rmr/command.h"
#include "rmr/cluster.h"
#include "partition.h"

#include <stdint.h>

/* A search cluster contains the configurations for partitioning and multiplexing commands */
typedef struct {
  size_t size;
  int* shardsStartSlots;
  PartitionCtx part;
  size_t myPartition;
} SearchCluster;

SearchCluster *GetSearchCluster();

/* Create a search cluster with a given number of partitions (size) and a partitioner.
 * TODO: This whole object is a bit redundant and adds nothing on top of the partitioner. Consider
 * consolidating the two  */
SearchCluster NewSearchCluster(size_t size, const char **table, size_t tableSize);
void InitGlobalSearchCluster(size_t size, const char **table, size_t tableSize);

void SearchCluster_Release(SearchCluster *sc);
/* A command generator that multiplexes a command across multiple partitions by tagging it */
typedef struct {
  MRCommand *cmd;
  char *keyAlias;
  int keyOffset;
  size_t offset;
  SearchCluster *cluster;
} SCCommandMuxIterator;

int SearchCluster_Ready(SearchCluster *sc);

/* Multiplex a command to the cluster using an iterator that will yield a multiplexed command per
 * iteration, based on the original command */
MRCommandGenerator SearchCluster_MultiplexCommand(SearchCluster *c, MRCommand *cmd);

/* Make sure that if the cluster is unaware of its sizing, it will take the size from the topology
 */
void SearchCluster_EnsureSize(RedisModuleCtx *ctx, SearchCluster *c, MRClusterTopology *topo);

void SetMyPartition(MRClusterTopology *ct, MRClusterShard *myShard);

int checkTLS(char** client_key, char** client_cert, char** ca_cert, char** key_pass);
