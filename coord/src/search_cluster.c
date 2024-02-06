/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "search_cluster.h"
#include "alias.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

size_t __searchCluster;

void InitGlobalSearchCluster() {
  __searchCluster = 0;
}

inline int SearchCluster_Ready() {
  return __searchCluster != 0;
}

inline size_t SearchCluster_Size() {
  return __searchCluster;
}


/* Make sure that the cluster either has a size or updates its size from the topology when updated.
 * If the user did not define the number of partitions, we just take the number of shards in the
 * first topology update and get a fix on that */
void SearchCluster_EnsureSize(RedisModuleCtx *ctx, MRClusterTopology *topo) {
  // If the cluster doesn't have a size yet - set the partition number aligned to the shard number
  if (MRClusterTopology_IsValid(topo)) {
    RedisModule_Log(ctx, "debug", "Setting number of partitions to %ld", topo->numShards);
    __searchCluster = topo->numShards;
  }
}
