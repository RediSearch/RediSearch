/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "rmr/cluster.h"

#include <stdint.h>

void InitGlobalSearchCluster();

int SearchCluster_Ready();
size_t SearchCluster_Size();

/* Make sure that if the cluster is unaware of its sizing, it will take the size from the topology
 */
void SearchCluster_EnsureSize(RedisModuleCtx *ctx, MRClusterTopology *topo);
