
#pragma once

#include "cluster.h"

// forward declaration
struct RedisModuleCtx;
MRClusterTopology *RedisCluster_GetTopology(struct RedisModuleCtx *);

int InitRedisTopologyUpdater();
