/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "redismodule.h"
#include "rmr/endpoint.h"
#include "../../src/config.h"

#include <string.h>

typedef enum { ClusterType_RedisOSS = 0, ClusterType_RedisLabs = 1 } MRClusterType;

typedef struct {
  size_t numPartitions;
  MRClusterType type;
  int timeoutMS;
  const char* globalPass;
  size_t connPerShard;
} SearchClusterConfig;

extern SearchClusterConfig clusterConfig;

#define CLUSTER_TYPE_OSS "redis_oss"
#define CLUSTER_TYPE_RLABS "redislabs"

#define DEFAULT_CLUSTER_CONFIG                                                             \
  (SearchClusterConfig) {                                                                  \
    .numPartitions = 0,                                                                    \
    .connPerShard = 0,                                                                     \
    .type = DetectClusterType(),                                                           \
    .timeoutMS = 500,                                                                      \
    .globalPass = NULL,                                                                    \
  }

/* Detect the cluster type, by trying to see if we are running inside RLEC.
 * If we cannot determine, we return OSS type anyway
 */
MRClusterType DetectClusterType();

/* Load the configuration from the module arguments.
 * Argument format: PARTITIONS {num_partitions} ENDPOINT {[password@]host:port}
 */
int ParseConfig(SearchClusterConfig *conf, RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

RSConfigOptions *GetClusterConfigOptions(void);
