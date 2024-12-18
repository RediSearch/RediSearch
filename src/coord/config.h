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
  MRClusterType type;
  int timeoutMS;
  const char* globalPass;
  size_t connPerShard;
  size_t cursorReplyThreshold;
  size_t coordinatorPoolSize; // number of threads in the coordinator thread pool
  size_t topologyValidationTimeoutMS;
  // The username for the ACL user used by the coordinator to connect to the shards on OSS cluster.
  const char* aclUsername;
} SearchClusterConfig;

extern SearchClusterConfig clusterConfig;
extern RedisModuleString *config_oss_acl_username;

#define CLUSTER_TYPE_OSS "redis_oss"
#define CLUSTER_TYPE_RLABS "redislabs"

#define COORDINATOR_POOL_DEFAULT_SIZE 20
#define DEFAULT_ACL_USERNAME "default"
#define DEFAULT_TOPOLOGY_VALIDATION_TIMEOUT 30000

#define DEFAULT_CLUSTER_CONFIG                                                 \
  (SearchClusterConfig) {                                                      \
    .connPerShard = 0,                                                         \
    .type = DetectClusterType(),                                               \
    .timeoutMS = 0,                                                            \
    .globalPass = NULL,                                                        \
    .cursorReplyThreshold = 1,                                                 \
    .coordinatorPoolSize = COORDINATOR_POOL_DEFAULT_SIZE,                      \
    .topologyValidationTimeoutMS = DEFAULT_TOPOLOGY_VALIDATION_TIMEOUT,        \
    .aclUsername = DEFAULT_ACL_USERNAME,                                       \
  }

/* Detect the cluster type, by trying to see if we are running inside RLEC.
 * If we cannot determine, we return OSS type anyway
 */
MRClusterType DetectClusterType();

RSConfigOptions *GetClusterConfigOptions(void);
void ClusterConfig_RegisterTriggers(void);

int RegisterClusterModuleConfig(RedisModuleCtx *ctx);
