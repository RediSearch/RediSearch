/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "config.h"
#include "util/config_macros.h"
#include "rmr/rmr.h"

#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "hiredis/hiredis.h"

#include <string.h>
#include <stdlib.h>

extern RedisModuleCtx *RSDummyContext;

#define CONFIG_FROM_RSCONFIG(c) ((SearchClusterConfig *)(c)->chainedConfig)

static SearchClusterConfig* getOrCreateRealConfig(RSConfig *config){
  if(!CONFIG_FROM_RSCONFIG(config)){
    config->chainedConfig = &clusterConfig;
  }
  return CONFIG_FROM_RSCONFIG(config);
}

// PARTITIONS
CONFIG_SETTER(setNumPartitions) {
  int acrc = AC_Advance(ac); // Consume the argument
  RedisModule_Log(RSDummyContext, "notice", "PARTITIONS option is deprecated. Set to `AUTO`");
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getNumPartitions) {
  return sdsnew("AUTO");
}

// TIMEOUT
CONFIG_SETTER(setClusterTimeout) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetInt(ac, &realConfig->timeoutMS, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getClusterTimeout) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  return sdsfromlonglong(realConfig->timeoutMS);
}

bool OssGlobalPasswordConfigExists(RedisModuleCtx *ctx) {
  RedisModuleCallReply *rep =
            RedisModule_Call(ctx, "CONFIG", "cc", "GET", "oss-global-password");
  RedisModule_Assert(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY);
  if (RedisModule_CallReplyLength(rep) == 2) {
    RedisModuleCallReply *valueRep = RedisModule_CallReplyArrayElement(rep, 0);
    if (RedisModule_CallReplyType(valueRep) == REDISMODULE_REPLY_STRING) {
      size_t len;
      const char* valueRepCStr = RedisModule_CallReplyStringPtr(valueRep, &len);
      if (strcmp(valueRepCStr, "oss-global-password") == 0) {
        RedisModule_FreeCallReply(rep);
        return true;
      }
    }
  }
  RedisModule_FreeCallReply(rep);
  return false;
}

CONFIG_SETTER(setGlobalPass) {
  RedisModule_Log(RSDummyContext, "warning",
    "OSS_GLOBAL_PASSWORD is deprecated. Use `CONFIG SET oss-global-password <password>` instead");
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetString(ac, &realConfig->globalPass, NULL, 0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getGlobalPass) {
  RedisModule_Log(RSDummyContext, "warning",
    "OSS_GLOBAL_PASSWORD is deprecated. Use `CONFIG GET oss-global-password` instead");
  return sdsnew("Password: *******");
}

// oss-global-password
CONFIG_API_STRING_SETTER(set_oss_global_password);

RedisModuleString * get_oss_global_password(const char *get_oss_global_password,
                                            void *privdata) {
  return RedisModule_CreateString(NULL, "Password: *******", 17);
}

// CONN_PER_SHARD
int triggerConnPerShard(RSConfig *config) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  size_t connPerShard;
  if (realConfig->connPerShard != 0) {
    connPerShard = realConfig->connPerShard;
  } else {
    connPerShard = config->numWorkerThreads + 1;
  }
  MR_UpdateConnPerShard(connPerShard);
  return REDISMODULE_OK;
}

CONFIG_SETTER(setConnPerShard) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetSize(ac, &realConfig->connPerShard, AC_F_GE0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  return triggerConnPerShard(config);
}

CONFIG_GETTER(getConnPerShard) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  return sdsfromlonglong(realConfig->connPerShard);
}

// CURSOR_REPLY_THRESHOLD
CONFIG_SETTER(setCursorReplyThreshold) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetSize(ac, &realConfig->cursorReplyThreshold, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getCursorReplyThreshold) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  return sdsfromlonglong(realConfig->cursorReplyThreshold);
}

// SEARCH_THREADS
CONFIG_SETTER(setSearchThreads) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  int acrc = AC_GetSize(ac, &realConfig->coordinatorPoolSize, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getSearchThreads) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  return sdsfromlonglong(realConfig->coordinatorPoolSize);
}

// search-threads
CONFIG_API_NUMERIC_SETTER(set_search_threads);
CONFIG_API_NUMERIC_GETTER(get_search_threads);

// TOPOLOGY_VALIDATION_TIMEOUT
CONFIG_SETTER(setTopologyValidationTimeout) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  int acrc = AC_GetSize(ac, &realConfig->topologyValidationTimeoutMS, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getTopologyValidationTimeout) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  return sdsfromlonglong(realConfig->topologyValidationTimeoutMS);
}

// ACL_USERNAME
CONFIG_GETTER(getOSSACLUsername) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  return sdsnew(realConfig->aclUsername);
}

CONFIG_SETTER(setOSSACLUsername) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  int acrc = AC_GetString(ac, &realConfig->aclUsername, NULL, 0);
  RETURN_STATUS(acrc);
}

// topology-validation-timeout
int set_topology_validation_timeout(const char *set_topology_validation_timeout,
                      long long val, void *privdata, RedisModuleString **err) {
  SearchClusterConfig *realConfig = (SearchClusterConfig *)privdata;
  realConfig->topologyValidationTimeoutMS = val;
  return REDISMODULE_OK;
}

long long get_topology_validation_timeout(
                const char *get_topology_validation_timeout, void *privdata) {
  SearchClusterConfig *realConfig = (SearchClusterConfig *)privdata;
  return realConfig->topologyValidationTimeoutMS;
}

static RSConfigOptions clusterOptions_g = {
    .vars =
        {
            {.name = "PARTITIONS",
             .helpText = "Number of RediSearch partitions to use",
             .setValue = setNumPartitions,
             .getValue = getNumPartitions,
             .flags = RSCONFIGVAR_F_IMMUTABLE},
            {.name = "TIMEOUT",
             .helpText = "Cluster synchronization timeout",
             .setValue = setClusterTimeout,
             .getValue = getClusterTimeout},
            {.name = "OSS_GLOBAL_PASSWORD",
             .helpText = "Global oss cluster password that will be used to connect to other shards",
             .setValue = setGlobalPass,
             .getValue = getGlobalPass,
             .flags = RSCONFIGVAR_F_IMMUTABLE},
            {.name = "CONN_PER_SHARD",
             .helpText = "Number of connections to each shard in the cluster. Default to 0. "
                         "If 0, the number of connections is set to `WORKERS` + 1.",
             .setValue = setConnPerShard,
             .getValue = getConnPerShard,},
            {.name = "CURSOR_REPLY_THRESHOLD",
             .helpText = "Maximum number of replies to accumulate before triggering `_FT.CURSOR READ` on the shards",
             .setValue = setCursorReplyThreshold,
             .getValue = getCursorReplyThreshold,},
            {.name = "SEARCH_THREADS",
             .helpText = "Sets the number of search threads in the coordinator thread pool",
             .setValue = setSearchThreads,
             .getValue = getSearchThreads,
             .flags = RSCONFIGVAR_F_IMMUTABLE,},
            {.name = "TOPOLOGY_VALIDATION_TIMEOUT",
             .helpText = "Sets the timeout for topology validation (in milliseconds). After this timeout, "
                         "any pending requests will be processed, even if the topology is not fully connected. "
                         "Default is 30000 (30 seconds). 0 means no timeout.",
             .setValue = setTopologyValidationTimeout,
             .getValue = getTopologyValidationTimeout,},
             {.name = "OSS_ACL_USERNAME",
             .helpText = "Set the username for the ACL user used by the coordinator to connect to the shards on OSS cluster.",
             .setValue = setOSSACLUsername,
             .getValue = getOSSACLUsername,
             .flags = RSCONFIGVAR_F_IMMUTABLE},
            {.name = NULL}
            // fin
        }
    // fin
};

SearchClusterConfig clusterConfig = {0};

/* Detect the cluster type, by trying to see if we are running inside RLEC.
 * If we cannot determine, we return OSS type anyway
 */
MRClusterType DetectClusterType() {
  RedisModuleCallReply *r = RedisModule_Call(RSDummyContext, "INFO", "c", "SERVER");
  MRClusterType ret = ClusterType_RedisOSS;

  if (r && RedisModule_CallReplyType(r) == REDISMODULE_REPLY_STRING) {
    size_t len;
    // INFO SERVER should contain the term rlec_version in it if we are inside an RLEC shard

    const char *str = RedisModule_CallReplyStringPtr(r, &len);
    if (str) {

      if (memmem(str, len, "rlec_version", strlen("rlec_version")) != NULL) {
        ret = ClusterType_RedisLabs;
      }
    }
    RedisModule_FreeCallReply(r);
  }
  // RedisModule_ThreadSafeContextUnlock(ctx);
  return ret;
}

RSConfigOptions *GetClusterConfigOptions(void) {
  return &clusterOptions_g;
}

void ClusterConfig_RegisterTriggers(void) {
  const char *connPerShardConfigs[] = {"WORKERS", NULL};
  RSConfigExternalTrigger_Register(triggerConnPerShard, connPerShardConfigs);
}

int RegisterClusterModuleConfig(RedisModuleCtx *ctx) {
  if (RedisModule_RegisterNumericConfig(
        ctx, "search-threads", COORDINATOR_POOL_DEFAULT_SIZE,
        REDISMODULE_CONFIG_IMMUTABLE, 1, LLONG_MAX, get_search_threads,
        set_search_threads, NULL,
        (void*)&(clusterConfig.coordinatorPoolSize)) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  } else {
    RedisModule_Log(ctx, "notice", "search-threads registered");
  }

  if (RedisModule_RegisterNumericConfig (
        ctx, "topology-validation-timeout", DEFAULT_TOPOLOGY_VALIDATION_TIMEOUT,
        REDISMODULE_CONFIG_DEFAULT, 0, LLONG_MAX, get_topology_validation_timeout,
        set_topology_validation_timeout, NULL, (void*)&clusterConfig) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  } else {
    RedisModule_Log(ctx, "notice", "topology-validation-timeout registered");
  }

  // Check if oss-global-password is already registered, because it is shared
  // between RediSearch and RedisTimeSeries
  // TODO: We need to use REDISMODULE_CONFIG_UNPREFIXED flag here,
  // but it is not available in Redis 7.x
  if (clusterConfig.type == ClusterType_RedisOSS && !OssGlobalPasswordConfigExists(ctx)) {
    if (RedisModule_RegisterStringConfig (
          ctx, "oss-global-password", "",
          REDISMODULE_CONFIG_IMMUTABLE, get_oss_global_password,
          set_oss_global_password, NULL,
          (void*)&clusterConfig.globalPass) == REDISMODULE_ERR) {
      RedisModule_Log(ctx, "notice", "oss-global-password NOT registered");
      return REDISMODULE_ERR;
    } else {
      RedisModule_Log(ctx, "notice", "oss-global-password registered");
    }
  }

  return REDISMODULE_OK;
}
