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
#include "../module.h"

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

CONFIG_SETTER(setGlobalPass) {
  RedisModule_Log(RSDummyContext, "warning",
    "OSS_GLOBAL_PASSWORD is deprecated. Use `CONFIG SET search-oss-global-password <password>` instead");
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetString(ac, &realConfig->globalPass, NULL, 0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getGlobalPass) {
  RedisModule_Log(RSDummyContext, "warning",
    "OSS_GLOBAL_PASSWORD is deprecated. Use `CONFIG GET search-oss-global-password` instead");
  return sdsnew("Password: *******");
}

// global-password
int set_immutable_cluster_string_config(const char *name, RedisModuleString *val,
                                      void *privdata, RedisModuleString **err) {
  REDISMODULE_NOT_USED(name);
  REDISMODULE_NOT_USED(err);
  RSConfig *config = (RSConfig *)privdata;
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  char **ptr = (char **)privdata;
  if (val) {
    size_t len;
    const char *ret = RedisModule_StringPtrLen(val, &len);
    if (len > 0) {
      *ptr = rm_strndup(ret, len);
    }
  }
  return REDISMODULE_OK;
}

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
int set_search_threads(const char *name, long long val, void *privdata,
                  RedisModuleString **err) {
  RSConfig *config = (RSConfig *)privdata;
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  realConfig->coordinatorPoolSize = (size_t)val;
  return REDISMODULE_OK;
}

long long get_search_threads(const char *name, void *privdata) {
  RSConfig *config = (RSConfig *)privdata;
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  return (long long)realConfig->coordinatorPoolSize;
}

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

// acl-username
RedisModuleString * get_oss_acl_username(const char *name, void *privdata) {
  char *str = *(char **)privdata;  
  config_oss_acl_username = RedisModule_CreateString(NULL, str, strlen(str));
  return config_oss_acl_username;
}

// topology-validation-timeout
int set_topology_validation_timeout(const char *name,
                      long long val, void *privdata, RedisModuleString **err) {
  RSConfig *config = (RSConfig *)privdata;
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  realConfig->topologyValidationTimeoutMS = val;
  return REDISMODULE_OK;
}

long long get_topology_validation_timeout(
                const char *name, void *privdata) {
  RSConfig *config = (RSConfig *)privdata;
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
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
  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-threads", COORDINATOR_POOL_DEFAULT_SIZE,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_search_threads, set_search_threads, NULL,
      (void*)&RSGlobalConfig
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig (
      ctx, "search-topology-validation-timeout", DEFAULT_TOPOLOGY_VALIDATION_TIMEOUT,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 0, LLONG_MAX,
      get_topology_validation_timeout, set_topology_validation_timeout, NULL,
      (void*)&RSGlobalConfig
    )
  )

  if (clusterConfig.type == ClusterType_RedisOSS) {
    if (RedisModule_RegisterStringConfig (
          ctx, "search-oss-global-password", "",
          REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED | REDISMODULE_CONFIG_SENSITIVE,
          get_oss_global_password, set_immutable_cluster_string_config, NULL,
          (void*)&clusterConfig.globalPass) == REDISMODULE_ERR) {
      return REDISMODULE_ERR;
    }
  }

  if (clusterConfig.type == ClusterType_RedisOSS) {
    if (RedisModule_RegisterStringConfig (
          ctx, "search-oss-acl-username", DEFAULT_ACL_USERNAME,
          REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED,
          get_oss_acl_username, set_immutable_cluster_string_config, NULL,
          (void*)&clusterConfig.aclUsername) == REDISMODULE_ERR) {
      return REDISMODULE_ERR;
    }
  }

  return REDISMODULE_OK;
}
