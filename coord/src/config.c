/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "config.h"
#include "util/config_macros.h"
#include "rmr/endpoint.h"

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

CONFIG_SETTER(setGlobalPass) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetString(ac, &realConfig->globalPass, NULL, 0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getGlobalPass) {
  return sdsnew("Password: *******");
}

// CONN_PER_SHARD
CONFIG_SETTER(setConnPerShard) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetSize(ac, &realConfig->connPerShard, AC_F_GE0);
  RETURN_STATUS(acrc);
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
             .getValue = getGlobalPass},
            {.name = "CONN_PER_SHARD",
             .helpText = "Number of connections to each shard in the cluster",
             .setValue = setConnPerShard,
             .getValue = getConnPerShard,
             .flags = RSCONFIGVAR_F_IMMUTABLE},
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
