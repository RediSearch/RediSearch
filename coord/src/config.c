/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "config.h"
#include "rmr/endpoint.h"

#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "hiredis/hiredis.h"

#include <string.h>
#include <stdlib.h>

extern RedisModuleCtx *RSDummyContext;

#define CONFIG_SETTER(name) \
  static int name(RSConfig *config, ArgsCursor *ac, QueryError *status)

#define CONFIG_GETTER(name) static sds name(const RSConfig *config)
#define CONFIG_FROM_RSCONFIG(c) ((SearchClusterConfig *)(c)->chainedConfig)

static SearchClusterConfig* getOrCreateRealConfig(RSConfig *config){
  if(!CONFIG_FROM_RSCONFIG(config)){
    config->chainedConfig = &clusterConfig;
  }
  return CONFIG_FROM_RSCONFIG(config);
}

// PARTITIONS
CONFIG_SETTER(setNumPartitions) {
  RedisModuleString *s;
  int acrc = AC_GetRString(ac, &s, 0);
  if (acrc != AC_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, AC_Strerror(acrc));
    return REDISMODULE_ERR;
  }
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  const char *sstr = RedisModule_StringPtrLen(s, NULL);
  if (!strcasecmp(sstr, "AUTO")) {
    realConfig->numPartitions = 0;
  } else {
    long long ll = 0;
    if (RedisModule_StringToLongLong(s, &ll) != REDISMODULE_OK || ll < 0) {
      QueryError_SetError(status, QUERY_EPARSEARGS, NULL);
      return REDISMODULE_ERR;
    }
    realConfig->numPartitions = ll;
  }
  return REDISMODULE_OK;
}

CONFIG_GETTER(getNumPartitions) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  sds ss = sdsempty();
  return sdscatprintf(ss, "%ld", realConfig->numPartitions);
}

// TIMEOUT
CONFIG_SETTER(setTimeout) {
  long long ll;
  int acrc = AC_GetLongLong(ac, &ll, 0);
  if (acrc != AC_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, AC_Strerror(acrc));
    return REDISMODULE_ERR;
  }
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  if (ll < 0) {
    QueryError_SetError(status, QUERY_EPARSEARGS, NULL);
    return REDISMODULE_ERR;
  }
  if (ll > 0) {
    realConfig->timeoutMS = ll;
  }
  return REDISMODULE_OK;
}

CONFIG_GETTER(getTimeout) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  sds ss = sdsempty();
  return sdscatprintf(ss, "%d", realConfig->timeoutMS);
}

CONFIG_SETTER(setGlobalPass) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetString(ac, &realConfig->globalPass, NULL, 0);
  if (acrc != AC_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, NULL);
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

CONFIG_GETTER(getGlobalPass) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "Password: *******");
}

// CONN_PER_SHARD
CONFIG_SETTER(setConnPerShard) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetSize(ac, &realConfig->connPerShard, AC_F_GE0);
  if (acrc != AC_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, AC_Strerror(acrc));
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

CONFIG_GETTER(getConnPerShard) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  sds ss = sdsempty();
  return sdscatprintf(ss, "%zu", realConfig->connPerShard);
}

// CURSOR_REPLY_THRESHOLD
CONFIG_SETTER(setCursorReplyThreshold) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig(config);
  int acrc = AC_GetSize(ac, &realConfig->cursorReplyThreshold, AC_F_GE1);
  if (acrc != AC_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, AC_Strerror(acrc));
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

CONFIG_GETTER(getCursorReplyThreshold) {
  SearchClusterConfig *realConfig = getOrCreateRealConfig((RSConfig *)config);
  return sdsfromlonglong(realConfig->cursorReplyThreshold);
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
             .setValue = setTimeout,
             .getValue = getTimeout},
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
