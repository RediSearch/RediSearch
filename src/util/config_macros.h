/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#define RETURN_PARSE_ERROR(rc)                                    \
  QueryError_SetError(status, QUERY_EPARSEARGS, AC_Strerror(rc)); \
  return REDISMODULE_ERR;

#define CHECK_RETURN_PARSE_ERROR(rc) \
  if (rc != AC_OK) {                 \
    RETURN_PARSE_ERROR(rc);          \
  }

#define RETURN_STATUS(rc)   \
  if (rc == AC_OK) {        \
    return REDISMODULE_OK;  \
  } else {                  \
    RETURN_PARSE_ERROR(rc); \
  }

#define CONFIG_SETTER(name) int name(RSConfig *config, ArgsCursor *ac, uint32_t externalTriggerId, QueryError *status)
#define CONFIG_GETTER(name) static sds name(const RSConfig *config)

#define CONFIG_BOOLEAN_GETTER(name, var, invert) \
  CONFIG_GETTER(name) {                          \
    int cv = config->var;                        \
    if (invert) {                                \
      cv = !cv;                                  \
    }                                            \
    return sdsnew(cv ? "true" : "false");        \
  }

#define CONFIG_BOOLEAN_SETTER(name, var)       \
  CONFIG_SETTER(name) {                        \
    const char *tf;                            \
    int acrc = AC_GetString(ac, &tf, NULL, 0); \
    CHECK_RETURN_PARSE_ERROR(acrc);            \
    if (!strcasecmp(tf, "true")) {             \
      config->var = 1;                         \
    } else if (!strcasecmp(tf, "false")) {     \
      config->var = 0;                         \
    } else {                                   \
      acrc = AC_ERR_PARSE;                     \
    }                                          \
    RETURN_STATUS(acrc);                       \
  }

// Define the getter and setter functions using Module Configurations API
#define CONFIG_API_STRING_GETTER(name) RedisModuleString * name(const char *name, void *privdata) 
#define CONFIG_API_NUMERIC_GETTER(name) long long name(const char *name, void *privdata)
#define CONFIG_API_ENUM_GETTER(name) int name(const char *name, void *privdata)

#define CONFIG_API_BOOL_GETTER(name, var)           \
static int name(const char *name, void *privdata) { \
  return RSGlobalConfig.var;                        \
}

#define CONFIG_API_STRING_SETTER(name) int name(const char *name, RedisModuleString *val, void *privdata, RedisModuleString **err)
#define CONFIG_API_NUMERIC_SETTER(name) int name(const char *name, long long val, void *privdata, RedisModuleString **err)
#define CONFIG_API_ENUM_SETTER(name) int name(const char *name, int val, void *privdata, RedisModuleString **err)

#define CONFIG_API_BOOL_SETTER(name, var)                                      \
static int name(const char *name, int val, void *privdata, RedisModuleString **err) { \
  RSGlobalConfig.var = val;                                                    \
  return REDISMODULE_OK;                                                       \
}

#define CONFIG_API_REGISTER_BOOL_CONFIG(ctx, name, getfn, setfn, default_val) \
  if(RedisModule_RegisterBoolConfig(                                          \
        ctx, name, default_val, REDISMODULE_CONFIG_DEFAULT,                   \
        getfn, setfn, NULL, NULL) == REDISMODULE_ERR) {                       \
  } else {                                                                    \
    RedisModule_Log(ctx, "notice", STRINGIFY(name)" registered");             \
  }

#ifdef RS_COORDINATOR
#define COORDINATOR_TRIGGER() RSGlobalConfigTriggers[externalTriggerId](config)
#else
#define COORDINATOR_TRIGGER()
#endif
