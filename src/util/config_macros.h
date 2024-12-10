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
#define CONFIG_API_ENUM_GETTER(getfn) int getfn(const char *name, void *privdata)

#define CONFIG_API_STRING_GETTER(getfn)                                  \
RedisModuleString * getfn(const char *name, void *privdata) {            \
  char *str = *(char **)privdata;                                       \
  return str ? RedisModule_CreateString(NULL, str, strlen(str)) : NULL; \
}

#define CONFIG_API_BOOL_GETTER(getfn, var, invert)   \
static int getfn(const char *name, void *privdata) { \
  if (invert) {                                     \
    return !RSGlobalConfig.var;                     \
  } else {                                          \
  return RSGlobalConfig.var;                        \
  }                                                 \
}

#define CONFIG_API_ENUM_SETTER(setfn) int setfn(const char *name, int val, void *privdata, RedisModuleString **err)

#define CONFIG_API_STRING_SETTER(setfn) int setfn(const char *name,            \
            RedisModuleString *val, void *privdata, RedisModuleString **err) { \
  char **ptr = (char **)privdata;                                              \
  if (val) {                                                                   \
    size_t len;                                                                \
    const char *ret = RedisModule_StringPtrLen(val, &len);                     \
    if (len > 0) {                                                             \
      *ptr = rm_strndup(ret, len);                                             \
    }                                                                          \
  }                                                                            \
  return REDISMODULE_OK;                                                       \
}

#define CONFIG_API_BOOL_SETTER(setfn, var)                  \
static int setfn(const char *name, int val, void *privdata, \
                RedisModuleString **err) {                 \
  RSGlobalConfig.var = val;                                \
  return REDISMODULE_OK;                                   \
}

#define CONFIG_API_REGISTER_BOOL_CONFIG(ctx, name, getfn, setfn, default_val, flags) \
  if(RedisModule_RegisterBoolConfig(                                           \
        ctx, name, default_val, flags,                                         \
        getfn, setfn, NULL, NULL) == REDISMODULE_ERR) {                        \
        return REDISMODULE_ERR;                                                \
  }

#define COORDINATOR_TRIGGER() RSGlobalConfigTriggers[externalTriggerId](config)
