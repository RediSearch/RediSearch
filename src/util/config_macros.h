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

#ifdef RS_COORDINATOR
#define COORDINATOR_TRIGGER() RSGlobalConfigTriggers[externalTriggerId](config)
#else
#define COORDINATOR_TRIGGER()
#endif
