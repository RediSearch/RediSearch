/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "util/dict.h"

#include <stddef.h>

typedef struct QueryError QueryError;
typedef struct RedisModuleString RedisModuleString;

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  PARAM_NONE = 0,
  PARAM_ANY,
  PARAM_TERM,
  PARAM_TERM_CASE,
  PARAM_SIZE,
  PARAM_NUMERIC,
  PARAM_NUMERIC_MIN_RANGE,
  PARAM_NUMERIC_MAX_RANGE,
  PARAM_GEO_COORD,
  PARAM_GEO_UNIT,
  PARAM_VEC,
  PARAM_WILDCARD,
} ParamType;

typedef struct Param {
  // Parameter name, borrowed from the parsed query text; not NUL-terminated
  const char *name;
  // Length of the parameter name
  size_t len;

  ParamType type;

  // The value the parameter will set when it is resolved
  void *target;
  // The length of the `target` value (if relevant for the parameter type)
  size_t *target_len;
  // The sign before $ sign in case of numeric range
  int sign;
} Param;

/* The params dict borrows its names and values; both must outlive the dict and anything that
 * resolved a parameter from it. Query requests satisfy this with their held argv. */
dict *Param_DictCreate();
int Param_DictAdd(dict *d, const char *name, size_t name_len, RedisModuleString *value, QueryError *status);
const char *Param_DictGet(dict *d, const char *name, size_t name_len, size_t *value_len, QueryError *status);
void Param_DictFree(dict *);
dict *Param_DictClone(dict *source);

#ifdef __cplusplus
}
#endif
