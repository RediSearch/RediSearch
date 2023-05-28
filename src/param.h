/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "query_error.h"
#include "util/dict.h"

#include <stddef.h>

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
  // Parameter name
  const char *name;
  // Length of the parameter name
  size_t len;

  ParamType type;

  // The value the parameter will set when it is resolved
  void *target;
  // The length of the `target` value (if relevant for the parameter type)
  size_t *target_len;
} Param;

void Param_FreeInternal(Param *param);

dict *Param_DictCreate();
int Param_DictAdd(dict *d, const char *name, const char *value, size_t value_len, QueryError *status);
const char *Param_DictGet(dict *d, const char *name, size_t *value_len, QueryError *status);
void Param_DictFree(dict *);
