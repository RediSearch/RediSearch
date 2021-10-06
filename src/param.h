#pragma once
#include <stddef.h>

typedef enum {
  PARAM_NONE = 0,
  PARAM_ANY,
  PARAM_TERM,
  PARAM_TERM_CASE,
  PARAM_NUMERIC,
  PARAM_NUMERIC_MIN_RANGE,
  PARAM_NUMERIC_MAX_RANGE,
  PARAM_GEO_COORD,
  PARAM_GEO_UNIT,
  PARAM_VEC_SIM_TYPE,
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

