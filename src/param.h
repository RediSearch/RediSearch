#pragma once
#include <stddef.h>

typedef enum {
  PARAM_NONE = 0,
  PARAM_ANY,
  PARAM_TERM,
  PARAM_NUMERIC,
  PARAM_GEO_COORD,
  PARAM_GEO_UNIT,
} ParamType;

typedef struct Param {
  const char *name;
  size_t len;
  ParamType type;
  void *target;
  size_t *target_len;
} Param;

void Param_Free(Param *param);

