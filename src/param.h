#pragma once
#include <stddef.h>
#include "util/dict.h"
#include "query_error.h"

typedef enum {
  PARAM_NONE = 0,
  PARAM_ANY,
  PARAM_TERM,
  PARAM_NUMERIC,
  PARAM_GEO_COORD,
  PARAM_GEO_UNIT,
} ParamKind;

typedef struct Param {
  const char *name;
  size_t len;
  ParamKind kind;
  void *target;
} Param;

Param *NewParam(const char *name, size_t len, ParamKind kind);
void Param_Free(Param *param);

/*
 * Resolve the value of a param
 * Return 0 if not parameterized
 * Return 1 if value was resolved successfully
 * Return -1 if param is missing or its kind is wrong
 */
int Param_Resolve(Param *param, dict *params, QueryError *status);