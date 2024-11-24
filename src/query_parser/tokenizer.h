/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __QUERY_TOKENIZER_H__
#define __QUERY_TOKENIZER_H__

#include <stdlib.h>
#include <stdbool.h>
#include "../tokenize.h"
#include "VecSim/vec_sim_common.h"
#include "spec.h"

typedef enum {
  // Concrete types
  QT_TERM,
  QT_TERM_CASE,
  QT_NUMERIC,
  QT_SIZE,
  QT_WILDCARD,
  // Parameterized types
  QT_PARAM_ANY,
  QT_PARAM_TERM,
  QT_PARAM_TERM_CASE,
  QT_PARAM_NUMERIC,
  QT_PARAM_SIZE,
  QT_PARAM_NUMERIC_MIN_RANGE,
  QT_PARAM_NUMERIC_MAX_RANGE,
  QT_PARAM_GEO_COORD,
  QT_PARAM_GEO_UNIT,
  QT_PARAM_VEC,
  QT_PARAM_WILDCARD,
} QueryTokenType;

/* A token in the process of parsing a query. Unlike the document tokenizer,  it
works iteratively and is not callback based.  */
typedef struct {
  const char *s;
  int len;
  int pos;
  double numval;
  QueryTokenType type;
  int sign; // for numeric range, it stores the sign of the parameter
} QueryToken;

typedef struct {
  double num;
  int inclusive;
} RangeNumber;

typedef struct {
  VecSimRawParam param;
  bool needResolve;
} SingleVectorQueryParam;

typedef struct {
  QueryToken tok;
  const FieldSpec *fs;
} FieldName;

#endif
