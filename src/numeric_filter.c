/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "numeric_filter.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "rmutil/vector.h"
#include "query_param.h"
#include "fast_float/fast_float_strtod.h"

int parseDoubleRange(const char *s, double *target, int isMin,
                      int sign, QueryError *status) {
  if (isMin && (
        (sign == 1 && !strcasecmp(s, "-inf")) ||
        (sign == -1 && !strcasecmp((*s == '+' ? s + 1 : s), "inf")))) {
    *target = NF_NEGATIVE_INFINITY;
    return REDISMODULE_OK;
  } else if (!isMin && (
        (sign == 1 && !strcasecmp((*s == '+' ? s + 1 : s), "inf")) ||
        (sign == -1 && !strcasecmp(s, "-inf")))){
    *target = NF_INFINITY;
    return REDISMODULE_OK;
  }
  char *endptr = NULL;
  errno = 0;
  *target = fast_float_strtod(s, &endptr);
  if (*endptr != '\0' || *target == HUGE_VAL || *target == -HUGE_VAL) {
    QERR_MKBADARGS_FMT(status, "Bad %s range: %s", isMin ? "lower" : "upper", s);
    return REDISMODULE_ERR;
  }
  if(sign == -1) {
    *target = -(*target);
  }
  return REDISMODULE_OK;
}

void NumericFilter_Free(NumericFilter *nf) {
  rm_free(nf);
}

NumericFilter *NewNumericFilter(double min, double max, int inclusiveMin, int inclusiveMax,
                                bool asc) {
  NumericFilter *f = rm_malloc(sizeof(NumericFilter));

  f->min = min;
  f->max = max;
  f->field = NULL;
  f->inclusiveMax = inclusiveMax;
  f->inclusiveMin = inclusiveMin;
  f->geoFilter = NULL;
  f->asc = asc;
  f->offset = 0;
  f->limit = 0;
  return f;
}
