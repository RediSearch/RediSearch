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

int parseDoubleRange(const char *s, bool *inclusive, double *target, int isMin,
                      int sign, QueryError *status) {
  if (*s == '(') {
    *inclusive = false;
    s++;
  }
  if (isMin && (
        (sign == 1 && !strcasecmp(s, "-inf")) ||
        (sign == -1 && !strcasecmp((*s == '+' ? s + 1 : s), "inf")))) {
    *target = -INFINITY;
    return REDISMODULE_OK;
  } else if (!isMin && (
        (sign == 1 && !strcasecmp((*s == '+' ? s + 1 : s), "inf")) ||
        (sign == -1 && !strcasecmp(s, "-inf")))){
    *target = INFINITY;
    return REDISMODULE_OK;
  }
  char *endptr = NULL;
  errno = 0;
  *target = strtod(s, &endptr);
  if (*endptr != '\0' || *target == HUGE_VAL || *target == -HUGE_VAL) {
    QERR_MKBADARGS_FMT(status, isMin ? "Bad lower range" : "Bad upper range", ": %s", s);
    return REDISMODULE_ERR;
  }
  if(sign == -1) {
    *target = -(*target);
  }
  return REDISMODULE_OK;
}

/*
 *  Parse numeric filter arguments, in the form of:
 *  <fieldname> min max
 *
 *  By default, the interval specified by min and max is closed (inclusive).
 *  It is possible to specify an open interval (exclusive) by prefixing the score
 * with the character
 * (.
 *  For example: "score (1 5"
 *  Will return filter elements with 1 < score <= 5
 *
 *  min and max can be -inf and +inf
 *
 *  Returns a numeric filter on success, NULL if there was a problem with the
 * arguments
 */
NumericFilter *NumericFilter_LegacyParse(ArgsCursor *ac, bool *hasEmptyFilterValue, QueryError *status) {
  if (AC_NumRemaining(ac) < 3) {
    QERR_MKBADARGS(status, "FILTER requires 3 arguments");
    return NULL;
  }

  NumericFilter *nf = rm_calloc(1, sizeof(*nf));

  // make sure we have an index spec for this filter and it's indeed numeric
  nf->inclusiveMax = 1;
  nf->inclusiveMin = 1;
  nf->min = 0;
  nf->max = 0;
  // Store the field name at the field spec pointer, to validate later
  const char *fieldName = AC_GetStringNC(ac, NULL);
  nf->field.resolved = false;
  nf->field.u.name = NewHiddenString(fieldName, strlen(fieldName), false);

  // Parse the min range
  const char *s = AC_GetStringNC(ac, NULL);
  if (!*s) {
    *hasEmptyFilterValue = true;
  }
  if (parseDoubleRange(s, &nf->inclusiveMin, &nf->min, 1, 1, status) != REDISMODULE_OK) {
    NumericFilter_Free(nf);
    return NULL;
  }

  s = AC_GetStringNC(ac, NULL);
  if (!*s) {
    *hasEmptyFilterValue = true;
  }
  if (parseDoubleRange(s, &nf->inclusiveMax, &nf->max, 0, 1, status) != REDISMODULE_OK) {
    NumericFilter_Free(nf);
    return NULL;
  }
  return nf;
}

void NumericFilter_Free(NumericFilter *nf) {
  if (!nf->field.resolved && nf->field.u.name) {
    HiddenString_Free(nf->field.u.name, false);
  }
  rm_free(nf);
}

NumericFilter *NewNumericFilter(double min, double max, int inclusiveMin, int inclusiveMax,
                                bool asc) {
  NumericFilter *f = rm_malloc(sizeof(NumericFilter));

  f->min = min;
  f->max = max;
  f->field.u.name = NULL;
  f->field.resolved = false;
  f->inclusiveMax = inclusiveMax;
  f->inclusiveMin = inclusiveMin;
  f->geoFilter = NULL;
  f->asc = asc;
  f->offset = 0;
  f->limit = 0;
  return f;
}
