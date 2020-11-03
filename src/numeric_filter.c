
#include "numeric_filter.h"

#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "rmutil/vector.h"

#include <cmath>

///////////////////////////////////////////////////////////////////////////////////////////////

int NumericFilter::parseDoubleRange(const char *s, bool &inclusive, double &target, bool isMin,
                                    QueryError *status) {
  if (isMin && !strcasecmp(s, "-inf")) {
    target = NF_NEGATIVE_INFINITY;
    return REDISMODULE_OK;
  } else if (!isMin && !strcasecmp(s, "+inf")) {
    target = NF_INFINITY;
    return REDISMODULE_OK;
  }
  if (*s == '(') {
    inclusive = false;
    s++;
  }
  char *endptr = NULL;
  errno = 0;
  target = strtod(s, &endptr);
  if (*endptr != '\0' || target == HUGE_VAL || target == -HUGE_VAL) {
    QERR_MKBADARGS_FMT(status, "Bad %s range: %s", isMin ? "lower" : "upper", s);
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

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

NumericFilter::NumericFilter(ArgsCursor *ac, QueryError *status) {
  if (AC_NumRemaining(ac) < 3) {
    QERR_MKBADARGS_FMT(status, "FILTER requires 3 arguments");
    throw Error("FILTER requires 3 arguments");
  }

  // make sure we have an index spec for this filter and it's indeed numeric
  inclusiveMax = true;
  inclusiveMin = true;
  min = 0;
  max = 0;
  fieldName = rm_strdup(AC_GetStringNC(ac, NULL));

  // Parse the min range
  const char *s = AC_GetStringNC(ac, NULL);
  if (parseDoubleRange(s, inclusiveMin, min, true, status) != REDISMODULE_OK) {
    throw Error(status->detail);
  }
  s = AC_GetStringNC(ac, NULL);
  if (parseDoubleRange(s, inclusiveMax, max, false, status) != REDISMODULE_OK) {
    throw Error(status->detail);
  }
}

//---------------------------------------------------------------------------------------------

NumericFilter::~NumericFilter() {
  if (fieldName) {
    rm_free((char *)fieldName);
  }
}

//---------------------------------------------------------------------------------------------

NumericFilter::NumericFilter(double min_, double max_, bool inclusiveMin_, bool inclusiveMax_) {
  min = min_;
  max = max_;
  fieldName = NULL;
  inclusiveMax = inclusiveMax_;
  inclusiveMin = inclusiveMin_;
}

//---------------------------------------------------------------------------------------------

NumericFilter::NumericFilter(const NumericFilter &nf) : min(nf.min), max(nf.max), 
    inclusiveMin(nf.inclusiveMin), inclusiveMax(nf.inclusiveMax) {
  if (nf.fieldName) {
    fieldName = rm_strdup(nf.fieldName);
  } else {
    fieldName = NULL;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
