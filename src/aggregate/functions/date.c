/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <util/minmax.h>
#include <util/array.h>
#include <util/block_alloc.h>
#include <aggregate/expr/expression.h>
#include <ctype.h>

#include "function.h"

#define ISOFMT "%FT%TZ"
#define ISOFMT_LEN sizeof(ISOFMT) - 1

// TIME(property, [fmt_string])
static int timeFormat(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {
  const char *fmt = ISOFMT;
  if (argc == 2) {
    VALIDATE_ARG_TYPE("time", argv, 1, RSValue_String);
    fmt = RSValue_StringPtrLen(&argv[1], NULL);
  }
  // Get the format
  char timebuf[1024] = {0};  // Should be enough for any human time string
  double n;
  // value is not a number
  if (!RSValue_ToNumber(&argv[0], &n)) {
    goto err;
  }
  time_t tt = (time_t)n;
  struct tm tm;
  if (!gmtime_r(&tt, &tm)) {
    // could not convert value to timestamp
    goto err;
  }

  size_t rv = strftime(timebuf, sizeof timebuf, fmt, &tm);
  if (rv == 0) {
    // invalid format
    goto err;
  }

  // Finally, allocate a buffer to store the time!
  char *buf = ExprEval_Strndup(ctx, timebuf, rv);

  // It will be released by the block allocator destruction, so we refer to it is a static string so
  // the value ref counter will not release it
  RSValue_SetConstString(result, buf, rv);
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

/* Fast alternative to mktime which is dog slow. From:
https://gmbabar.wordpress.com/2010/12/01/mktime-slow-use-custom-function/ */
// Fix and performance improvements:
// https://godbolt.org/z/qscb5d9dT
// https://quick-bench.com/q/oTV4_9uVqPTcrj2fpDEvbbMzZ48
// https://quick-bench.com/q/2Bc8WY1Ys0vmbp-HPagWFxu81jI
static time_t fast_timegm(const struct tm *ltm) {
  long years = ltm->tm_year - 70; // tm->tm_year is from 1900, epoch is from 1970.
  long leaps = (years + 1) / 4;   // number of leap years from 1970, not including the current year.
                                  // correct until 2100.

  // `ltm->tm_yday` is the number of days since January 1st of the current year (0-365).
  // It includes the leap day if the current year is a leap year.
  long days = ltm->tm_yday + (years * 365) + leaps;

  return (days * (24 * 60 * 60)) + (ltm->tm_hour * (60 * 60)) + (ltm->tm_min * 60) + ltm->tm_sec;
}

static int func_hour(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {

  double d;
  if (!RSValue_ToNumber(&argv[0], &d) || d < 0) {
    goto err;
  }
  time_t ts = (time_t)d;
  struct tm tmm;

  gmtime_r(&ts, &tmm);
  tmm.tm_sec = 0;
  tmm.tm_min = 0;
  ts = fast_timegm(&tmm);
  RSValue_SetNumber(result, (double)ts);

  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

static int func_minute(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {

  double d;
  if (!RSValue_ToNumber(&argv[0], &d) || d < 0) {
    goto err;
  }
  RSValue_SetNumber(result, floor(d - fmod(d, 60)));
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

/* Round timestamp to its day start */
static int func_day(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {

  double d;
  if (!RSValue_ToNumber(&argv[0], &d) || d < 0) {
    goto err;
  }
  time_t ts = (time_t)d;
  struct tm tmm;

  gmtime_r(&ts, &tmm);
  tmm.tm_sec = 0;
  tmm.tm_hour = 0;
  tmm.tm_min = 0;
  ts = fast_timegm(&tmm);
  RSValue_SetNumber(result, (double)ts);
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

static int func_dayofmonth(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {

  double d;
  if (!RSValue_ToNumber(&argv[0], &d) || d < 0) {
    goto err;
  }
  time_t ts = (time_t)d;
  struct tm tmm;
  gmtime_r(&ts, &tmm);

  RSValue_SetNumber(result, (double)tmm.tm_mday);
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

static int func_dayofweek(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {

  double d;
  if (!RSValue_ToNumber(&argv[0], &d) || d < 0) {
    goto err;
  }
  time_t ts = (time_t)d;
  struct tm tmm;
  gmtime_r(&ts, &tmm);

  RSValue_SetNumber(result, (double)tmm.tm_wday);
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

static int func_dayofyear(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {

  double d;
  if (!RSValue_ToNumber(&argv[0], &d) || d < 0) {
    goto err;
  }
  time_t ts = (time_t)d;
  struct tm tmm;
  gmtime_r(&ts, &tmm);

  RSValue_SetNumber(result, (double)tmm.tm_yday);
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

static int func_year(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {

  double d;
  if (!RSValue_ToNumber(&argv[0], &d) || d < 0) {
    goto err;
  }
  time_t ts = (time_t)d;
  struct tm tmm;
  gmtime_r(&ts, &tmm);

  RSValue_SetNumber(result, (double)tmm.tm_year + 1900);
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null
  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

/* Round a timestamp to the beginning of the month */
static int func_month(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {

  double d;
  if (!RSValue_ToNumber(&argv[0], &d) || d < 0) {
    goto err;
  }
  time_t ts = (time_t)d;
  struct tm tmm;
  gmtime_r(&ts, &tmm);
  tmm.tm_sec = 0;
  tmm.tm_hour = 0;
  tmm.tm_min = 0;
  tmm.tm_yday -= tmm.tm_mday - 1; // set to first day of month
  ts = fast_timegm(&tmm);
  RSValue_SetNumber(result, (double)ts);
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

static int func_monthofyear(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {

  double d;
  if (!RSValue_ToNumber(&argv[0], &d) || d < 0) {
    goto err;
  }
  time_t ts = (time_t)d;
  struct tm tmm;
  gmtime_r(&ts, &tmm);
  RSValue_SetNumber(result, (double)tmm.tm_mon);
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

static int parseTime(ExprEval *ctx, RSValue *argv, size_t argc, RSValue *result) {
  VALIDATE_ARG_ISSTRING("parsetime", argv, 0);
  VALIDATE_ARG_ISSTRING("parsetime", argv, 1);

  const char *val = RSValue_StringPtrLen(&argv[0], NULL);
  const char *fmt = RSValue_StringPtrLen(&argv[1], NULL);

  struct tm tm = {0};
  char *rc = strptime(val, fmt, &tm);
  if (rc == NULL) {
    goto err;
  }
  time_t rv = timegm(&tm);
  RSValue_SetNumber(result, rv);
  return EXPR_EVAL_OK;

err:
  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

void RegisterDateFunctions() {
  RSFunctionRegistry_RegisterFunction("timefmt", timeFormat, RSValue_String, 1, 2);
  RSFunctionRegistry_RegisterFunction("parsetime", parseTime, RSValue_Number, 2, 2);
  RSFunctionRegistry_RegisterFunction("hour", func_hour, RSValue_Number, 1, 1);
  RSFunctionRegistry_RegisterFunction("minute", func_minute, RSValue_Number, 1, 1);
  RSFunctionRegistry_RegisterFunction("day", func_day, RSValue_Number, 1, 1);
  RSFunctionRegistry_RegisterFunction("month", func_month, RSValue_Number, 1, 1);
  RSFunctionRegistry_RegisterFunction("monthofyear", func_monthofyear, RSValue_Number, 1, 1);

  RSFunctionRegistry_RegisterFunction("year", func_year, RSValue_Number, 1, 1);
  RSFunctionRegistry_RegisterFunction("dayofmonth", func_dayofmonth, RSValue_Number, 1, 1);
  RSFunctionRegistry_RegisterFunction("dayofweek", func_dayofweek, RSValue_Number, 1, 1);
  RSFunctionRegistry_RegisterFunction("dayofyear", func_dayofyear, RSValue_Number, 1, 1);
}
