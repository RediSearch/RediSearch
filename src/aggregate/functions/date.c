#include <util/minmax.h>
#include <util/array.h>
#include <util/block_alloc.h>
#include <aggregate/expr/expression.h>
#include <ctype.h>

#include "function.h"

#define ISOFMT "%FT%TZ"
#define ISOFMT_LEN sizeof(ISOFMT) - 1

// TIME(propert, [fmt_string])
static int timeFormat(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc,
                      char **err) {

  VALIDATE_ARGS("time", 1, 2, err);
  const char *fmt = ISOFMT;
  if (argc == 2) {
    VALIDATE_ARG_TYPE("time", argv, 1, RSValue_String);
    fmt = RSValue_StringPtrLen(&argv[1], NULL);
  }
  // Get the format
  static char timebuf[1024] = {0};  // Should be enough for any human time string
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
  char *buf = RSFunction_Strndup(ctx, timebuf, rv);

  // It will be released by the block allocator destruction, so we refer to it is a static string so
  // the value ref counter will not release it
  RSValue_SetConstString(result, buf, rv);
  return EXPR_EVAL_OK;
err:
  // on runtime error (bad formatting, etc) we just set the result to null

  RSValue_MakeReference(result, RS_NullVal());
  return EXPR_EVAL_OK;
}

static int parseTime(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc, char **err) {
  VALIDATE_ARGS("parse_time", 2, 2, err);
  VALIDATE_ARG_ISSTRING("parse_time", argv, 0);
  VALIDATE_ARG_ISSTRING("parse_time", argv, 1);

  char fmtbuf[1024] = {0};
  char valbuf[1024] = {0};
  size_t fmtlen;

  const char *origfmt = RSValue_StringPtrLen(&argv[0], &fmtlen);
  if (fmtlen > sizeof(fmtbuf)) {
    goto err;
  }

  size_t vallen;
  const char *origval = RSValue_StringPtrLen(&argv[1], &vallen);
  if (vallen > sizeof(valbuf)) {
    goto err;
  }
  memcpy(fmtbuf, origfmt, fmtlen);
  memcpy(valbuf, origval, vallen);

  struct tm tm = {0};
  char *rc = strptime(valbuf, fmtbuf, &tm);
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
  RSFunctionRegistry_RegisterFunction("timefmt", timeFormat, RSValue_String);
  RSFunctionRegistry_RegisterFunction("parse_time", parseTime, RSValue_Number);
}