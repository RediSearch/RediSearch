/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdio.h>
#include <redismodule.h>
#include <unistd.h>
#include <string.h>
#include "assert.h"
#include "test.h"
#include "args.h"

int testCArgs() {
  ArgsCursor ac;
  const char *args[] = {"hello",  "stringArg",   "goodbye",        "666", "cute", "3.14",
                        "toobig", "99999999999", "negative_nancy", "-1"};
  size_t argc = sizeof(args) / sizeof(args[0]);
  ArgsCursor_InitCString(&ac, args, argc);
  ASSERT(ac.offset == 0);
  ASSERT(ac.argc == argc);

  // Get the string
  const char *arg;
  ASSERT(!AC_GetString(&ac, &arg, NULL, 0));
  ASSERT(!strcmp(arg, "hello"));

  // Get the next string
  ASSERT(!AC_GetString(&ac, &arg, NULL, 0));
  ASSERT(!strcmp(arg, "stringArg"));

  // Get the goodbye arg
  ASSERT(!AC_GetString(&ac, &arg, NULL, 0));
  ASSERT(!strcmp("goodbye", arg));

  int intArg = 0;
  ASSERT(!AC_GetInt(&ac, &intArg, 0));
  ASSERT(666 == intArg);

  double dArg = 0.0;
  ASSERT(!AC_GetString(&ac, &arg, NULL, 0));
  ASSERT(!strcmp("cute", arg));

  ASSERT(!AC_GetDouble(&ac, &dArg, 0));
  ASSERT(3.14 == dArg);

  // Now let's work on errors
  ASSERT(!AC_GetString(&ac, &arg, NULL, 0));
  ASSERT(!strcmp("toobig", arg));

  ASSERT(AC_ERR_ELIMIT == AC_GetInt(&ac, &intArg, 0));

  AC_Advance(&ac);  // skip anyway

  ASSERT(!AC_GetString(&ac, &arg, NULL, 0));
  ASSERT(!strcmp("negative_nancy", arg));

  // Negative args
  ASSERT(AC_ERR_ELIMIT == AC_GetInt(&ac, &intArg, AC_F_GE0));
  ASSERT(AC_ERR_ELIMIT == AC_GetInt(&ac, &intArg, AC_F_GE1));

  // Parse args[1] as a number
  ac.offset = 1;
  ASSERT(AC_ERR_PARSE == AC_GetInt(&ac, &intArg, 0));
  ASSERT(AC_ERR_PARSE == AC_GetDouble(&ac, &dArg, 0));
  return 0;
}

static int testTypeConversion() {
  const char *objs[] = {NULL};
  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, objs, 1);
#define PREP_ARG(arg) \
  ac.objs[0] = arg;   \
  ac.offset = 0;      \
  ac.argc = 1;

  int intArg;
  PREP_ARG("3.14");
  // Try to parse the double as an int
  ASSERT(AC_ERR_PARSE == AC_GetInt(&ac, &intArg, 0));
  // Same, but with coalesce
  ASSERT(0 == AC_GetInt(&ac, &intArg, AC_F_COALESCE));

  unsigned uArg;
  PREP_ARG("0");
  ASSERT(AC_ERR_ELIMIT == AC_GetUnsigned(&ac, &uArg, AC_F_GE1));
  ASSERT(0 == AC_GetUnsigned(&ac, &uArg, AC_F_GE0));

  // negative arguments fail by default on unsigned conversions. no overflow
  PREP_ARG("-1");
  ASSERT(AC_ERR_ELIMIT == AC_GetUnsigned(&ac, &uArg, 0));
  return 0;
}

// Integer conversion through the double fallback, at and beyond the
// representable range.
static int testNumericConversionLimits() {
  const char *objs[] = {NULL};
  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, objs, 1);

  long long llArg;
  // nan has no meaningful integer value under either flag
  PREP_ARG("nan");
  ASSERT(AC_ERR_PARSE == AC_GetLongLong(&ac, &llArg, 0));
  ASSERT(AC_ERR_PARSE == AC_GetLongLong(&ac, &llArg, AC_F_COALESCE));

  // Out-of-range doubles: coalescing saturates by sign...
  PREP_ARG("1e30");
  ASSERT(0 == AC_GetLongLong(&ac, &llArg, AC_F_COALESCE));
  ASSERT(LLONG_MAX == llArg);
  PREP_ARG("-1e30");
  ASSERT(0 == AC_GetLongLong(&ac, &llArg, AC_F_COALESCE));
  ASSERT(LLONG_MIN == llArg);

  // ...and plain conversion rejects
  PREP_ARG("1e30");
  ASSERT(AC_ERR_PARSE == AC_GetLongLong(&ac, &llArg, 0));
  PREP_ARG("-1e30");
  ASSERT(AC_ERR_PARSE == AC_GetLongLong(&ac, &llArg, 0));

  // 2^63 is the exclusive bound: rejected plain, clamped when coalescing
  PREP_ARG("9223372036854775808");
  ASSERT(AC_ERR_PARSE == AC_GetLongLong(&ac, &llArg, 0));
  ASSERT(0 == AC_GetLongLong(&ac, &llArg, AC_F_COALESCE));
  ASSERT(LLONG_MAX == llArg);

  // -2^63 is exactly representable: accepted, via the double fallback (the
  // integer path treats the strtoll LLONG_MIN return value as overflow)
  PREP_ARG("-9223372036854775808");
  ASSERT(0 == AC_GetLongLong(&ac, &llArg, 0));
  ASSERT(LLONG_MIN == llArg);

  // Fractional input truncates only when coalescing
  PREP_ARG("3.99");
  ASSERT(AC_ERR_PARSE == AC_GetLongLong(&ac, &llArg, 0));
  ASSERT(0 == AC_GetLongLong(&ac, &llArg, AC_F_COALESCE));
  ASSERT(3 == llArg);
  return 0;
}

TEST_MAIN({
  TESTFUNC(testCArgs);
  TESTFUNC(testTypeConversion);
  TESTFUNC(testNumericConversionLimits);
})