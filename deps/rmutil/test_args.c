/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <limits.h>
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

static int testLongLongFallbackBounds() {
  const char *objs[] = {NULL};
  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, objs, 1);
  long long value;
  const char *invalid[] = {"nan", "inf", "-inf", "9223372036854775808", "-9223372036854777856"};
  for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
    objs[0] = invalid[i];
    ac.offset = 0;
    value = 42;
    ASSERT(AC_ERR_PARSE == AC_GetLongLong(&ac, &value, 0));
    ASSERT(ac.offset == 0 && value == 42);
  }

  objs[0] = "nan";
  ASSERT(AC_ERR_PARSE == AC_GetLongLong(&ac, &value, AC_F_COALESCE));
  ASSERT(ac.offset == 0 && value == 42);

  const char *positive[] = {"9223372036854775808"};
  for (size_t i = 0; i < sizeof(positive) / sizeof(positive[0]); ++i) {
    objs[0] = positive[i];
    ac.offset = 0;
    ASSERT(AC_OK == AC_GetLongLong(&ac, &value, AC_F_COALESCE));
    ASSERT(ac.offset == 1 && value == LLONG_MAX);
  }

  const char *negative[] = {"-9223372036854777856"};
  for (size_t i = 0; i < sizeof(negative) / sizeof(negative[0]); ++i) {
    objs[0] = negative[i];
    ac.offset = 0;
    ASSERT(AC_OK == AC_GetLongLong(&ac, &value, AC_F_COALESCE));
    ASSERT(ac.offset == 1 && value == LLONG_MIN);
  }
  return 0;
}

TEST_MAIN({
  TESTFUNC(testCArgs);
  TESTFUNC(testTypeConversion);
  TESTFUNC(testLongLongFallbackBounds);
})