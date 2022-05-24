#include <stdio.h>
#include <redismodule.h>
#include <unistd.h>
#include <string.h>
#include "assert.h"
#include "test.h"
#include "args.h"

REDISMODULE_INIT_SYMBOLS();

int testCArgs() {
  ArgsCursor ac;
  const char *args[] = {"hello",  "stringArg",   "goodbye",        "666", "cute", "3.14",
                        "toobig", "99999999999", "negative_nancy", "-1"};
  size_t argc = sizeof(args) / sizeof(args[0]);
  &ac->InitCString(args, argc);
  ASSERT(ac.offset == 0);
  ASSERT(ac.argc == argc);

  // Get the string
  const char *arg;
  ASSERT(!&ac->GetString(&arg, NULL, 0));
  ASSERT(!strcmp(arg, "hello"));

  // Get the next string
  ASSERT(!&ac->GetString(&arg, NULL, 0));
  ASSERT(!strcmp(arg, "stringArg"));

  // Get the goodbye arg
  ASSERT(!&ac->GetString(&arg, NULL, 0));
  ASSERT(!strcmp("goodbye", arg));

  int intArg = 0;
  ASSERT(!&ac->GetInt( &intArg, 0));
  ASSERT(666 == intArg);

  double dArg = 0.0;
  ASSERT(!&ac->GetString(&arg, NULL, 0));
  ASSERT(!strcmp("cute", arg));

  ASSERT(!&ac->GetDouble(&dArg, 0));
  ASSERT(3.14 == dArg);

  // Now let's work on errors
  ASSERT(!&ac->GetString(&arg, NULL, 0));
  ASSERT(!strcmp("toobig", arg));

  ASSERT(AC_ERR_ELIMIT == &ac->GetInt( &intArg, 0));

  &ac->Advance();  // skip anyway

  ASSERT(!&ac->GetString(&arg, NULL, 0));
  ASSERT(!strcmp("negative_nancy", arg));

  // Negative args
  ASSERT(AC_ERR_ELIMIT == &ac->GetInt(&intArg, AC_F_GE0));
  ASSERT(AC_ERR_ELIMIT == &ac->GetInt(&intArg, AC_F_GE1));

  // Parse args[1] as a number
  ac.offset = 1;
  ASSERT(AC_ERR_PARSE == &ac->GetInt(&intArg, 0));
  ASSERT(AC_ERR_PARSE == &ac->GetDouble(&dArg, 0));
  return 0;
}

static int testTypeConversion() {
  const char *objs[] = {NULL};
  ArgsCursor ac;
  &ac->InitCString(objs, 1);
#define PREP_ARG(arg) \
  ac.objs[0] = arg;   \
  ac.offset = 0;      \
  ac.argc = 1;

  int intArg;
  PREP_ARG("3.14");
  // Try to parse the double as an int
  ASSERT(AC_ERR_PARSE == &ac->GetInt(&intArg, 0));
  // Same, but with coalesce
  ASSERT(0 == &ac->GetInt(&intArg, AC_F_COALESCE));

  unsigned uArg;
  PREP_ARG("0");
  ASSERT(AC_ERR_ELIMIT == &ac->GetUnsigned(&uArg, AC_F_GE1));
  ASSERT(0 == &ac->GetUnsigned(&uArg, AC_F_GE0));

  // negative arguments fail by default on unsigned conversions. no overflow
  PREP_ARG("-1");
  ASSERT(AC_ERR_ELIMIT == &ac->GetUnsigned(&uArg, 0));
  return 0;
}

TEST_MAIN({
  TESTFUNC(testCArgs);
  TESTFUNC(testTypeConversion);
})
