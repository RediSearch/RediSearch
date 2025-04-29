/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "test_util.h"
#include "src/util/misc.h"
#include <string.h>

int testErrorCodeLengthExtraction() {
  ASSERT_EQUAL(GetRedisErrorCodeLength("ERR Error message"), 3);
  ASSERT_EQUAL(GetRedisErrorCodeLength("ERR"), 0);
  ASSERT_EQUAL(GetRedisErrorCodeLength("ERR "), 3);
  ASSERT_EQUAL(GetRedisErrorCodeLength(""), 0);
  ASSERT_EQUAL(GetRedisErrorCodeLength(" "), 0);
  return 0;
}

int testErrorCodeFormat(const char* error, const char* expected) {
  char buf[1024];
  sprintf(buf, "%.*s", GetRedisErrorCodeLength(error), error);
  ASSERT_STRING_EQ(buf, expected);
  return 0;
}

int testErrorCodeFormatting() {
  testErrorCodeFormat("ERR Error message", "ERR");
  testErrorCodeFormat("ERR-Error-message", "");
  testErrorCodeFormat("ERR", "");
  testErrorCodeFormat(" ERR", "");
  testErrorCodeFormat("", "");
  return 0;
}

TEST_MAIN({
  TESTFUNC(testErrorCodeLengthExtraction);
  TESTFUNC(testErrorCodeFormatting);
})
