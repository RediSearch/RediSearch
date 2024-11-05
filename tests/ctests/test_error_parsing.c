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
