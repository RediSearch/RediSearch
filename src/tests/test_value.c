#include "test_util.h"
#include <value.h>

int testValue() {
  RSValue v = RS_NumVal(3);
  ASSERT_EQUAL(3, v.numval);
  ASSERT_EQUAL(RSValue_Number, v.t);

  v = RS_NullVal();
  ASSERT_EQUAL(RSValue_Null, v.t);

  const char *str = "hello world";
  v = RS_CStringVal(strdup(str));
  ASSERT_EQUAL(RSValue_String, v.t);
  ASSERT_EQUAL(strlen(str), v.strval.len);
  ASSERT(!strcmp(str, v.strval.str));
  RSValue_Free(&v);

  // cannot use redis strings in tests...
  v = RS_RedisStringVal(NULL);
  ASSERT_EQUAL(RSValue_RedisString, v.t);
  RETURN_TEST_SUCCESS;
}

int testField() {

  RETURN_TEST_SUCCESS;
}

int testArray() {

  RETURN_TEST_SUCCESS;
}

int testFieldMap() {
  RETURN_TEST_SUCCESS;
}

TEST_MAIN({ TESTFUNC(testValue); })