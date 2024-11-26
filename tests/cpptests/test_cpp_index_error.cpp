#include "gtest/gtest.h"

#include "src/info/index_error.h"

class IndexErrorTest : public ::testing::Test {};

TEST_F(IndexErrorTest, testBasic) {
  IndexError error;
  error = IndexError_Init();
  const char* expected = "secret";
  RedisModuleString *key = RedisModule_CreateString(NULL, expected, 6);
  IndexError_AddError(&error, "error1", key);
  ASSERT_STREQ(error.last_error, "error1");
  RedisModuleString *lastErrorKey = IndexError_LastErrorKey(&error, false);
  ASSERT_EQ(key, lastErrorKey);
  const char* text = RedisModule_StringPtrLen(lastErrorKey, NULL);
  ASSERT_STREQ(text, expected);
  RedisModule_FreeString(NULL, lastErrorKey);

  error.last_error_time = {0};
  lastErrorKey = IndexError_LastErrorKey(&error, true);
  text = RedisModule_StringPtrLen(lastErrorKey, NULL);
  ASSERT_NE(key, lastErrorKey);
  ASSERT_STREQ("Key@0", text);
  RedisModule_FreeString(NULL, lastErrorKey);
  IndexError_Clear(error);
  RedisModule_FreeString(NULL, key);

}