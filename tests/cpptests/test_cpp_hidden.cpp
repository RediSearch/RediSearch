#include "gtest/gtest.h"
#include "obfuscation/hidden.h"

#include <stdlib.h>
#include <string.h>

class HiddenTest : public ::testing::Test {};

TEST_F(HiddenTest, testHiddenOwnership) {
  const char *expected = "Text";
  size_t length = 0;
  HiddenString *view = NewHiddenString(expected, strlen(expected), false);
  HiddenString *name = NewHiddenString(expected, strlen(expected), true);
  ASSERT_EQ(HiddenString_GetUnsafe(view, &length), expected);
  ASSERT_EQ(length, strlen(expected));
  ASSERT_NE(HiddenString_GetUnsafe(name, NULL), expected);
  HiddenString_TakeOwnership(view);
  ASSERT_NE(HiddenString_GetUnsafe(view, NULL), expected);
  HiddenString_Free(view, true);
  HiddenString_Free(name, true);
}

TEST_F(HiddenTest, testHiddenCompare) {
  const char *expected = "Text";
  HiddenString *first = NewHiddenString(expected, strlen(expected), true);
  HiddenString *second = NewHiddenString(expected, strlen(expected), true);
  ASSERT_EQ(HiddenString_Compare(first, second), 0);
  ASSERT_EQ(HiddenString_CompareC(first, expected, strlen(expected)), 0);
  ASSERT_NE(HiddenString_CompareC(first, expected, strlen(expected) + 1), 0);
  const char *lowerCase = "text";
  HiddenString *lower = NewHiddenString(lowerCase, strlen(lowerCase), true);
  ASSERT_EQ(HiddenString_CaseInsensitiveCompareC(first, lowerCase, strlen(lowerCase)), 0);
  ASSERT_EQ(HiddenString_CaseInsensitiveCompare(first, lower), 0);
  ASSERT_NE(HiddenString_CaseInsensitiveCompareC(first, lowerCase, strlen(lowerCase) + 1), 0);
  HiddenString_Free(first, true);
  HiddenString_Free(second, true);
  HiddenString_Free(lower, true);
}

TEST_F(HiddenTest, testHiddenDuplicate) {
  const char *expected = "Text";
  HiddenString *name = NewHiddenString(expected, strlen(expected), true);
  HiddenString *clone = HiddenString_Duplicate(name);
  ASSERT_EQ(HiddenString_Compare(name, clone), 0);
  HiddenString_Free(name, true);
  HiddenString_Free(clone, true);
}

void testCloning(HiddenString *first, HiddenString *second) {
  HiddenString *clone = NULL;
  HiddenString_Clone(first, &clone);
  size_t firstLength = 0;
  HiddenString_GetUnsafe(first, &firstLength);

  size_t length = 0;
  HiddenString_GetUnsafe(clone, &length);
  ASSERT_EQ(length, firstLength);
  HiddenString_Clone(second, &clone);
  HiddenString_GetUnsafe(clone, &length);

  size_t secondLength = 0;
  HiddenString_GetUnsafe(second, &secondLength);
  ASSERT_EQ(length, secondLength);
  HiddenString_Free(clone, true);
}

TEST_F(HiddenTest, testHiddenClone) {
  const char *longText = "LongerText";
  const char *shortText = "ShortText";

  HiddenString *l = NewHiddenString(longText, strlen(longText), true);
  HiddenString *s = NewHiddenString(shortText, strlen(shortText), true);
  testCloning(l, s);
  testCloning(s, l);
  HiddenString_Free(l, true);
  HiddenString_Free(s, true);
}

TEST_F(HiddenTest, testHiddenCreateString) {
    const char *expected = "Text";
    HiddenString *name = NewHiddenString(expected, strlen(expected), true);
    RedisModuleString* string = HiddenString_CreateRedisModuleString(name, NULL);
    const char *text = RedisModule_StringPtrLen(string, NULL);
    ASSERT_EQ(strlen(expected), strlen(text));
    ASSERT_EQ(strncmp(text, expected, strlen(expected)), 0);
    RedisModule_FreeString(NULL, string);
    HiddenString_Free(name, true);
}

TEST_F(HiddenTest, testHiddenDropFromKeySpace) {
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    const char *key = "Hello";
    const char *value = "World";
    RedisModuleString* redisKey = RedisModule_CreateString(ctx, key, strlen(key));
    RedisModuleString* redisValue = RedisModule_CreateString(ctx, value, strlen(value));

    RedisModuleCallReply* noReply = NULL;
    ASSERT_EQ(RedisModule_Call(ctx, "SET", "ss", redisKey, redisValue), noReply);
    RedisModule_FreeString(ctx, redisValue);

    RedisModuleCallReply* reply = RedisModule_Call(ctx, "GET", "ss", redisKey);
    ASSERT_EQ(RedisModule_CallReplyType(reply), REDISMODULE_REPLY_STRING);
    RedisModule_FreeCallReply(reply);

    HiddenString *name = NewHiddenString(key, strlen(key), true);
    HiddenString_DropFromKeySpace(ctx, key, name);
    ASSERT_EQ(RedisModule_Call(ctx, "GET", "ss", redisKey), noReply);
    RedisModule_FreeString(ctx, redisKey);
    HiddenString_Free(name, true);
    RedisModule_FreeThreadSafeContext(ctx);
}
