#include "gtest/gtest.h"
#include "obfuscation/hidden.h"

#include <stdlib.h>
#include <string.h>

class HiddenTest : public ::testing::Test {};

TEST_F(HiddenTest, testHiddenOwnership) {
  const char *expected = "Text";
  size_t length = 0;
  HiddenName *view = NewHiddenName(expected, strlen(expected), false);
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  ASSERT_EQ(HiddenName_GetUnsafe(view, &length), expected);
  ASSERT_EQ(length, strlen(expected));
  ASSERT_NE(HiddenName_GetUnsafe(name, NULL), expected);
  HiddenName_TakeOwnership(view);
  ASSERT_NE(HiddenName_GetUnsafe(view, NULL), expected);
  HiddenName_Free(view, true);
  HiddenName_Free(name, true);
}

TEST_F(HiddenTest, testHiddenCompare) {
  const char *expected = "Text";
  HiddenName *first = NewHiddenName(expected, strlen(expected), true);
  HiddenName *second = NewHiddenName(expected, strlen(expected), true);
  ASSERT_EQ(HiddenName_Compare(first, second), 0);
  ASSERT_EQ(HiddenName_CompareC(first, expected, strlen(expected)), 0);
  ASSERT_NE(HiddenName_CompareC(first, expected, strlen(expected) + 1), 0);
  const char *lowerCase = "text";
  HiddenName *lower = NewHiddenName(lowerCase, strlen(lowerCase), true);
  ASSERT_EQ(HiddenName_CaseInsensitiveCompareC(first, lowerCase, strlen(lowerCase)), 0);
  ASSERT_EQ(HiddenName_CaseInsensitiveCompare(first, lower), 0);
  ASSERT_NE(HiddenName_CaseInsensitiveCompareC(first, lowerCase, strlen(lowerCase) + 1), 0);
  HiddenName_Free(first, true);
  HiddenName_Free(second, true);
  HiddenName_Free(lower, true);
}

TEST_F(HiddenTest, testHiddenDuplicate) {
  const char *expected = "Text";
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  HiddenName *clone = HiddenName_Duplicate(name);
  ASSERT_EQ(HiddenName_Compare(name, clone), 0);
  HiddenName_Free(name, true);
  HiddenName_Free(clone, true);
}

void testCloning(HiddenName *first, HiddenName *second) {
  HiddenName *clone = NULL;
  HiddenName_Clone(first, &clone);
  size_t firstLength = 0;
  HiddenName_GetUnsafe(first, &firstLength);

  size_t length = 0;
  HiddenName_GetUnsafe(clone, &length);
  ASSERT_EQ(length, firstLength);
  HiddenName_Clone(second, &clone);
  HiddenName_GetUnsafe(clone, &length);

  size_t secondLength = 0;
  HiddenName_GetUnsafe(second, &secondLength);
  ASSERT_EQ(length, secondLength);
  HiddenName_Free(clone, true);
}

TEST_F(HiddenTest, testHiddenClone) {
  const char *longText = "LongerText";
  const char *shortText = "ShortText";

  HiddenName *l = NewHiddenName(longText, strlen(longText), true);
  HiddenName *s = NewHiddenName(shortText, strlen(shortText), true);
  testCloning(l, s);
  testCloning(s, l);
  HiddenName_Free(l, true);
  HiddenName_Free(s, true);
}

TEST_F(HiddenTest, testHiddenCreateString) {
    const char *expected = "Text";
    HiddenName *name = NewHiddenName(expected, strlen(expected), true);
    RedisModuleString* string = HiddenName_CreateString(name, NULL);
    const char *text = RedisModule_StringPtrLen(string, NULL);
    ASSERT_EQ(strlen(expected), strlen(text));
    ASSERT_EQ(strncmp(text, expected, strlen(expected)), 0);
    RedisModule_FreeString(NULL, string);
    HiddenName_Free(name, true);
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

    HiddenName *name = NewHiddenName(key, strlen(key), true);
    HiddenName_DropFromKeySpace(ctx, key, name);
    ASSERT_EQ(RedisModule_Call(ctx, "GET", "ss", redisKey), noReply);
    RedisModule_FreeString(ctx, redisKey);
    HiddenName_Free(name, true);
    RedisModule_FreeThreadSafeContext(ctx);
}
