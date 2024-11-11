extern "C" {
#include "hiredis/sds.h"
}

#include "gtest/gtest.h"
#include "obfuscation/hidden.h"
#include "obfuscation/hidden_unicode.h"

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
  HiddenString_Free(view);
  HiddenString_Free(name);
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
  HiddenString_Free(first);
  HiddenString_Free(second);
  HiddenString_Free(lower);
}

TEST_F(HiddenTest, testHiddenUnicodeCompare) {
  sds expected = sdsnew("¥£€$®a");
  HiddenUnicodeString *first = NewHiddenUnicodeString(expected);
  const char *internalExpected = HiddenUnicodeString_GetUnsafe(first, NULL);
  sds unicode = sdsnew("¥£€$®A");
  HiddenUnicodeString *second = NewHiddenUnicodeString(unicode);
  const char *internalUnicode = HiddenUnicodeString_GetUnsafe(second, NULL);
  ASSERT_NE(expected, internalExpected);
  ASSERT_NE(unicode, internalUnicode);

  // Compare Hidden with Hidden
  ASSERT_NE(HiddenUnicodeString_Compare(first, second), 0);
  // Compare Hidden with sds
  ASSERT_EQ(HiddenUnicodeString_CompareC(first, expected), 0);
  ASSERT_NE(HiddenUnicodeString_CompareC(first, unicode), 0);

  HiddenUnicodeString_Free(first);
  HiddenUnicodeString_Free(second);
  sdsfree(expected);
  sdsfree(unicode);
}

TEST_F(HiddenTest, testHiddenDuplicate) {
  const char *expected = "Text";
  HiddenString *name = NewHiddenString(expected, strlen(expected), true);
  HiddenString *clone = HiddenString_Retain(name);
  ASSERT_EQ(HiddenString_Compare(name, clone), 0);
  HiddenString_Free(name);
  HiddenString_Free(clone);
}

void testRetention(HiddenString *first, HiddenString *second) {
  HiddenString *clone = HiddenString_Retain(first);
  size_t firstLength = 0;
  HiddenString_GetUnsafe(first, &firstLength);

  size_t length = 0;
  HiddenString_GetUnsafe(clone, &length);
  ASSERT_EQ(length, firstLength);
  HiddenString_Free(clone);
  clone = HiddenString_Retain(second);
  HiddenString_GetUnsafe(clone, &length);

  size_t secondLength = 0;
  HiddenString_GetUnsafe(second, &secondLength);
  ASSERT_EQ(length, secondLength);
  HiddenString_Free(clone);
}

TEST_F(HiddenTest, testHiddenClone) {
  const char *longText = "LongerText";
  const char *shortText = "ShortText";

  HiddenString *l = NewHiddenString(longText, strlen(longText), true);
  HiddenString *s = NewHiddenString(shortText, strlen(shortText), true);
  testRetention(l, s);
  testRetention(s, l);
  HiddenString_Free(l);
  HiddenString_Free(s);
}

TEST_F(HiddenTest, testHiddenCreateString) {
    const char *expected = "Text";
    HiddenString *name = NewHiddenString(expected, strlen(expected), true);
    RedisModuleString* string = HiddenString_CreateRedisModuleString(name, NULL);
    const char *text = RedisModule_StringPtrLen(string, NULL);
    ASSERT_EQ(strlen(expected), strlen(text));
    ASSERT_EQ(strncmp(text, expected, strlen(expected)), 0);
    RedisModule_FreeString(NULL, string);
    HiddenString_Free(name);
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
    HiddenString_Free(name);
    RedisModule_FreeThreadSafeContext(ctx);
}
