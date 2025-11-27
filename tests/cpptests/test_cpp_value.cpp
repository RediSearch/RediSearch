/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

#include "value.h"

class ValueTest : public ::testing::Test {};

TEST_F(ValueTest, testBasic) {
  RSValue *v = RSValue_NewNumber(3);
  ASSERT_EQ(3, RSValue_Number_Get(v));
  ASSERT_EQ(RSValueType_Number, RSValue_Type(v));
  ASSERT_EQ(1, RSValue_Refcount(v));
  RSValue_DecrRef(v);

  v = RSValue_NullStatic();
  ASSERT_EQ(RSValueType_Null, RSValue_Type(v));
  RSValue *v2 = RSValue_NullStatic();
  ASSERT_EQ(v, v2);  // Pointer is always the same
  RSValue_DecrRef(v2);

  const char *str = "hello world";
  char *s = strdup(str);
  v = RSValue_NewString(s, strlen(s));
  ASSERT_EQ(RSValueType_String, RSValue_Type(v));
  uint32_t v_str_len;
  char *v_str = RSValue_String_Get(v, &v_str_len);
  ASSERT_EQ(strlen(str), v_str_len);
  ASSERT_EQ(0, strcmp(str, v_str));
  RSValue_DecrRef(v);

  // cannot use redis strings in tests...
  v = RSValue_NewBorrowedRedisString(NULL);
  ASSERT_EQ(RSValueType_RedisString, RSValue_Type(v));
  RSValue_DecrRef(v);
}

TEST_F(ValueTest, testArray) {
  RSValue *arr = RSValue_NewVStringArray(3, strdup("foo"), strdup("bar"), strdup("baz"));
  ASSERT_EQ(3, RSValue_ArrayLen(arr));
  ASSERT_EQ(RSValueType_String, RSValue_Type(RSValue_ArrayItem(arr, 0)));
  ASSERT_STREQ("foo", RSValue_String_Get(RSValue_ArrayItem(arr, 0), NULL));

  ASSERT_EQ(RSValueType_String, RSValue_Type(RSValue_ArrayItem(arr, 1)));
  ASSERT_STREQ("bar", RSValue_String_Get(RSValue_ArrayItem(arr, 1), NULL));

  ASSERT_EQ(RSValueType_String, RSValue_Type(RSValue_ArrayItem(arr, 2)));
  ASSERT_STREQ("baz", RSValue_String_Get(RSValue_ArrayItem(arr, 2), NULL));
  RSValue_DecrRef(arr);

  char *strs[] = {strdup("foo"), strdup("bar"), strdup("baz")};
  arr = RSValue_NewStringArray(strs, 3);
  ASSERT_EQ(3, RSValue_ArrayLen(arr));
  ASSERT_EQ(RSValueType_String, RSValue_Type(RSValue_ArrayItem(arr, 0)));
  ASSERT_STREQ("foo", RSValue_String_Get(RSValue_ArrayItem(arr, 0), NULL));

  ASSERT_EQ(RSValueType_String, RSValue_Type(RSValue_ArrayItem(arr, 1)));
  ASSERT_STREQ("bar", RSValue_String_Get(RSValue_ArrayItem(arr, 1), NULL));

  ASSERT_EQ(RSValueType_String, RSValue_Type(RSValue_ArrayItem(arr, 2)));
  ASSERT_STREQ("baz", RSValue_String_Get(RSValue_ArrayItem(arr, 2), NULL));

  RSValue_DecrRef(arr);
}

static std::string toString(RSValue *v) {
  RSValue *tmp = RSValue_NewUndefined();
  RSValue_ToString(tmp, v);
  size_t n = 0;
  const char *s = RSValue_StringPtrLen(tmp, &n);
  std::string ret(s, n);
  RSValue_DecrRef(tmp);
  return ret;
}

TEST_F(ValueTest, testNumericFormat) {
  RSValue *v = RSValue_NewNumber(0.01);
  ASSERT_STREQ("0.01", toString(v).c_str());
  RSValue_SetNumber(v, 0.001);

  ASSERT_STREQ("0.001", toString(v).c_str());
  RSValue_SetNumber(v, 0.00123);

  ASSERT_STREQ("0.00123", toString(v).c_str());

  RSValue_SetNumber(v, 0.0012345);
  ASSERT_STREQ("0.0012345", toString(v).c_str());

  RSValue_SetNumber(v, 0.0000001);
  ASSERT_STREQ("1e-07", toString(v).c_str());

  RSValue_SetNumber(v, 1581011976800);
  ASSERT_STREQ("1581011976800", toString(v).c_str());
  RSValue_DecrRef(v);
}
