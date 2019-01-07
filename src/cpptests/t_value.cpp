#include <gtest/gtest.h>
#include <value.h>

class ValueTest : public ::testing::Test {};

TEST_F(ValueTest, testBasic) {
  RSValue *v = RS_NumVal(3);
  ASSERT_EQ(3, v->numval);
  ASSERT_EQ(RSValue_Number, v->t);
  ASSERT_EQ(1, v->refcount);
  RSValue_Decref(v);

  v = RS_NullVal();
  ASSERT_EQ(RSValue_Null, v->t);
  RSValue *v2 = RS_NullVal();
  ASSERT_EQ(v, v2);  // Pointer is always the same
  RSValue_Decref(v2);

  const char *str = "hello world";
  v = RS_StringValC(strdup(str));
  ASSERT_EQ(RSValue_String, v->t);
  ASSERT_EQ(strlen(str), v->strval.len);
  ASSERT_EQ(0, strcmp(str, v->strval.str));
  RSValue_Decref(v);

  // cannot use redis strings in tests...
  v = RS_RedisStringVal(NULL);
  ASSERT_EQ(RSValue_RedisString, v->t);
  RSValue_Decref(v);
}

TEST_F(ValueTest, testArray) {
  RSValue *arr = RS_VStringArray(3, strdup("foo"), strdup("bar"), strdup("baz"));
  ASSERT_EQ(3, arr->arrval.len);
  ASSERT_EQ(RSValue_String, RSValue_ArrayItem(arr, 0)->t);
  ASSERT_STREQ("foo", RSValue_ArrayItem(arr, 0)->strval.str);

  ASSERT_EQ(RSValue_String, RSValue_ArrayItem(arr, 1)->t);
  ASSERT_STREQ("bar", RSValue_ArrayItem(arr, 1)->strval.str);

  ASSERT_EQ(RSValue_String, RSValue_ArrayItem(arr, 2)->t);
  ASSERT_STREQ("baz", RSValue_ArrayItem(arr, 2)->strval.str);
  RSValue_Decref(arr);

  char *strs[] = {strdup("foo"), strdup("bar"), strdup("baz")};
  arr = RS_StringArray(strs, 3);
  ASSERT_EQ(3, arr->arrval.len);
  ASSERT_EQ(RSValue_String, RSValue_ArrayItem(arr, 0)->t);
  ASSERT_STREQ("foo", RSValue_ArrayItem(arr, 0)->strval.str);

  ASSERT_EQ(RSValue_String, RSValue_ArrayItem(arr, 1)->t);
  ASSERT_STREQ("bar", RSValue_ArrayItem(arr, 1)->strval.str);

  ASSERT_EQ(RSValue_String, RSValue_ArrayItem(arr, 2)->t);
  ASSERT_STREQ("baz", RSValue_ArrayItem(arr, 2)->strval.str);

  RSValue_Decref(arr);
}