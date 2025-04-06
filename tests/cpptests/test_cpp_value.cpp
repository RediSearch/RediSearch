#include "gtest/gtest.h"

#include "value.h"

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

static std::string toString(RSValue *v) {
  RSValue *tmp = RS_NewValue(RSValue_Undef);
  RSValue_ToString(tmp, v);
  size_t n = 0;
  const char *s = RSValue_StringPtrLen(tmp, &n);
  std::string ret(s, n);
  RSValue_Decref(tmp);
  return ret;
}

TEST_F(ValueTest, testNumericFormat) {
  RSValue *v = RS_NumVal(0.01);
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
  RSValue_Decref(v);
}

TEST_F(ValueTest, testConvertStringPtrLen) {
  // Test string conversion
  const char *str = "hello world";
  RSValue *strVal = RS_StringValC(strdup(str));
  size_t len = 0;
  char buf[100];
  
  const char *result = RSValue_ConvertStringPtrLen(strVal, &len, buf, sizeof(buf));
  ASSERT_STREQ(str, result);
  ASSERT_EQ(strlen(str), len);
  
  // Test numeric conversion with sufficient buffer
  RSValue *numVal = RS_NumVal(123.456);
  result = RSValue_ConvertStringPtrLen(numVal, &len, buf, sizeof(buf));
  ASSERT_STREQ("123.456000", result);  // Default float format with 6 decimal places
  ASSERT_EQ(10, len);  // Length of "123.456000"
  
  // Test numeric conversion with insufficient buffer
  char smallBuf[5];  // Too small for "123.456000"
  result = RSValue_ConvertStringPtrLen(numVal, &len, smallBuf, sizeof(smallBuf));
  ASSERT_STREQ("", result);  // Should return empty string when buffer is too small
  ASSERT_EQ(0, len);
  
  // Test array conversion (non-string, non-numeric)
  RSValue *arr = RS_VStringArray(2, strdup("foo"), strdup("bar"));
  result = RSValue_ConvertStringPtrLen(arr, &len, buf, sizeof(buf));
  ASSERT_STREQ("", result);  // Should return empty string for arrays
  ASSERT_EQ(0, len);
  
  // Test null conversion
  RSValue *null = RS_NullVal();
  result = RSValue_ConvertStringPtrLen(null, &len, buf, sizeof(buf));
  ASSERT_STREQ("", result);  // Should return empty string for null
  ASSERT_EQ(0, len);
  
  // Test with null parameters
  
  // Test with NULL lenp parameter
  result = RSValue_ConvertStringPtrLen(strVal, NULL, buf, sizeof(buf));
  ASSERT_STREQ(str, result);  // Should still return the string value
  
  // Test with NULL buffer parameter for numeric values
  ASSERT_ANY_THROW(RSValue_ConvertStringPtrLen(numVal, &len, NULL, sizeof(buf)));
  
  // Test with NULL value parameter
  ASSERT_ANY_THROW(RSValue_ConvertStringPtrLen(NULL, &len, buf, sizeof(buf)));
  
  // Test with buffer size larger then RSVALUE_MAX_BUFFER_LEN
  size_t largeBufSize = RSVALUE_MAX_BUFFER_LEN + 1;
  ASSERT_ANY_THROW(RSValue_ConvertStringPtrLen(strVal, &len, buf, largeBufSize));

  // Cleanup
  RSValue_Decref(strVal);
  RSValue_Decref(numVal);
  RSValue_Decref(arr);

}
