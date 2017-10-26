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
  const char *k = "key";
  RSField f = RS_NewField(k, RS_NumVal(3));

  ASSERT_STRING_EQ(f.key, k);
  ASSERT_EQUAL(3, f.val.numval);
  ASSERT_EQUAL(RSValue_Number, f.val.t);

  RETURN_TEST_SUCCESS;
}

int testArray() {

  RSValue arr = RS_VStringArray(3, strdup("foo"), strdup("bar"), strdup("baz"));
  ASSERT_EQUAL(3, arr.arrval.len);
  ASSERT_EQUAL(RSValue_String, RSValue_ArrayItem(&arr, 0)->t);
  ASSERT_STRING_EQ("foo", RSValue_ArrayItem(&arr, 0)->strval.str);

  ASSERT_EQUAL(RSValue_String, RSValue_ArrayItem(&arr, 1)->t);
  ASSERT_STRING_EQ("bar", RSValue_ArrayItem(&arr, 1)->strval.str);

  ASSERT_EQUAL(RSValue_String, RSValue_ArrayItem(&arr, 2)->t);
  ASSERT_STRING_EQ("baz", RSValue_ArrayItem(&arr, 2)->strval.str);

  RSValue_Free(&arr);

  char *strs[] = {strdup("foo"), strdup("bar"), strdup("baz")};
  arr = RS_StringArray(strs, 3);
  ASSERT_EQUAL(3, arr.arrval.len);
  ASSERT_EQUAL(RSValue_String, RSValue_ArrayItem(&arr, 0)->t);
  ASSERT_STRING_EQ("foo", RSValue_ArrayItem(&arr, 0)->strval.str);

  ASSERT_EQUAL(RSValue_String, RSValue_ArrayItem(&arr, 1)->t);
  ASSERT_STRING_EQ("bar", RSValue_ArrayItem(&arr, 1)->strval.str);

  ASSERT_EQUAL(RSValue_String, RSValue_ArrayItem(&arr, 2)->t);
  ASSERT_STRING_EQ("baz", RSValue_ArrayItem(&arr, 2)->strval.str);

  RSValue_Free(&arr);

  RETURN_TEST_SUCCESS;
}

int testFieldMap() {

  RSFieldMap *m = RS_NewFieldMap(1);
  ASSERT_EQUAL(0, m->len);
  ASSERT_EQUAL(1, m->cap);

  RSFieldMap_Add(&m, "foo", RS_NumVal(1));
  RSFieldMap_Add(&m, "bar", RS_NumVal(2));
  RSFieldMap_Add(&m, "baz", RS_NumVal(3));
  ASSERT_EQUAL(3, m->len);
  ASSERT_EQUAL(4, m->cap);

  RSValue *v = RSFieldMap_Item(m, 0);
  ASSERT_EQUAL(v->t, RSValue_Number);
  ASSERT_EQUAL(1, v->numval);

  RSValue *v2 = RSFieldMap_Get(m, "foo");
  ASSERT(v == v2);

  v = RSFieldMap_Item(m, 1);
  ASSERT_EQUAL(v->t, RSValue_Number);
  ASSERT_EQUAL(2, v->numval);
  v2 = RSFieldMap_Get(m, "bar");
  ASSERT(v == v2);

  v = RSFieldMap_Item(m, 2);
  ASSERT_EQUAL(v->t, RSValue_Number);
  ASSERT_EQUAL(3, v->numval);
  v2 = RSFieldMap_Get(m, "baz");
  ASSERT(v == v2);

  RSFieldMap_Set(&m, "foo", RS_NumVal(10));
  v = RSFieldMap_Item(m, 0);
  ASSERT_EQUAL(v->t, RSValue_Number);
  ASSERT_EQUAL(10, v->numval);

  v2 = RSFieldMap_Get(m, "foo");
  ASSERT(v == v2);
  RSFieldMap_Free(m, 0);
  RETURN_TEST_SUCCESS;
}

TEST_MAIN({
  TESTFUNC(testValue);
  TESTFUNC(testField);
  TESTFUNC(testArray);
  TESTFUNC(testFieldMap);

})