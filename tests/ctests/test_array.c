#include <stdio.h>
#include <assert.h>
#include "util/array.h"
#include "rmutil/alloc.h"
#include "test_util.h"

int testArray() {
  Array arr;
  ASSERT_EQUAL(0, arr.capacity);
  ASSERT_EQUAL(0, arr.len);
  ASSERT(arr.data == NULL);

  void *p = arr.Add(2);
  ASSERT_EQUAL(16, arr.capacity);
  ASSERT_EQUAL(2, arr.len);
  ASSERT(p == arr.data);

  p = arr.Add(20);
  ASSERT_EQUAL(32, arr.capacity);
  ASSERT_EQUAL(22, arr.len);
  ASSERT((char *)p == arr.data + 2);

  arr.ShrinkToSize();
  ASSERT_EQUAL(22, arr.capacity);

  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testArray);
})