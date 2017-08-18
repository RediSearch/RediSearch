#include <stdio.h>
#include <assert.h>
#include "qint.h"
#include "rmutil/alloc.h"
#include "test_util.h"

int testBasic() {
  Buffer *b = NewBuffer(1024);
  BufferWriter w = NewBufferWriter(b);
  qint_encode4(&w, 123, 456, 789, 101112);

  uint32_t arr[4];
  BufferReader r = NewBufferReader(b);
  qint_decode(&r, arr, 4);
  assert(arr[0] == 123);
  assert(arr[1] == 456);
  assert(arr[2] == 789);
  assert(arr[3] == 101112);

  memset(arr, 0, sizeof arr);
  r = NewBufferReader(b);
  qint_decode4(&r, &arr[0], &arr[1], &arr[2], &arr[3]);
  assert(arr[0] == 123);
  assert(arr[1] == 456);
  assert(arr[2] == 789);
  assert(arr[3] == 101112);

  memset(arr, 0, sizeof arr);
  r = NewBufferReader(b);
  qint_decode3(&r, &arr[0], &arr[1], &arr[2]);
  assert(arr[0] == 123);
  assert(arr[1] == 456);
  assert(arr[2] == 789);
  Buffer_Free(b);
  free(b);
  return 0;
}

int testEncode64() {
  Buffer *b = NewBuffer(1024);
  BufferWriter w = NewBufferWriter(b);
  size_t ret = qint_encode32_64pair(&w, UINT32_MAX, UINT64_MAX);
  // 1 + 4 + 8
  ASSERT_EQUAL(1 + 4 + 8, ret);

  BufferReader r = NewBufferReader(b);
  uint32_t n32;
  uint64_t n64;
  qint_decode32_64pair(&r, &n32, &n64);
  ASSERT_EQUAL(UINT32_MAX, n32);
  ASSERT_EQUAL(UINT64_MAX, n64);

  b->offset = 0;

  w = NewBufferWriter(b);
  ret = qint_encode32_64pair(&w, 0, 0);
  ASSERT_EQUAL(3, ret);

  r = NewBufferReader(b);
  qint_decode32_64pair(&r, &n32, &n64);
  ASSERT_EQUAL(0, n32);
  ASSERT_EQUAL(0LLU, n64);

  b->offset = 0;
  w = NewBufferWriter(b);
  ret = qint_encode32_64pair(&w, 1, 1LLU);
  ASSERT_EQUAL(3, ret);

  r = NewBufferReader(b);
  qint_decode32_64pair(&r, &n32, &n64);
  ASSERT_EQUAL(1, n32);
  ASSERT_EQUAL(1LLU, n64);

  Buffer_Free(b);
  free(b);
  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testBasic);
  TESTFUNC(testEncode64);
})
