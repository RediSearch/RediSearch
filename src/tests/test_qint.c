#include <stdio.h>
#include <assert.h>
#include "qint.h"
#include "rmutil/alloc.h"

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  Buffer b = {0};
  Buffer_Init(&b, 1024);
  BufferWriter w = NewBufferWriter(&b);
  qint_encode4(&w, 123, 456, 789, 101112);

  uint32_t arr[4];
  BufferReader r = NewBufferReader(&b);
  qint_decode(&r, arr, 4);
  assert(arr[0] == 123);
  assert(arr[1] == 456);
  assert(arr[2] == 789);
  assert(arr[3] == 101112);

  memset(arr, 0, sizeof arr);
  r = NewBufferReader(&b);
  qint_decode4(&r, &arr[0], &arr[1], &arr[2], &arr[3]);
  assert(arr[0] == 123);
  assert(arr[1] == 456);
  assert(arr[2] == 789);
  assert(arr[3] == 101112);

  memset(arr, 0, sizeof arr);
  r = NewBufferReader(&b);
  qint_decode3(&r, &arr[0], &arr[1], &arr[2]);
  assert(arr[0] == 123);
  assert(arr[1] == 456);
  assert(arr[2] == 789);
  Buffer_Free(&b);
  return 0;
}