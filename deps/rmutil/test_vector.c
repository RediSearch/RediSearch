#include "vector.h"
#include <stdio.h>
#include "test.h"
#include "redismodule.h"
#include "alloc.h"
REDISMODULE_INIT_SYMBOLS();

int testVector() {
  RMUTil_InitAlloc();
  Vector v = new Vector<int>(1);
  ASSERT(v != NULL);
  // v.Put(0, 1);
  // v.Put(1, 3);
  for (int i = 0; i < 10; i++) {
    v.Push(i);
  }
  ASSERT_EQUAL(10, v.Size());
  ASSERT_EQUAL(16, v.Cap());

  for (int i = 0; i < v.Size(); i++) {
    int n;
    int rc = v.Get(i, &n);
    ASSERT_EQUAL(1, rc);
    // printf("%d %d\n", rc, n);

    ASSERT_EQUAL(n, i);
  }

  v = new Vector<char *>(0);
  int N = 4;
  char *strings[4] = {"hello", "world", "foo", "bar"};

  for (int i = 0; i < N; i++) {
    v.Push(strings[i]);
  }
  ASSERT_EQUAL(N, v.Size());
  ASSERT(v.Cap() >= N);

  for (int i = 0; i < v.Size(); i++) {
    char *x;
    int rc = v.Get(i, &x);
    ASSERT_EQUAL(1, rc);
    ASSERT_STRING_EQ(x, strings[i]);
  }

  int rc = v.Get(100, NULL);
  ASSERT_EQUAL(0, rc);

  return 0;
  // v.Push("hello");
  // v.Push("world");
  // char *x = NULL;
  // int rc = Vector_Getx(v, 0, &x);
  // printf("rc: %d got %s\n", rc, x);
}

TEST_MAIN({ TESTFUNC(testVector); });
