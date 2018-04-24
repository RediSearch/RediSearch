#include <stdio.h>
#include "test_util.h"
#include <util/arr.h>

typedef struct {
  int x;
  double y;
} foo;

int testStruct() {

  foo *arr = array_new(foo, 8);

  for (int i = 0; i < 10; i++) {
    arr = array_append(arr, (foo){i});
    ASSERT_EQUAL(i + 1, array_len(arr));
  }

  for (int i = 0; i < 10; i++) {
    ASSERT_EQUAL(i, arr[i].x);
  }
  array_foreach(arr, elem, printf("%d\n", elem.x));
  array_free(arr);
  RETURN_TEST_SUCCESS;
}

int testScalar() {
  int *ia = array_new(int, 8);
  for (int i = 0; i < 100; i++) {
    ia = array_append(ia, i);
    ASSERT_EQUAL(i + 1, array_len(ia));
    ASSERT_EQUAL(i, array_tail(ia));
  }

  for (int i = 0; i < array_len(ia); i++) {
    ASSERT_EQUAL(i, ia[i]);

    printf("%d %zd\n", ia[i], array_len(ia));
  }
  array_free(ia);
  RETURN_TEST_SUCCESS;
}

int testStrings() {

  char *strs[] = {"foo", "bar", "baz", NULL};
  char **a = array_new(char *, 1);
  int i = 0;
  for (i = 0; strs[i] != NULL; i++) {
    a = array_append(a, strdup(strs[i]));
    ASSERT_EQUAL(i + 1, array_len(a));
    ASSERT_STRING_EQ(strs[i], array_tail(a));
  }
  for (int j = 0; j < i; j++) {
    ASSERT_STRING_EQ(strs[j], a[j]);

    // printf("%s\n", a[j]);
  }
  array_free_ex(a, free(*(void **)ptr));
  RETURN_TEST_SUCCESS;
}

TEST_MAIN({
  TESTFUNC(testStruct);
  TESTFUNC(testStrings);
  TESTFUNC(testScalar);
})