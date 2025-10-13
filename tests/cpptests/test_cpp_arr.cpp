/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "gtest/gtest.h"
#include "util/arr.h"

class ArrTest : public ::testing::Test {};

typedef struct Foo {
  size_t x;
  double y;
} Foo;

TEST_F(ArrTest, testStruct) {
  Foo *arr = (Foo *)array_new(Foo, 8);

  for (size_t i = 0; i < 10; i++) {
    array_append(arr, ((Foo){i, i * 2.0}));
    ASSERT_EQ(i + 1, array_len(arr));
  }

  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(i, arr[i].x);
    ASSERT_EQ(i * 2.0, arr[i].y);
  }
  // array_foreach(arr, elem, printf("%d\n", elem.x));
  array_free(arr);
}

TEST_F(ArrTest, testScalar) {
  int *ia = array_new(int, 8);
  for (size_t i = 0; i < 100; i++) {
    array_append(ia, i);
    ASSERT_EQ(i + 1, array_len(ia));
    ASSERT_EQ(i, array_tail(ia));
  }

  for (size_t i = 0; i < array_len(ia); i++) {
    ASSERT_EQ(i, ia[i]);

    // printf("%d %zd\n", ia[i], array_len(ia));
  }
  array_free(ia);
}

TEST_F(ArrTest, testStrings) {
  const char *strs[] = {"foo", "bar", "baz", NULL};
  char **a = array_new(char *, 1);
  size_t i = 0;
  for (i = 0; strs[i] != NULL; i++) {
    array_append(a, strdup(strs[i]));
    ASSERT_EQ(i + 1, array_len(a));
    ASSERT_STREQ(strs[i], array_tail(a));
  }
  for (size_t j = 0; j < i; j++) {
    ASSERT_STREQ(strs[j], a[j]);

    // printf("%s\n", a[j]);
  }
  array_free_ex(a, free(*(void **)ptr));
}

TEST_F(ArrTest, testTrimm) {
  const char *strs[] = {"foo", "bar", "baz", "far", "faz", "boo", NULL};
  const char **a = array_new(const char *, 16);
  size_t i = 0;
  for (i = 0; strs[i] != NULL; i++) {
    array_append(a, strs[i]);
    ASSERT_EQ(i + 1, array_len(a));
    ASSERT_STREQ(strs[i], array_tail(a));
  }
  array_trimm_len(a, 1);
  ASSERT_EQ(array_len(a), 5);
  array_trimm_len(a, 3);
  ASSERT_EQ(array_len(a), 2);
  array_free(a);
}

TEST_F(ArrTest, testEnsure) {
  Foo *f = array_new(Foo, 1);
  array_hdr_t *hdr = array_hdr(f);
  Foo *tail = array_ensure_tail(&f, Foo);
  // Make sure Valgrind does not complain!
  tail->x = 0;
  tail->y = 0;

  Foo *middle = array_ensure_at(&f, 5, Foo);
  ASSERT_EQ(0, middle->x);
  ASSERT_EQ(0, middle->y);

  for (size_t ii = 0; ii < array_len(f); ++ii) {
    ASSERT_EQ(0, f[ii].x);
    ASSERT_EQ(0, f[ii].y);
  }

  // Try again with ensure_tail
  tail = array_ensure_tail(&f, Foo);
  f->x = 99;
  f->y = 990;
  tail->x = 100;
  tail->y = 200;

  // ensure_append
  Foo threeFoos[] = {{10, 11}, {20, 21}, {30, 31}};
  size_t prevlen = array_len(f);
  f = array_ensure_append(f, &threeFoos, 3, Foo);
  ASSERT_EQ(10, f[prevlen].x);
  ASSERT_EQ(20, f[prevlen + 1].x);
  ASSERT_EQ(30, f[prevlen + 2].x);
  array_free(f);
}

TEST_F(ArrTest, testDelete) {
  int *a = array_new(int, 1);
  array_append(a, 42);
  a = array_del(a, 0);
  ASSERT_EQ(0, array_len(a));

  // repopulate
  for (size_t ii = 0; ii < 10; ++ii) {
    array_append(a, ii);
  }
  ASSERT_EQ(10, array_len(a));
  // Remove last element
  for (ssize_t ii = 9; ii >= 0; --ii) {
    ASSERT_LT(ii, array_len(a)) << ii;
    a = array_del(a, ii);
  }
  ASSERT_EQ(0, array_len(a));
  array_free(a);

  int tmp = 1;
  a = NULL;
  a = array_ensure_append(a, &tmp, 1, int);
  ASSERT_EQ(1, array_len(a));
  tmp = 2;
  a = array_ensure_append(a, &tmp, 1, int);
  ASSERT_EQ(2, array_len(a));
  ASSERT_EQ(1, a[0]);
  ASSERT_EQ(2, a[1]);

  a = array_del(a, 0);
  ASSERT_EQ(1, array_len(a));
  ASSERT_EQ(2, a[1]);
  array_free(a);
}

// Test the new array_clear_func function
TEST_F(ArrTest, testArrayClearFunc) {
  // Test with NULL array
  int *arr = NULL;
  arr = (int *)array_clear_func(arr, sizeof(int));
  ASSERT_NE(nullptr, arr);
  ASSERT_EQ(0, array_len(arr));
  array_free(arr);

  // Test with existing array
  arr = array_new(int, 5);
  for (int i = 0; i < 10; i++) {
    array_append(arr, i);
  }
  ASSERT_EQ(10, array_len(arr));

  arr = (int *)array_clear_func(arr, sizeof(int));
  ASSERT_EQ(0, array_len(arr));

  // Verify we can still append after clearing
  array_append(arr, 42);
  ASSERT_EQ(1, array_len(arr));
  ASSERT_EQ(42, arr[0]);
  array_free(arr);

  // Test with struct array
  Foo *foo_arr = array_new(Foo, 3);
  for (size_t i = 0; i < 5; i++) {
    array_append(foo_arr, ((Foo){i, i * 2.0}));
  }
  ASSERT_EQ(5, array_len(foo_arr));

  foo_arr = (Foo *)array_clear_func(foo_arr, sizeof(Foo));
  ASSERT_EQ(0, array_len(foo_arr));

  // Verify we can still append after clearing
  array_append(foo_arr, ((Foo){99, 99.5}));
  ASSERT_EQ(1, array_len(foo_arr));
  ASSERT_EQ(99, foo_arr[0].x);
  ASSERT_EQ(99.5, foo_arr[0].y);
  array_free(foo_arr);
}

// Test the new array_ensure_append_n_func function
TEST_F(ArrTest, testArrayEnsureAppendNFunc) {
  // Test with NULL destination array
  int *dest = NULL;
  int src[] = {1, 2, 3, 4, 5};

  dest = (int *)array_ensure_append_n_func(dest, src, 5, sizeof(int));
  ASSERT_NE(nullptr, dest);
  ASSERT_EQ(5, array_len(dest));
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(src[i], dest[i]);
  }
  array_free(dest);

  // Test with existing destination array
  dest = array_new(int, 3);
  array_append(dest, 10);
  array_append(dest, 20);
  ASSERT_EQ(2, array_len(dest));

  dest = (int *)array_ensure_append_n_func(dest, src, 3, sizeof(int));
  ASSERT_EQ(5, array_len(dest));
  ASSERT_EQ(10, dest[0]);
  ASSERT_EQ(20, dest[1]);
  ASSERT_EQ(1, dest[2]);
  ASSERT_EQ(2, dest[3]);
  ASSERT_EQ(3, dest[4]);
  array_free(dest);

  // Test with NULL source (should just grow the array)
  dest = array_new(int, 2);
  array_append(dest, 100);
  ASSERT_EQ(1, array_len(dest));

  dest = (int *)array_ensure_append_n_func(dest, NULL, 3, sizeof(int));
  ASSERT_EQ(4, array_len(dest));
  ASSERT_EQ(100, dest[0]);
  // Note: dest[1], dest[2], dest[3] contain uninitialized data when src is NULL
  array_free(dest);

  // Test with struct array
  Foo *foo_dest = NULL;
  Foo foo_src[] = {{1, 1.1}, {2, 2.2}, {3, 3.3}};

  foo_dest = (Foo *)array_ensure_append_n_func(foo_dest, foo_src, 3, sizeof(Foo));
  ASSERT_NE(nullptr, foo_dest);
  ASSERT_EQ(3, array_len(foo_dest));
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(foo_src[i].x, foo_dest[i].x);
    ASSERT_EQ(foo_src[i].y, foo_dest[i].y);
  }
  array_free(foo_dest);

  // Test appending zero elements
  dest = array_new(int, 2);
  array_append(dest, 42);
  ASSERT_EQ(1, array_len(dest));

  dest = (int *)array_ensure_append_n_func(dest, src, 0, sizeof(int));
  ASSERT_EQ(1, array_len(dest));
  ASSERT_EQ(42, dest[0]);
  array_free(dest);
}

// Test edge cases and combinations of new functions
TEST_F(ArrTest, testArrayFunctionsCombined) {
  // Test clear followed by append_n
  int *arr = array_new(int, 5);
  for (int i = 0; i < 8; i++) {
    array_append(arr, i * 10);
  }
  ASSERT_EQ(8, array_len(arr));

  arr = (int *)array_clear_func(arr, sizeof(int));
  ASSERT_EQ(0, array_len(arr));

  int new_data[] = {100, 200, 300};
  arr = (int *)array_ensure_append_n_func(arr, new_data, 3, sizeof(int));
  ASSERT_EQ(3, array_len(arr));
  ASSERT_EQ(100, arr[0]);
  ASSERT_EQ(200, arr[1]);
  ASSERT_EQ(300, arr[2]);
  array_free(arr);

  // Test multiple append_n operations
  arr = NULL;
  int batch1[] = {1, 2};
  int batch2[] = {3, 4, 5};
  int batch3[] = {6};

  arr = (int *)array_ensure_append_n_func(arr, batch1, 2, sizeof(int));
  ASSERT_EQ(2, array_len(arr));

  arr = (int *)array_ensure_append_n_func(arr, batch2, 3, sizeof(int));
  ASSERT_EQ(5, array_len(arr));

  arr = (int *)array_ensure_append_n_func(arr, batch3, 1, sizeof(int));
  ASSERT_EQ(6, array_len(arr));

  for (int i = 0; i < 6; i++) {
    ASSERT_EQ(i + 1, arr[i]);
  }
  array_free(arr);
}
