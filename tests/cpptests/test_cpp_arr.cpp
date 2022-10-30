#include <gtest/gtest.h>
#include <util/arr.h>

class ArrTest : public ::testing::Test {};

typedef struct Foo {
  size_t x;
  double y;
} Foo;

TEST_F(ArrTest, testStruct) {
  Foo *arr = (Foo *)array_new(Foo, 8);

  for (size_t i = 0; i < 10; i++) {
    arr = (Foo *)array_append(arr, (Foo){i});
    ASSERT_EQ(i + 1, array_len(arr));
  }

  for (size_t i = 0; i < 10; i++) {
    ASSERT_EQ(i, arr[i].x);
  }
  // array_foreach(arr, elem, printf("%d\n", elem.x));
  array_free(arr);
}

TEST_F(ArrTest, testScalar) {
  int *ia = array_new(int, 8);
  for (size_t i = 0; i < 100; i++) {
    ia = array_append(ia, i);
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
    a = array_append(a, strdup(strs[i]));
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
  const char *strs[] = {"foo", "bar", "baz", NULL};
  const char **a = array_new(const char *, 16);
  size_t i = 0;
  for (i = 0; strs[i] != NULL; i++) {
    a = array_append(a, strs[i]);
    ASSERT_EQ(i + 1, array_len(a));
    ASSERT_STREQ(strs[i], array_tail(a));
  }
  a = array_trimm_cap(a, 2);
  ASSERT_EQ(array_len(a), 2);
  array_trimm_len(a, 1);
  ASSERT_EQ(array_len(a), 1);
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
  a = array_append(a, 42);
  a = array_del(a, 0);
  ASSERT_EQ(0, array_len(a));

  // repopulate
  for (size_t ii = 0; ii < 10; ++ii) {
    a = array_append(a, ii);
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