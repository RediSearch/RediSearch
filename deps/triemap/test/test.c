#include "../triemap.h"
#include "minunit.h"
#include "time_sample.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

void testTrie() {
  TrieMap *tm = new TrieMap();

  char buf[32];

  for (int i = 0; i < 100; i++) {
    sprintf(buf, "key%d", i);
    int *pi = malloc(sizeof(int));
    *pi = i;
    int rc = tm->Add(buf, strlen(buf), NULL, NULL);
    mu_check(rc);
    rc = tm->Add(buf, strlen(buf), pi, NULL);
    mu_check(rc == 0);
  }
  mu_assert_int_eq(100, tm->cardinality);

  // check insertion of empty node
  int *empty = malloc(sizeof(int));
  *empty = 1337;
  mu_check(1 == tm->Add("", 0, NULL, NULL));
  mu_assert_int_eq(101, tm->cardinality);
  mu_check(0 == tm->Add("", 0, empty, NULL));
  mu_assert_int_eq(101, tm->cardinality);
  void *ptr = tm->Find("", 0);
  mu_check(ptr != TRIEMAP_NOTFOUND);
  mu_check(*(int *)ptr == 1337);
  mu_check(tm->Delete("", 0, NULL));
  mu_assert_int_eq(100, tm->cardinality);

  // check that everything was found
  for (int i = 0; i < 100; i++) {
    sprintf(buf, "key%d", i);

    void *p = tm->Find(buf, strlen(buf));
    mu_check(p != NULL);
    mu_check(p != TRIEMAP_NOTFOUND);
    mu_check(*(int *)p == i);
  }

  for (int i = 0; i < 100; i++) {
    sprintf(buf, "key%d", i);

    int rc = tm->Delete(buf, strlen(buf), NULL);
    mu_check(rc);
    rc = tm->Delete(buf, strlen(buf), NULL);
    mu_check(rc == 0);
    mu_check(tm->cardinality == 100 - i - 1);
  }

  TrieMap_Free(tm, NULL);
}

void testTrieIterator() {
  TrieMap *tm = new TrieMap();

  char buf[32];

  for (int i = 0; i < 100; i++) {
    sprintf(buf, "key%d", i);
    int *pi = malloc(sizeof(int));
    *pi = i;
    tm->Add(buf, strlen(buf), pi, NULL);
  }
  mu_assert_int_eq(100, tm->cardinality);
  mu_check(1 == tm->Add("", 0, NULL, NULL));
  mu_assert_int_eq(101, tm->cardinality);

  TrieMapIterator *it = tm->Iterate("key1", 4);
  mu_check(it);
  int count = 0;

  char *str = NULL;
  tm_len_t len = 0;
  void *ptr = NULL;

  while (0 != it->Next(&str, &len, &ptr)) {
    mu_check(!strncmp("key1", str, 4));
    mu_check(str);
    mu_check(len > 0);
    mu_check(ptr);
    mu_check(*(int *)ptr > 0);
    count++;
  }
  mu_assert_int_eq(11, count);
  delete it;

  /* Test iteration starting from the empty node */
  it = tm->Iterate("", 0);
  mu_check(it);
  mu_check(it->Next(&str, &len, &ptr));

  mu_check(len == 0);
  mu_check(ptr == NULL);

  count = 0;
  while (it->Next(&str, &len, &ptr)) {
    mu_check(str);
    mu_check(len > 0);
    mu_check(ptr);

    count++;
  }
  mu_assert_int_eq(100, count);
  delete it;

  TrieMap_Free(tm, NULL);
}

struct rmstring {
  char *_p;
  rmstring(const char *p) {
    _p = rm_strdup(p);
  }
  rmstring(const char *fmt, ...) {
  }
  ~rmstring() {
    rm_free(_p);
  }
  operator const char *() const { return _p; }
  operator char *() { return _p; }
  const char *operator*() const { return _p; }
  char *operator*() { return _p; }
};

void testRandomWalk() {
  TrieMap<rmstring> tm;

  char buf[32];
  int N = 1000;

  for (int i = 0; i < N; i++) {
    sprintf(buf, "key%d", i);
    tm.Add(buf, strlen(buf), new rmstring(buf));
  }
  char *sbuf;
  tm_len_t len;
  rmstring* ptr;
  for (int i = 0; i < 100; i++) {
    int rc = tm.RandomKey(&sbuf, &len, &ptr);
    mu_check(rc);
    mu_check(ptr);

    free(sbuf);
  }

  for (int i = 1; i < 9; i++) {
    char prefix[5];
    sprintf(prefix, "key%d", i);
    for (int x = 0; x < 5; x++) {
      void *val = tm.RandomValueByPrefix(prefix, strlen(prefix));

      mu_check(val);
      mu_check(!strncmp((char *)val, prefix, strlen(prefix)));
    }
  }

  void *p = tm->RandomValueByPrefix("x2x2x2", 6);
  mu_check(p == NULL);
}

void testRandom() {
  TrieMap *tm = new TrieMap();

  char buf[0xfffff + 10];
  int N = 1000;
  for (int i = 0; i < N; i++) {
    int n = rand() % sizeof(buf);
    for (int j = 0; j < n; j++) {
      buf[j] = rand() % 255;
    }

    int *pi = malloc(sizeof(int));
    *pi = i + 1;
    tm->Add(buf, n, pi, NULL);
    // if (i % 1000 == 0) printf("%d\n", i);
  }
  mu_assert_int_eq(N, tm->cardinality);
  // mu_check(1 == tm->Add("", 0, NULL, NULL));
  // mu_assert_int_eq(101, tm->cardinality);

  TrieMapIterator *it = tm->Iterate("", 0);
  mu_check(it);
  int count = 0;

  char *str = NULL;
  tm_len_t len = 0;
  void *ptr = NULL;

  while (0 != it->Next(&str, &len, &ptr)) {
    // mu_check(!strncmp("key1", str, 4));
    mu_check(str);
    mu_check(len > 0);
    mu_check(ptr);
    mu_check(*(int *)ptr > 0);
    count++;
  }
  mu_assert_int_eq(N, count);
  delete it;
}

int main(int argc, char **argv) {
  MU_RUN_TEST(testTrie);
  MU_RUN_TEST(testTrieIterator);
  MU_RUN_TEST(testRandomWalk);
  MU_RUN_TEST(testRandom);

  MU_REPORT();
  return minunit_status;
}
