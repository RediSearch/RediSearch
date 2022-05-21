#include "../triemap.h"
#include "minunit.h"
#include "time_sample.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

void testTrie() {
  TrieMap *tm = NewTrieMap();

  char buf[32];

  for (int i = 0; i < 100; i++) {
    sprintf(buf, "key%d", i);
    int *pi = malloc(sizeof(int));
    *pi = i;
    int rc = TrieMap_Add(tm, buf, strlen(buf), NULL, NULL);
    mu_check(rc);
    rc = TrieMap_Add(tm, buf, strlen(buf), pi, NULL);
    mu_check(rc == 0);
  }
  mu_assert_int_eq(100, tm->cardinality);

  // check insertion of empty node
  int *empty = malloc(sizeof(int));
  *empty = 1337;
  mu_check(1 == TrieMap_Add(tm, "", 0, NULL, NULL));
  mu_assert_int_eq(101, tm->cardinality);
  mu_check(0 == TrieMap_Add(tm, "", 0, empty, NULL));
  mu_assert_int_eq(101, tm->cardinality);
  void *ptr = TrieMap_Find(tm, "", 0);
  mu_check(ptr != TRIEMAP_NOTFOUND);
  mu_check(*(int *)ptr == 1337);
  mu_check(TrieMap_Delete(tm, "", 0, NULL));
  mu_assert_int_eq(100, tm->cardinality);

  // check that everything was found
  for (int i = 0; i < 100; i++) {
    sprintf(buf, "key%d", i);

    void *p = TrieMap_Find(tm, buf, strlen(buf));
    mu_check(p != NULL);
    mu_check(p != TRIEMAP_NOTFOUND);
    mu_check(*(int *)p == i);
  }

  for (int i = 0; i < 100; i++) {
    sprintf(buf, "key%d", i);

    int rc = TrieMap_Delete(tm, buf, strlen(buf), NULL);
    mu_check(rc);
    rc = TrieMap_Delete(tm, buf, strlen(buf), NULL);
    mu_check(rc == 0);
    mu_check(tm->cardinality == 100 - i - 1);
  }

  TrieMap_Free(tm, NULL);
}

void testTrieIterator() {
  TrieMap *tm = NewTrieMap();

  char buf[32];

  for (int i = 0; i < 100; i++) {
    sprintf(buf, "key%d", i);
    int *pi = malloc(sizeof(int));
    *pi = i;
    TrieMap_Add(tm, buf, strlen(buf), pi, NULL);
  }
  mu_assert_int_eq(100, tm->cardinality);
  mu_check(1 == TrieMap_Add(tm, "", 0, NULL, NULL));
  mu_assert_int_eq(101, tm->cardinality);

  TrieMapIterator *it = TrieMap_Iterate(tm, "key1", 4);
  mu_check(it);
  int count = 0;

  char *str = NULL;
  tm_len_t len = 0;
  void *ptr = NULL;

  while (0 != TrieMapIterator_Next(it, &str, &len, &ptr)) {
    mu_check(!strncmp("key1", str, 4));
    mu_check(str);
    mu_check(len > 0);
    mu_check(ptr);
    mu_check(*(int *)ptr > 0);
    count++;
  }
  mu_assert_int_eq(11, count);
  TrieMapIterator_Free(it);

  /* Test iteration starting from the empty node */
  it = TrieMap_Iterate(tm, "", 0);
  mu_check(it);
  mu_check(TrieMapIterator_Next(it, &str, &len, &ptr));

  mu_check(len == 0);
  mu_check(ptr == NULL);

  count = 0;
  while (TrieMapIterator_Next(it, &str, &len, &ptr)) {
    mu_check(str);
    mu_check(len > 0);
    mu_check(ptr);

    count++;
  }
  mu_assert_int_eq(100, count);
  TrieMapIterator_Free(it);

  TrieMap_Free(tm, NULL);
}

void testRandomWalk() {
  TrieMap *tm = NewTrieMap();

  char buf[32];
  int N = 1000;

  for (int i = 0; i < N; i++) {
    sprintf(buf, "key%d", i);
    TrieMap_Add(tm, buf, strlen(buf), rm_strdup(buf), NULL);
  }
  char *sbuf;
  tm_len_t len;
  void *ptr;
  for (int i = 0; i < 100; i++) {
    int rc = TrieMap_RandomKey(tm, &sbuf, &len, &ptr);
    mu_check(rc);
    mu_check(ptr);

    free(sbuf);
  }

  for (int i = 1; i < 9; i++) {
    char prefix[5];
    sprintf(prefix, "key%d", i);
    for (int x = 0; x < 5; x++) {
      void *val = TrieMap_RandomValueByPrefix(tm, prefix, strlen(prefix));

      mu_check(val);
      // printf("%d %s\n", i, (char*)val);
      mu_check(!strncmp((char *)val, prefix, strlen(prefix)));
    }
  }

  void *p = TrieMap_RandomValueByPrefix(tm, "x2x2x2", 6);
  mu_check(p == NULL);

  TrieMap_Free(tm, NULL);
}

void testRandom() {
  TrieMap *tm = NewTrieMap();

  char buf[0xfffff + 10];
  int N = 1000;
  for (int i = 0; i < N; i++) {
    int n = rand() % sizeof(buf);
    for (int j = 0; j < n; j++) {
      buf[j] = rand() % 255;
    }

    int *pi = malloc(sizeof(int));
    *pi = i + 1;
    TrieMap_Add(tm, buf, n, pi, NULL);
    // if (i % 1000 == 0) printf("%d\n", i);
  }
  mu_assert_int_eq(N, tm->cardinality);
  // mu_check(1 == TrieMap_Add(tm, "", 0, NULL, NULL));
  // mu_assert_int_eq(101, tm->cardinality);

  TrieMapIterator *it = TrieMap_Iterate(tm, "", 0);
  mu_check(it);
  int count = 0;

  char *str = NULL;
  tm_len_t len = 0;
  void *ptr = NULL;

  while (0 != TrieMapIterator_Next(it, &str, &len, &ptr)) {
    // mu_check(!strncmp("key1", str, 4));
    mu_check(str);
    mu_check(len > 0);
    mu_check(ptr);
    mu_check(*(int *)ptr > 0);
    count++;
  }
  mu_assert_int_eq(N, count);
  TrieMapIterator_Free(it);
}

int main(int argc, char **argv) {
  MU_RUN_TEST(testTrie);
  MU_RUN_TEST(testTrieIterator);
  MU_RUN_TEST(testRandomWalk);
  MU_RUN_TEST(testRandom);

  MU_REPORT();
  return minunit_status;
}
