
#include "gtest/gtest.h"
#include "triemap/triemap.h"

#include <set>
#include <string>

class TrieMapTest : public ::testing::Test {};

typedef int(affixFunc)(TrieMapIterator *, char **, tm_len_t *, void **);

TrieMap *loadTrieMap() {
  TrieMap *t = NewTrieMap();
  const char *words[] = {"he", "her", "hell", "help", "helper", "hello",
                         "hello world", "towel", "dealer", "bell"};
  for (int i = 0; i < 10; ++i) {
    TrieMap_Add(t, (char *)words[i], strlen(words[i]), (void *)words[i], NULL);
  }
  return t;
}

int testTMNumResults(TrieMap *t, const char *str, tm_iter_mode mode) {
  char *ptr;
  tm_len_t len;
  void *val;
  int numRes = 0;
  
  TrieMapIterator *it = TrieMap_Iterate(t, str, strlen(str));
  it->mode = mode;
  affixFunc *f = (mode == TM_PREFIX_MODE) ? TrieMapIterator_Next : TrieMapIterator_NextContains;
  while (f(it, &ptr, &len, &val)) {
    ptr[len] = '\0';
    printf("numres %d string %s\n", numRes, ptr);
    ++numRes;
  }
  TrieMapIterator_Free(it);
  return numRes;
}

void freeCb(void *val) {}
/*
TEST_F(TrieMapTest, testPrefix) {
  TrieMap *t = loadTrieMap();

  ASSERT_EQ(testTMNumResults(t, "he", TM_PREFIX_MODE), 7);
  ASSERT_EQ(testTMNumResults(t, "hel", TM_PREFIX_MODE), 5);
  ASSERT_EQ(testTMNumResults(t, "hell", TM_PREFIX_MODE), 3);

  TrieMap_Free(t, freeCb);
}

TEST_F(TrieMapTest, testSuffix) {
  TrieMap *t = loadTrieMap();
  
  ASSERT_EQ(testTMNumResults(t, "he", TM_SUFFIX_MODE), 1);
  ASSERT_EQ(testTMNumResults(t, "er", TM_SUFFIX_MODE), 3);

  TrieMap_Free(t, freeCb);
}
*/
TEST_F(TrieMapTest, testContains) {
  TrieMap *t = loadTrieMap();
  
  //ASSERT_EQ(testTMNumResults(t, "wel", TM_CONTAINS_MODE), 1);
  ASSERT_EQ(testTMNumResults(t, "el", TM_CONTAINS_MODE), 7);
  //ASSERT_EQ(testTMNumResults(t, "ell", TM_CONTAINS_MODE), 4);
  //ASSERT_EQ(testTMNumResults(t, "ll", TM_CONTAINS_MODE), 4);

  TrieMap_Free(t, freeCb);
}
