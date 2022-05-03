
#include "gtest/gtest.h"
#include "triemap/triemap.h"

#include <set>
#include <string>

typedef std::set<std::string> ElemSet;

class TrieMapTest : public ::testing::Test {};

/*
TEST_F(TrieTest, testBasicRange) {
  TrieMap *t = NewTrieMap();
  char rbuf[256];
  for (size_t ii = 0; ii < 1000; ++ii) {
    char buf[64];
    sprintf(buf, "%lu", (unsigned long)ii);
    auto n = trieInsert(t, buf);
    ASSERT_TRUE(n);
  }

  // Get all numbers within the lexical range of 1 and 1Z
  auto ret = trieIterRange(t, "1", "1Z");
  ASSERT_EQ(111, ret.size());

  // What does a NULL range return? the entire trie
  ret = trieIterRange(t, NULL, NULL);
  ASSERT_EQ(t->size, ret.size());

  // Min and max the same- should return only one value
  ret = trieIterRange(t, "1", "1");
  ASSERT_EQ(1, ret.size());

  // Min and Min+1
  ret = trieIterRange(t, "10", 2, "10\x01", 3);
  ASSERT_EQ(1, ret.size());

  // No min, but has a max
  ret = trieIterRange(t, NULL, "5");
  ASSERT_EQ(445, ret.size());

  TrieType_Free(t);
}*/

TrieMap *loadTrieMap() {
  TrieMap *t = NewTrieMap();
  const char *words[] = {"he", "her", "hell", "help", "helper", "hello", "hello world"};
  for (int i = 0; i < 7; ++i) {
    TrieMap_Add(t, (char *)words[i], strlen(words[i]), (void *)words[i], NULL);
  }
  return t;
}

/**
 * This test ensures that the stack isn't overflown from all the frames.
 * The maximum trie depth cannot be greater than the maximum length of the
 * string.
 */
TEST_F(TrieMapTest, testPrefix) {
  TrieMap *t = loadTrieMap();
  TrieMapIterator *it = TrieMap_Iterate(t, "hel", strlen("hel"));
  
  char *ptr;
  tm_len_t len;
  void *val;
  int numRes = 0;
  
  while (TrieMapIterator_Next(it, &ptr, &len, &val)) {
    ++numRes;
  }
  ASSERT_EQ(numRes, 5);

  TrieMap_Free(t, NULL);
}
