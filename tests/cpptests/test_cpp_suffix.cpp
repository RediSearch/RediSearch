
#include "gtest/gtest.h"

#include "suffix.h"

class SuffixTest : public ::testing::Test {};

TEST_F(SuffixTest, testBasic) {
  TrieMap *t = NewTrieMap();

  // Add first word
  const char *str1 = "hello";
  writeSuffixTrie(t, str1, strlen(str1));
  ASSERT_EQ(4, t->cardinality);
  writeSuffixTrie(t, str1, strlen(str1));
  ASSERT_EQ(4, t->cardinality);

  // Add a second word with 4 suffix matching
  const char *str2 = "jello";
  writeSuffixTrie(t, str2, strlen(str2));
  ASSERT_EQ(5, t->cardinality);

  // Add a second word with 4 suffix matching
  const char *str3 = "hell";
  writeSuffixTrie(t, str3, strlen(str3));
  ASSERT_EQ(8, t->cardinality);

  // Add a second word with 4 suffix matching
  const char *str4 = "shell";
  writeSuffixTrie(t, str4, strlen(str4));
  ASSERT_EQ(9, t->cardinality);

  // find all substrings
  const char *strs[4] = { str1, str2, str3, str4 };
  for (int i = 0; i < 4; ++i) {
    for (int j = 0; j < strlen(strs[i]) - MIN_SUFFIX; ++j) {
      ASSERT_NE(TRIEMAP_NOTFOUND, TrieMap_Find(t, strs[i] + j, strlen(strs[i]) - j));
    }
  }
  /*
  arrayof(void *) results;
  ASSERT_EQ(0, TrieMap_FindPrefixes(t, "world", strlen("world"), &results));
  ASSERT_EQ(1, TrieMap_FindPrefixes(t, str1, strlen(str1), &results));
  ASSERT_EQ(2, TrieMap_FindPrefixes(t, str3, strlen(str3), &results));
  */


  //  ASSERT_TRUE(n);

  SuffixTrieFree(t);
}
