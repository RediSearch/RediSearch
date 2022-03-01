
#include "gtest/gtest.h"

#include "suffix.h"

class SuffixTest : public ::testing::Test {};

TEST_F(SuffixTest, testBasic) {
  TrieMap *t = NewTrieMap();

  const char *str = "hello";
  writeSuffixTrie(t, str, strlen(str));
  ASSERT_EQ(5, t->size);
  //  ASSERT_TRUE(n);

  SuffixTrieFree(t);
}
