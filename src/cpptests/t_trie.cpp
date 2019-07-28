#include <gtest/gtest.h>
#include <trie/trie.h>
#include <trie/trie_type.h>
#include <set>
#include <string>

typedef std::set<std::string> ElemSet;

class TrieTest : public ::testing::Test {};

static bool trieInsert(Trie *t, const char *s, size_t n) {
  return Trie_InsertStringBuffer(t, s, n, 1, 1, NULL);
}
static bool trieInsert(Trie *t, const char *s) {
  return trieInsert(t, s, strlen(s));
}
static bool trieInsert(Trie *t, const std::string &s) {
  return trieInsert(t, s.c_str(), s.size());
}

static ElemSet trieIterRange(Trie *t, const char *begin, size_t nbegin, const char *end,
                             size_t nend) {
  rune r1[256] = {0};
  rune r2[256] = {0};
  size_t nr1, nr2;

  rune *r1Ptr = r1;
  rune *r2Ptr = r2;

  nr1 = strToRunesN(begin, nbegin, r1);
  nr2 = strToRunesN(end, nend, r2);

  if (!begin) {
    r1Ptr = NULL;
    nr1 = -1;
  }

  if (!end) {
    r2Ptr = NULL;
    nr2 = -1;
  }

  ElemSet foundElements;
  TrieNode_IterateRange(t->root, r1Ptr, nr1, true, r2Ptr, nr2, false,
                        [](const rune *u16, size_t nrune, void *ctx) {
                          size_t n;
                          char *s = runesToStr(u16, nrune, &n);
                          std::string xs(s, n);
                          free(s);
                          ElemSet *e = (ElemSet *)ctx;
                          ASSERT_EQ(e->end(), e->find(xs));
                          e->insert(xs);
                        },
                        &foundElements);
  return foundElements;
}

static ElemSet trieIterRange(Trie *t, const char *begin, const char *end) {
  return trieIterRange(t, begin, begin ? strlen(begin) : 0, end, end ? strlen(end) : 0);
}

TEST_F(TrieTest, testBasicRange) {
  Trie *t = NewTrie();
  rune rbuf[TRIE_INITIAL_STRING_LEN + 1];
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
}

/**
 * This test ensures that the stack isn't overflown from all the frames.
 * The maximum trie depth cannot be greater than the maximum length of the
 * string.
 */
TEST_F(TrieTest, testDeepEntry) {
  Trie *t = NewTrie();
  const size_t maxbuf = TRIE_INITIAL_STRING_LEN - 1;
  char manyOnes[maxbuf + 1];
  for (size_t ii = 0; ii < maxbuf; ++ii) {
    manyOnes[ii] = '1';
  }

  manyOnes[maxbuf] = 0;
  size_t manyLen = strlen(manyOnes);

  for (size_t ii = 0; ii < manyLen; ++ii) {
    size_t curlen = ii + 1;
    int rc = trieInsert(t, manyOnes, curlen);
    ASSERT_TRUE(rc);
    // printf("Inserting with len=%u: %d\n", curlen, rc);
  }

  auto ret = trieIterRange(t, "1", "1Z");
  ASSERT_EQ(maxbuf, ret.size());
  TrieType_Free(t);
}
