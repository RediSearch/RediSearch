
#include "gtest/gtest.h"
#include "trie/trie.h"
#include "trie/trie_type.h"

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

static int rangeFunc(const rune *u16, size_t nrune, void *ctx, void *payload) {
  size_t n;
  char *s = runesToStr(u16, nrune, &n);
  std::string xs(s, n);
  free(s);
  ElemSet *e = (ElemSet *)ctx;
  assert(e->end() == e->find(xs));
  e->insert(xs);
  return REDISEARCH_OK;
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
                        rangeFunc, &foundElements);
  return foundElements;
}

static ElemSet trieIterRange(Trie *t, const char *begin, const char *end) {
  return trieIterRange(t, begin, begin ? strlen(begin) : 0, end, end ? strlen(end) : 0);
}

TEST_F(TrieTest, testBasicRange) {
  Trie *t = NewTrie(NULL, Trie_Sort_Lex);
  rune rbuf[TRIE_INITIAL_STRING_LEN + 1];
  for (size_t ii = 0; ii < 1000; ++ii) {
    char buf[64];
    sprintf(buf, "%lu", (unsigned long)ii);
    auto n = trieInsert(t, buf);
    ASSERT_TRUE(n);
  }

  //TrieNode_Print(t->root, 0, 0);

  // Get all numbers within the lexical range of 1 and 1Z
  auto ret = trieIterRange(t, "1", "1Z");
  ASSERT_EQ(111, ret.size());

  // What does a NULL range return? the entire trie
  ret = trieIterRange(t, NULL, NULL);
  ASSERT_EQ(t->size, ret.size());

  // Min and max the same- should return only one value
  ret = trieIterRange(t, "1", "1");
  ASSERT_EQ(1, ret.size());

  ret = trieIterRange(t, "10", 2, "11", 2);
  ASSERT_EQ(11, ret.size());

  // Min and Min+1
  ret = trieIterRange(t, "10", 2, "10\x01", 3);
  ASSERT_EQ(1, ret.size());

  // No min, but has a max
  ret = trieIterRange(t, NULL, "5");
  ASSERT_EQ(445, ret.size());

  TrieType_Free(t);
}

TEST_F(TrieTest, testBasicRangeWithScore) {
  Trie *t = NewTrie(NULL, Trie_Sort_Score);
  rune rbuf[TRIE_INITIAL_STRING_LEN + 1];
  for (size_t ii = 0; ii < 1000; ++ii) {
    char buf[64];
    sprintf(buf, "%lu", (unsigned long)ii);
    auto n = trieInsert(t, buf);
    ASSERT_TRUE(n);
  }

  //TrieNode_Print(t->root, 0, 0);

  // Get all numbers within the lexical range of 1 and 1Z
  auto ret = trieIterRange(t, "1", "1Z");
  ASSERT_EQ(111, ret.size());

  // What does a NULL range return? the entire trie
  ret = trieIterRange(t, NULL, NULL);
  ASSERT_EQ(t->size, ret.size());

  // Min and max the same- should return only one value
  ret = trieIterRange(t, "1", "1");
  ASSERT_EQ(1, ret.size());

  ret = trieIterRange(t, "10", 2, "11", 2);
  ASSERT_EQ(11, ret.size());

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
  Trie *t = NewTrie(NULL, Trie_Sort_Score);
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

/**
 * This test ensures payload isn't corrupted when the trie changes.
 */
TEST_F(TrieTest, testPayload) {
  char buf1[] = "world";

  Trie *t = NewTrie(NULL, Trie_Sort_Score);

  RSPayload payload = { .data = buf1, .len = 2 };
  Trie_InsertStringBuffer(t, buf1, 2, 1, 1, &payload);
  payload.len = 4;
  Trie_InsertStringBuffer(t, buf1, 4, 1, 1, &payload);
  payload.len = 5;
  Trie_InsertStringBuffer(t, buf1, 5, 1, 1, &payload);
  payload.len = 3;
  Trie_InsertStringBuffer(t, buf1, 3, 1, 1, &payload);

  char buf2[] = "work";
  payload = { .data = buf2, .len = 4 };
  Trie_InsertStringBuffer(t, buf2, 4, 1, 1, &payload);


  // check for prefix of existing term
  // with exact returns null, w/o return load of next term
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 1, 0), "wo", 2), 0);
  ASSERT_TRUE((char*)Trie_GetValueStringBuffer(t, buf1, 1, 1) == NULL);

  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 2, 1), "wo", 2), 0);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 3, 1), "wor", 3), 0);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 4, 1), "worl", 4), 0);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 5, 1), "world", 5), 0);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf2, 4, 1), "work", 4), 0);

  ASSERT_EQ(Trie_Delete(t, buf1, 3), 1);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 2, 1), "wo", 2), 0);
  ASSERT_TRUE((char*)Trie_GetValueStringBuffer(t, buf1, 3, 1) == NULL);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 4, 1), "worl", 4), 0);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 5, 1), "world", 5), 0);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf2, 4, 1), "work", 4), 0);

  ASSERT_EQ(Trie_Delete(t, buf1, 4), 1);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 2, 1), "wo", 2), 0);
  ASSERT_TRUE((char*)Trie_GetValueStringBuffer(t, buf1, 3, 1) == NULL);
  ASSERT_TRUE((char*)Trie_GetValueStringBuffer(t, buf1, 4, 1) == NULL);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 5, 1), "world", 5), 0);
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf2, 4, 1), "work", 4), 0);

  // testing with exact = 0
  // "wor" node exists with NULL payload.
  ASSERT_TRUE((char*)Trie_GetValueStringBuffer(t, buf1, 3, 0) == NULL);
  // "worl" does not exist but is partial offset of =>`wor`+`ld`.
  // payload of `ld` is returned.
  ASSERT_EQ(strncmp((char*)Trie_GetValueStringBuffer(t, buf1, 4, 0), "world", 5), 0);

  TrieType_Free(t);
}

/**
 * This test check free callback.
 */
void trieFreeCb(void *val) {
  char **str = (char **)val;
  rm_free(*str);
}

TEST_F(TrieTest, testFreeCallback) {
  Trie *t = NewTrie(trieFreeCb, Trie_Sort_Score);

  char buf[] = "world";
  char *str = rm_strdup("hello");

  RSPayload payload = { .data = (char *)&str, .len = sizeof(str) };
  Trie_InsertStringBuffer(t, buf, 5, 1, 1, &payload);

  TrieType_Free(t);
}

void checkNext(TrieIterator *iter, const char *str) {
  char buf[16];
  rune *rstr = (rune *)&buf;
  t_len rlen;
  float score;
  RSPayload payload;

  TrieIterator_Next(iter, &rstr, &rlen, &payload, &score, NULL);
  size_t len;
  char *res_str = runesToStr(rstr, rlen, &len);
  ASSERT_STREQ(res_str, str);
  rm_free(res_str);
}

TEST_F(TrieTest, testLexOrder) {
  Trie *t = NewTrie(trieFreeCb, Trie_Sort_Lex);

  trieInsert(t, "hello");
  trieInsert(t, "world");
  trieInsert(t, "helen");
  trieInsert(t, "foo");
  trieInsert(t, "bar");
  trieInsert(t, "help");

  TrieIterator *iter = Trie_Iterate(t, "", 0, 0, 1);
  checkNext(iter, "bar");
  checkNext(iter, "foo");
  checkNext(iter, "helen");
  checkNext(iter, "hello");
  checkNext(iter, "help");
  checkNext(iter, "world");
  TrieIterator_Free(iter);

  Trie_Delete(t, "bar", 3);
  Trie_Delete(t, "hello", 5);
  Trie_Delete(t, "world", 5);

  iter = Trie_Iterate(t, "", 0, 0, 1);
  checkNext(iter, "foo");
  checkNext(iter, "helen");
  checkNext(iter, "help");
  TrieIterator_Free(iter);

  TrieType_Free(t);
}

bool trieInsertByScore(Trie *t, const char *s, float score) {
  return Trie_InsertStringBuffer(t, s, strlen(s), score, 1, NULL);
}

TEST_F(TrieTest, testScoreOrder) {
  Trie *t = NewTrie(trieFreeCb, Trie_Sort_Score);

  trieInsertByScore(t, "hello", 4);
  trieInsertByScore(t, "world", 2);
  trieInsertByScore(t, "foo", 6);
  trieInsertByScore(t, "bar", 1);
  trieInsertByScore(t, "help", 3);
  trieInsertByScore(t, "helen", 5);

  TrieIterator *iter = Trie_Iterate(t, "", 0, 0, 1);
  checkNext(iter, "foo");
  checkNext(iter, "helen");
  checkNext(iter, "hello");
  checkNext(iter, "help");
  checkNext(iter, "world");
  checkNext(iter, "bar");
  TrieIterator_Free(iter);

  Trie_Delete(t, "hello", 5);
  Trie_Delete(t, "world", 5);
  Trie_Delete(t, "bar", 3);

  iter = Trie_Iterate(t, "", 0, 0, 1);
  checkNext(iter, "foo");
  checkNext(iter, "helen");
  checkNext(iter, "help");
  TrieIterator_Free(iter);

  TrieType_Free(t);
}

/* leave for future benchmarks if needed
TEST_F(TrieTest, testbenchmark) {
  Trie *t = NewTrie(trieFreeCb, Trie_Sort_Lex);
  char buf[128];
  int count = 1024 * 1024 * 8;
  for (size_t i = 0; i < count; ++i) {
    int random = rand() % (count / 5);
    sprintf(buf, "%x", random);
    Trie_InsertStringBuffer(t, buf, strlen(buf), 1, 0,10 NULL);
  }

  TrieType_Free(t);
}*/
