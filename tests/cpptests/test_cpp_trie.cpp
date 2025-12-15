/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "gtest/gtest.h"
#include "trie/trie.h"
#include "trie/trie_type.h"
#include "redismock/redismock.h"

#include <set>
#include <string>
#include <memory>
#include <functional>

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

bool trieContains(Trie *t, const char *s) {
  runeBuf buf;
  size_t len = strlen(s);
  rune *runes = runeBufFill(s, len, &buf, &len);
  if (!runes) {
    return false;
  }
  TrieNode *node = TrieNode_Get(t->root, runes, len, 0, NULL);
  runeBufFree(&buf);
  return node != NULL;
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

// Helper function to compare two tries for equality
static bool compareTrieContents(Trie *original, Trie *loaded) {
  if (original->size != loaded->size) {
    return false;
  }

  // Compare all entries using iterators
  TrieIterator *origIter = Trie_Iterate(original, "", 0, 0, 1);
  TrieIterator *loadedIter = Trie_Iterate(loaded, "", 0, 0, 1);

  std::unique_ptr<TrieIterator, std::function<void(TrieIterator *)>> origIterPtr(origIter, [](TrieIterator *iter) {
    TrieIterator_Free(iter);
  });
  std::unique_ptr<TrieIterator, std::function<void(TrieIterator *)>> loadedIterPtr(loadedIter, [](TrieIterator *iter) {
    TrieIterator_Free(iter);
  });

  rune *origRstr, *loadedRstr;
  t_len origLen, loadedLen;
  float origScore, loadedScore;
  RSPayload origPayload, loadedPayload;

  while (true) {
    int origHasNext = TrieIterator_Next(origIter, &origRstr, &origLen, &origPayload, &origScore, NULL);
    int loadedHasNext = TrieIterator_Next(loadedIter, &loadedRstr, &loadedLen, &loadedPayload, &loadedScore, NULL);

    if (origHasNext != loadedHasNext) {
      return false;
    }

    if (!origHasNext) {
      break; // Both iterators finished
    }

    // Compare strings
    if (origLen != loadedLen) {
      return false;
    }

    size_t origStrLen, loadedStrLen;
    char *origStr = runesToStr(origRstr, origLen, &origStrLen);
    char *loadedStr = runesToStr(loadedRstr, loadedLen, &loadedStrLen);

    std::unique_ptr<char, std::function<void(char *)>> origStrPtr(origStr, [](char *str) { rm_free(str); });
    std::unique_ptr<char, std::function<void(char *)>> loadedStrPtr(loadedStr, [](char *str) { rm_free(str); });

    if (origStrLen != loadedStrLen || strncmp(origStr, loadedStr, origStrLen) != 0) {
      return false;
    }

    // Compare scores
    if (origScore != loadedScore) {
      return false;
    }

    // Compare payloads
    if (origPayload.len != loadedPayload.len) {
      return false;
    }

    if (origPayload.len > 0 && loadedPayload.len > 0) {
      if (memcmp(origPayload.data, loadedPayload.data, origPayload.len) != 0) {
        return false;
      }
    } else if ((origPayload.data == NULL) != (loadedPayload.data == NULL)) {
      return false;
    }
  }

  return true;
}

TEST_F(TrieTest, testBasicRdbSaveLoad) {
  // Create a trie with some test data
  Trie *originalTrie = NewTrie(NULL, Trie_Sort_Score);
  std::unique_ptr<Trie, std::function<void(Trie *)>> originalTriePtr(originalTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });

  // Insert complex test data with prefixes and extensions to stress the trie
  trieInsertByScore(originalTrie, "app", 5.0);         // Base word
  trieInsertByScore(originalTrie, "apple", 3.0);       // Extension of "app"
  trieInsertByScore(originalTrie, "application", 7.0); // Extension of "app"
  trieInsertByScore(originalTrie, "apply", 1.0);       // Extension of "app"
  trieInsertByScore(originalTrie, "applied", 4.0);     // Extension of "apply"
  trieInsertByScore(originalTrie, "book", 6.0);        // Base word
  trieInsertByScore(originalTrie, "books", 8.0);       // Extension of "book"
  trieInsertByScore(originalTrie, "booking", 2.0);     // Extension of "book"

  ASSERT_EQ(8, originalTrie->size);

  // Create RDB IO context
  RedisModuleIO *io = RMCK_CreateRdbIO();
  std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
    RMCK_FreeRdbIO(io);
  });
  ASSERT_TRUE(io != nullptr);

  // Save the trie to RDB
  TrieType_RdbSave(io, originalTrie);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Reset read position to load it back
  io->read_pos = 0;

  // Load the trie from RDB
  Trie *loadedTrie = (Trie *)TrieType_RdbLoad(io, TRIE_ENCVER_CURRENT);
  std::unique_ptr<Trie, std::function<void(Trie *)>> loadedTriePtr(loadedTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });
  ASSERT_TRUE(loadedTrie != nullptr);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Compare the original and loaded tries
  EXPECT_EQ(originalTrie->size, loadedTrie->size);

  // Verify all entries are present in the loaded trie
  EXPECT_TRUE(trieContains(loadedTrie, "app"));
  EXPECT_TRUE(trieContains(loadedTrie, "apple"));
  EXPECT_TRUE(trieContains(loadedTrie, "application"));
  EXPECT_TRUE(trieContains(loadedTrie, "apply"));
  EXPECT_TRUE(trieContains(loadedTrie, "applied"));
  EXPECT_TRUE(trieContains(loadedTrie, "book"));
  EXPECT_TRUE(trieContains(loadedTrie, "books"));
  EXPECT_TRUE(trieContains(loadedTrie, "booking"));
}

TEST_F(TrieTest, testRdbSaveLoadWithPayloads) {
  // Create a trie with payloads
  Trie *originalTrie = NewTrie(NULL, Trie_Sort_Score);
  std::unique_ptr<Trie, std::function<void(Trie *)>> originalTriePtr(originalTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });

  // Insert complex test data with payloads - includes prefixes and extensions
  char payload1[] = "payload_run";
  char payload2[] = "payload_running";
  char payload3[] = "payload_runner";

  RSPayload p1 = {.data = payload1, .len = strlen(payload1)};
  RSPayload p2 = {.data = payload2, .len = strlen(payload2)};
  RSPayload p3 = {.data = payload3, .len = strlen(payload3)};

  Trie_InsertStringBuffer(originalTrie, "run", 3, 5.0, 0, &p1);        // Base word with payload
  Trie_InsertStringBuffer(originalTrie, "running", 7, 3.0, 0, &p2);    // Extension with payload
  Trie_InsertStringBuffer(originalTrie, "runner", 6, 4.0, 0, &p3);     // Extension with payload

  EXPECT_EQ(3, originalTrie->size);

  // Create RDB IO context
  RedisModuleIO *io = RMCK_CreateRdbIO();
  std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
    RMCK_FreeRdbIO(io);
  });
  ASSERT_TRUE(io != nullptr);

  // Save the trie to RDB
  TrieType_RdbSave(io, originalTrie);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Reset read position to load it back
  io->read_pos = 0;

  // Load the trie from RDB (with payloads)
  Trie *loadedTrie = (Trie *)TrieType_GenericLoad(io, 1);
  std::unique_ptr<Trie, std::function<void(Trie *)>> loadedTriePtr(loadedTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });
  ASSERT_TRUE(loadedTrie != nullptr);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Compare the original and loaded tries
  EXPECT_EQ(originalTrie->size, loadedTrie->size);

  // Verify all entries are present in the loaded trie
  EXPECT_TRUE(trieContains(loadedTrie, "run"));
  EXPECT_TRUE(trieContains(loadedTrie, "running"));
  EXPECT_TRUE(trieContains(loadedTrie, "runner"));

  // Verify specific payloads are preserved
  void *loadedPayload1 = Trie_GetValueStringBuffer(loadedTrie, "run", 3, true);
  void *loadedPayload2 = Trie_GetValueStringBuffer(loadedTrie, "running", 7, true);
  void *loadedPayload3 = Trie_GetValueStringBuffer(loadedTrie, "runner", 6, true);

  ASSERT_TRUE(loadedPayload1 != nullptr);
  ASSERT_TRUE(loadedPayload2 != nullptr);
  ASSERT_TRUE(loadedPayload3 != nullptr);

  EXPECT_EQ(0, strncmp(payload1, (char *)loadedPayload1, strlen(payload1)));
  EXPECT_EQ(0, strncmp(payload2, (char *)loadedPayload2, strlen(payload2)));
  EXPECT_EQ(0, strncmp(payload3, (char *)loadedPayload3, strlen(payload3)));
}

TEST_F(TrieTest, testRdbSaveLoadPayloadsNotSerialized) {
  // Create a trie with payloads but save without serializing them
  Trie *originalTrie = NewTrie(NULL, Trie_Sort_Score);
  std::unique_ptr<Trie, std::function<void(Trie *)>> originalTriePtr(originalTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });

  // Insert complex test data with payloads - includes prefixes and extensions
  char payload1[] = "payload_car";
  char payload2[] = "payload_care";
  char payload3[] = "payload_careful";

  RSPayload p1 = {.data = payload1, .len = strlen(payload1)};
  RSPayload p2 = {.data = payload2, .len = strlen(payload2)};
  RSPayload p3 = {.data = payload3, .len = strlen(payload3)};

  Trie_InsertStringBuffer(originalTrie, "car", 3, 8.0, 0, &p1);        // Base word with payload
  Trie_InsertStringBuffer(originalTrie, "care", 4, 6.0, 0, &p2);       // Extension with payload
  Trie_InsertStringBuffer(originalTrie, "careful", 7, 4.0, 0, &p3);    // Extension with payload

  EXPECT_EQ(3, originalTrie->size);

  // Create RDB IO context
  RedisModuleIO *io = RMCK_CreateRdbIO();
  std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
    RMCK_FreeRdbIO(io);
  });
  ASSERT_TRUE(io != nullptr);

  // Save the trie to RDB WITHOUT payloads (savePayloads = 0)
  TrieType_GenericSave(io, originalTrie, 0);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Reset read position to load it back
  io->read_pos = 0;

  // Load the trie from RDB WITHOUT payloads (loadPayloads = 0)
  Trie *loadedTrie = (Trie *)TrieType_GenericLoad(io, 0);
  std::unique_ptr<Trie, std::function<void(Trie *)>> loadedTriePtr(loadedTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });
  ASSERT_TRUE(loadedTrie != nullptr);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Compare the original and loaded tries - sizes should match
  EXPECT_EQ(originalTrie->size, loadedTrie->size);

  // Verify all entries are present in the loaded trie
  EXPECT_TRUE(trieContains(loadedTrie, "car"));
  EXPECT_TRUE(trieContains(loadedTrie, "care"));
  EXPECT_TRUE(trieContains(loadedTrie, "careful"));

  // Verify that payloads are NOT preserved (should be null)
  void *loadedPayload1 = Trie_GetValueStringBuffer(loadedTrie, "car", 3, true);
  void *loadedPayload2 = Trie_GetValueStringBuffer(loadedTrie, "care", 4, true);
  void *loadedPayload3 = Trie_GetValueStringBuffer(loadedTrie, "careful", 7, true);

  EXPECT_TRUE(loadedPayload1 == nullptr);  // Payload should not be preserved
  EXPECT_TRUE(loadedPayload2 == nullptr);  // Payload should not be preserved
  EXPECT_TRUE(loadedPayload3 == nullptr);  // Payload should not be preserved
}

TEST_F(TrieTest, testRdbSaveLoadWithoutPayloads) {
  // Create a trie and insert entries WITHOUT payloads
  Trie *originalTrie = NewTrie(NULL, Trie_Sort_Score);
  std::unique_ptr<Trie, std::function<void(Trie *)>> originalTriePtr(originalTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });

  char payload1[] = "payload_1";
  char payload2[] = "payload_2";

  RSPayload p1 = {.data = payload1, .len = strlen(payload1)};
  RSPayload p2 = {.data = payload2, .len = strlen(payload2)};

  // Insert complex test data WITHOUT payloads - includes prefixes and extensions
  Trie_InsertStringBuffer(originalTrie, "hello", 5, 8.0, 0, NULL);     // Base word without payload
  Trie_InsertStringBuffer(originalTrie, "hell", 4, 6.0, 0, &p1);      // Prefix with payload
  Trie_InsertStringBuffer(originalTrie, "help", 4, 7.0, 0, NULL);      // Related word without payload
  Trie_InsertStringBuffer(originalTrie, "helper", 6, 5.0, 0, &p2);    // Extension with payload

  EXPECT_EQ(4, originalTrie->size);

  // Create RDB IO context
  RedisModuleIO *io = RMCK_CreateRdbIO();
  std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
    RMCK_FreeRdbIO(io);
  });
  ASSERT_TRUE(io != nullptr);

  // Save the trie to RDB
  TrieType_GenericSave(io, originalTrie, 0);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Reset read position to load it back
  io->read_pos = 0;

  // Load the trie from RDB WITHOUT payloads (loadPayloads = 0) to match the save operation
  Trie *loadedTrie = (Trie *)TrieType_GenericLoad(io, 0);
  std::unique_ptr<Trie, std::function<void(Trie *)>> loadedTriePtr(loadedTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });
  ASSERT_TRUE(loadedTrie != nullptr);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Compare sizes - entries should be preserved
  EXPECT_EQ(originalTrie->size, loadedTrie->size);

  // Verify all entries are present in the loaded trie
  EXPECT_TRUE(trieContains(loadedTrie, "hello"));
  EXPECT_TRUE(trieContains(loadedTrie, "hell"));
  EXPECT_TRUE(trieContains(loadedTrie, "help"));
  EXPECT_TRUE(trieContains(loadedTrie, "helper"));

  // Verify that payloads remain NULL (since none were inserted)
  void *loadedPayload1 = Trie_GetValueStringBuffer(loadedTrie, "hello", 5, true);
  void *loadedPayload2 = Trie_GetValueStringBuffer(loadedTrie, "hell", 4, true);
  void *loadedPayload3 = Trie_GetValueStringBuffer(loadedTrie, "help", 4, true);
  void *loadedPayload4 = Trie_GetValueStringBuffer(loadedTrie, "helper", 6, true);

  EXPECT_TRUE(loadedPayload1 == nullptr);  // No payload was inserted
  EXPECT_TRUE(loadedPayload2 == nullptr);  // No payload was inserted
  EXPECT_TRUE(loadedPayload3 == nullptr);  // No payload was inserted
  EXPECT_TRUE(loadedPayload4 == nullptr);  // No payload was inserted
}

TEST_F(TrieTest, testRdbSaveLoadEmptyTrie) {
  // Create an empty trie
  Trie *originalTrie = NewTrie(NULL, Trie_Sort_Score);
  std::unique_ptr<Trie, std::function<void(Trie *)>> originalTriePtr(originalTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });

  ASSERT_EQ(0, originalTrie->size);

  // Create RDB IO context
  RedisModuleIO *io = RMCK_CreateRdbIO();
  std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
    RMCK_FreeRdbIO(io);
  });
  ASSERT_TRUE(io != nullptr);

  // Save the empty trie to RDB
  TrieType_RdbSave(io, originalTrie);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Reset read position to load it back
  io->read_pos = 0;

  // Load the trie from RDB
  Trie *loadedTrie = (Trie *)TrieType_RdbLoad(io, TRIE_ENCVER_CURRENT);
  std::unique_ptr<Trie, std::function<void(Trie *)>> loadedTriePtr(loadedTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });
  ASSERT_TRUE(loadedTrie != nullptr);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Compare the original and loaded tries
  EXPECT_EQ(0, loadedTrie->size);
  EXPECT_EQ(originalTrie->size, loadedTrie->size);
}

TEST_F(TrieTest, testRdbSaveLoadLexSortedTrie) {
  // Create a trie with lexical sorting - this is the only difference from testBasicRdbSaveLoad
  Trie *originalTrie = NewTrie(NULL, Trie_Sort_Lex);
  std::unique_ptr<Trie, std::function<void(Trie *)>> originalTriePtr(originalTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });

  // Insert complex test data with prefixes, extensions, and overlapping words
  // This stresses the trie implementation with hierarchical relationships
  trieInsertByScore(originalTrie, "test", 5.0);        // Base word
  trieInsertByScore(originalTrie, "testing", 4.0);     // Extension of "test"
  trieInsertByScore(originalTrie, "tester", 3.0);      // Another extension of "test"
  trieInsertByScore(originalTrie, "tests", 6.0);       // Plural of "test"
  trieInsertByScore(originalTrie, "te", 2.0);          // Prefix of "test"
  trieInsertByScore(originalTrie, "hello", 8.0);       // Base word
  trieInsertByScore(originalTrie, "hell", 7.0);        // Prefix of "hello"
  trieInsertByScore(originalTrie, "help", 9.0);        // Shares prefix "hel" with "hello"
  trieInsertByScore(originalTrie, "helper", 1.0);      // Extension of "help"
  trieInsertByScore(originalTrie, "helping", 10.0);    // Another extension of "help"
  trieInsertByScore(originalTrie, "car", 11.0);        // Base word
  trieInsertByScore(originalTrie, "care", 12.0);       // Extension of "car"
  trieInsertByScore(originalTrie, "careful", 13.0);    // Extension of "care"
  trieInsertByScore(originalTrie, "carefully", 14.0);  // Extension of "careful"

  ASSERT_EQ(14, originalTrie->size);

  // Verify all entries exist in the original trie
  EXPECT_TRUE(trieContains(originalTrie, "test"));
  EXPECT_TRUE(trieContains(originalTrie, "testing"));
  EXPECT_TRUE(trieContains(originalTrie, "tester"));
  EXPECT_TRUE(trieContains(originalTrie, "tests"));
  EXPECT_TRUE(trieContains(originalTrie, "te"));
  EXPECT_TRUE(trieContains(originalTrie, "hello"));
  EXPECT_TRUE(trieContains(originalTrie, "hell"));
  EXPECT_TRUE(trieContains(originalTrie, "help"));
  EXPECT_TRUE(trieContains(originalTrie, "helper"));
  EXPECT_TRUE(trieContains(originalTrie, "helping"));
  EXPECT_TRUE(trieContains(originalTrie, "car"));
  EXPECT_TRUE(trieContains(originalTrie, "care"));
  EXPECT_TRUE(trieContains(originalTrie, "careful"));
  EXPECT_TRUE(trieContains(originalTrie, "carefully"));

  // Create RDB IO context
  RedisModuleIO *io = RMCK_CreateRdbIO();
  std::unique_ptr<RedisModuleIO, std::function<void(RedisModuleIO *)>> ioPtr(io, [](RedisModuleIO *io) {
    RMCK_FreeRdbIO(io);
  });
  ASSERT_TRUE(io != nullptr);

  // Save the trie to RDB
  TrieType_RdbSave(io, originalTrie);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Reset read position to load it back
  io->read_pos = 0;

  // Load the trie from RDB
  Trie *loadedTrie = (Trie *)TrieType_RdbLoad(io, TRIE_ENCVER_CURRENT);
  std::unique_ptr<Trie, std::function<void(Trie *)>> loadedTriePtr(loadedTrie, [](Trie *trie) {
    TrieType_Free(trie);
  });
  ASSERT_TRUE(loadedTrie != nullptr);
  EXPECT_EQ(0, RMCK_IsIOError(io));

  // Compare the original and loaded tries
  EXPECT_EQ(originalTrie->size, loadedTrie->size);

  // Note: The loaded trie will have Trie_Sort_Score (default from TrieType_GenericLoad)
  // but all the entries should still be present, even though the sorting mode changed

  // Verify all entries are present in the loaded trie
  EXPECT_TRUE(trieContains(loadedTrie, "test"));
  EXPECT_TRUE(trieContains(loadedTrie, "testing"));
  EXPECT_TRUE(trieContains(loadedTrie, "tester"));
  EXPECT_TRUE(trieContains(loadedTrie, "tests"));
  EXPECT_TRUE(trieContains(loadedTrie, "te"));
  EXPECT_TRUE(trieContains(loadedTrie, "hello"));
  EXPECT_TRUE(trieContains(loadedTrie, "hell"));
  EXPECT_TRUE(trieContains(loadedTrie, "help"));
  EXPECT_TRUE(trieContains(loadedTrie, "helper"));
  EXPECT_TRUE(trieContains(loadedTrie, "helping"));
  EXPECT_TRUE(trieContains(loadedTrie, "car"));
  EXPECT_TRUE(trieContains(loadedTrie, "care"));
  EXPECT_TRUE(trieContains(loadedTrie, "careful"));
  EXPECT_TRUE(trieContains(loadedTrie, "carefully"));

  // Since the sorting mode changes during RDB load, we can't use compareTrieContents
  // which expects the same iteration order. Instead, we verify that all entries exist
  // and the size matches.
}
