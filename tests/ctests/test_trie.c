/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "src/trie/trie.h"
#include "src/trie/trie_type.h"
#include "src/trie/levenshtein.h"
#include "src/trie/rune_util.h"
#include "libnu/libnu.h"
#include "rmutil/alloc.h"
#include "test_util.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/param.h>
#include <time.h>

int count = 0;

FilterCode stepFilter(unsigned char b, void *ctx, int *matched, void *matchCtx) {
  return F_CONTINUE;
}
// void *stepFilter(char b, void *ctx, void *stackCtx) {
//     SparseAutomaton *a = ctx;
//     dfaNode *dn = stackCtx;
//     unsigned char c = b;
//     if (dn->distance == -1) {
//         count++;
//         return NULL;
//     }
//     return dn->edges[c] ? dn->edges[c] : dn->fallback;
//     // // if (!SparseAutomaton_CanMatch(a,v)) {

//     // //     return NULL;
//     // // }
//     // sparseVector *nv = SparseAutomaton_Step(a, v, b);

//     // // we should continue
//     // if (SparseAutomaton_CanMatch(a, nv)) {
//     //     return nv;
//     // }
//     // sparseVector_free(nv);
//     // return NULL;
// }

int __trie_add(TrieNode **n, char *str, char *payloadStr, float sc, TrieAddOp op) {
  size_t rlen;
  rune *runes = strToRunes(str, &rlen);

  RSPayload payload = {.data = payloadStr, .len = payloadStr ? strlen(payloadStr) : 0};
  int rc = TrieNode_Add(n, runes, rlen, &payload, sc, op, NULL, 0);
  free(runes);
  return rc;
}

int testRuneUtil() {
  // convert from string to runes
  char *str = "yY";
  rune expectedRunes[3] = {121, 89, 3};
  size_t len;
  rune *runes = strToRunes(str, &len);
  ASSERT_EQUAL(len, 2);
  ASSERT_EQUAL(runes[0], expectedRunes[0]);
  ASSERT_EQUAL(runes[1], expectedRunes[1]);
  free(runes);
  // convert from runes back to string
  size_t backToStrLen;
  char *backToStr = runesToStr(expectedRunes, 2, &backToStrLen);
  ASSERT_STRING_EQ(str, backToStr);
  free(backToStr);

  // convert from string to runes
  size_t unicodeLen;
  rune expectedUnicodeRunes[5] = {216, 8719, 960, 229, 197};
  char *expectedUnicodeStr = "Ø∏πåÅ";
  rune *unicodeRunes = strToRunes(expectedUnicodeStr, &unicodeLen);
  ASSERT_EQUAL(unicodeLen, 5);
  for (int i = 0; i < 5; i++) {
    ASSERT_EQUAL(unicodeRunes[i], expectedUnicodeRunes[i]);
  }
  free(unicodeRunes);
  // convert from runes back to string
  size_t backUnicodeStrUtfLen;
  char *backUnicodeStr = runesToStr(expectedUnicodeRunes, 2, &backUnicodeStrUtfLen);
  for (int i = 0; i < 5; i++) {
    ASSERT_EQUAL(backUnicodeStr[i], expectedUnicodeStr[i]);
  }
  free(backUnicodeStr);

  size_t foldedLen;
  rune *foldedRunes = strToSingleCodepointFoldedRunes("yY", &foldedLen);
  ASSERT_EQUAL(foldedLen, 2);
  ASSERT_EQUAL(foldedRunes[0], 121);
  ASSERT_EQUAL(foldedRunes[1], 121);
  free(foldedRunes);

  // TESTING ∏ and Å because ∏ doesn't have a lowercase form, but Å does
  size_t foldedUnicodeLen;
  rune *foldedUnicodeRunes = strToSingleCodepointFoldedRunes("Ø∏πåÅ", &foldedUnicodeLen);
  ASSERT_EQUAL(runeFold(foldedUnicodeRunes[1]), foldedUnicodeRunes[1]);
  ASSERT_EQUAL(foldedUnicodeLen, 5);
  ASSERT_EQUAL(foldedUnicodeRunes[0], 248);
  ASSERT_EQUAL(foldedUnicodeRunes[1], 8719);
  ASSERT_EQUAL(foldedUnicodeRunes[2], 960);
  ASSERT_EQUAL(foldedUnicodeRunes[3], 229);
  ASSERT_EQUAL(foldedUnicodeRunes[4], 229);
  ASSERT_EQUAL(runeFold(foldedUnicodeRunes[4]), foldedUnicodeRunes[3]);
  free(foldedUnicodeRunes);

  return 0;
}

int testPayload() {
  rune *rootRunes = strToRunes("", NULL);
  TrieNode *root = __newTrieNode(rootRunes, 0, 0, NULL, 0, 0, 1, 0, Trie_Sort_Score, 0);
  ASSERT(root != NULL)
  free(rootRunes);

  char expectedRunes[3] = {'y', 'Y', '\0'};
  int rc = __trie_add(&root, "hello", "yY", 1, ADD_REPLACE);
  ASSERT_EQUAL(1, rc);

  size_t rlen;
  rune *runes = strToRunes("hel", &rlen);
  DFAFilter *fc = NewDFAFilter(runes, rlen, 1, 1);
  TrieIterator *it = TrieNode_Iterate(root, FoldingFilterFunc, StackPop, fc);
  rune *s;
  t_len len;
  float score;
  RSPayload payload = {.data = NULL, .len = 0};
  int matches = 0;
  int dist = 0;

  while (TrieIterator_Next(it, &s, &len, &payload, &score, NULL, &dist)) {
    ASSERT(score == 1);
    ASSERT(len > 0);
    ASSERT(payload.len == 2);
    ASSERT_EQUAL(payload.data[0], expectedRunes[0]);
    ASSERT_EQUAL(payload.data[1], expectedRunes[1]);
    matches++;
  }
  ASSERT(matches > 0);

  TrieIterator_Free(it);
  free(runes);

  TrieNode_Free(root, NULL);
  return 0;
}

int testTrie() {
  rune *rootRunes = strToRunes("", NULL);
  TrieNode *root = __newTrieNode(rootRunes, 0, 0, NULL, 0, 0, 1, 0, Trie_Sort_Score, 0);
  ASSERT(root != NULL)
  free(rootRunes);

  int rc = __trie_add(&root, "hello", NULL, 1, ADD_REPLACE);
  ASSERT_EQUAL(1, rc);
  rc = __trie_add(&root, "hello", NULL, 1, ADD_REPLACE);
  ASSERT_EQUAL(0, rc);  // the second insert of the same term should result in 0
  rc = __trie_add(&root, "help", NULL, 2, ADD_REPLACE);
  ASSERT_EQUAL(1, rc);

  __trie_add(&root, "helter skelter", NULL, 3, ADD_REPLACE);
  size_t rlen;
  rune *runes = strToRunes("helter skelter", &rlen);
  float sc = TrieNode_Find(root, runes, rlen);
  ASSERT(sc == 3);

  __trie_add(&root, "heltar skelter", NULL, 4, ADD_REPLACE);
  __trie_add(&root, "helter shelter", NULL, 5, ADD_REPLACE);

  // replace the score
  __trie_add(&root, "helter skelter", NULL, 6, ADD_REPLACE);

  sc = TrieNode_Find(root, runes, rlen);
  ASSERT(sc == 6);

  /// add with increment
  __trie_add(&root, "helter skelter", NULL, 6, ADD_INCR);
  sc = TrieNode_Find(root, runes, rlen);
  ASSERT(sc == 12);

  TrieNode_Free(root, NULL);
  free(runes);

  return 0;
}

int testUnicode() {

  char *str = "\xc4\x8c\xc4\x87";

  rune *rn = strToRunes("", NULL);
  TrieNode *root = __newTrieNode(rn, 0, 0, NULL, 0, 0, 1, 0, Trie_Sort_Score, 0);
  free(rn);
  ASSERT(root != NULL)

  int rc = __trie_add(&root, str, NULL, 1, ADD_REPLACE);
  ASSERT_EQUAL(1, rc);
  rc = __trie_add(&root, str, NULL, 1, ADD_REPLACE);
  ASSERT_EQUAL(0, rc);
  size_t rlen;
  rune *runes = strToRunes(str, &rlen);
  float sc = TrieNode_Find(root, runes, rlen);
  free(runes);
  ASSERT(sc == 1);
  TrieNode_Free(root, NULL);
  return 0;
}

int testDFAFilter() {
  FILE *fp = fopen("./titles.csv", "r");
  assert(fp != NULL);

  char *line = NULL;

  size_t len = 0;
  ssize_t read;
  size_t rlen;
  rune *runes = strToRunes("root", &rlen);
  TrieNode *root = __newTrieNode(runes, 0, rlen, NULL, 0, 0, 0, 0, Trie_Sort_Score, 0);
  ASSERT(root != NULL)
  free(runes);
  int i = 0;
  while ((read = getline(&line, &len, fp)) != -1) {
    char *sep = strchr(line, ',');
    if (!sep) continue;

    *sep = 0;
    double score = atof(sep + 1) + 1;
    sep--;
    while (*sep == ' ') {
      *sep-- = 0;
    }

    runes = strToRunes(line, &rlen);
    int rc = TrieNode_Add(&root, runes, rlen, NULL, (float)score, ADD_REPLACE, NULL, 0);
    ASSERT(rc == 1);
    free(runes);

    i++;
  }

  fclose(fp);

  if (line) free(line);

  printf("loaded %d entries\n", i);

  char *terms[] = {"DostOEvsky",
                   "dostoevski",
                   "cbs",
                   "cbxs",
                   "gangsta",
                   "geNGsta",
                   "jezebel",
                   "hezebel",
                   "\xd7\xa9\xd7\x9c\xd7\x95\xd7\x9d",
                   "\xd7\xa9\xd7\x97\xd7\x95\xd7\x9d",
                   NULL};
  struct timespec start_time;
  clock_gettime(CLOCK_REALTIME, &start_time);

  for (i = 0; terms[i] != NULL; i++) {
    runes = strToSingleCodepointFoldedRunes(terms[i], &rlen);
    DFAFilter *fc = NewDFAFilter(runes, rlen, 2, 0);

    TrieIterator *it = TrieNode_Iterate(root, FoldingFilterFunc, StackPop, fc);
    rune *s;
    t_len len;
    float score;
    int matches = 0;
    int dist = 0;

    clock_gettime(CLOCK_REALTIME, &start_time);
    while (TrieIterator_Next(it, &s, &len, NULL, &score, NULL, &dist)) {
      ASSERT(score > 0);
      ASSERT(dist <= 2 && dist >= 0)
      ASSERT(len > 0);

      // size_t ulen;
      // char *str = runesToStr(s, len, &ulen);
      //   printf("Found %s -> %.*s -> %f, dist %d\n", terms[i], len, str, score,
      //           dist);
      matches++;
    }
    ASSERT(matches > 0);

    TrieIterator_Free(it);
    free(runes);
  }

  char *prefixes[] = {"dos", "cb", "gang", "jez", "של", "שח", NULL};
  for (i = 0; prefixes[i] != NULL; i++) {
    // printf("prefix %d: %s\n", i, prefixes[i]);
    runes = strToRunes(prefixes[i], &rlen);

    DFAFilter *fc = NewDFAFilter(runes, rlen, 1, 1);

    TrieIterator *it = TrieNode_Iterate(root, FoldingFilterFunc, StackPop, fc);
    rune *s;
    t_len len;
    float score;
    int matches = 0;
    int dist = 0;

    while (TrieIterator_Next(it, &s, &len, NULL, &score, NULL, &dist)) {
      ASSERT(score > 0);
      ASSERT(dist <= 1 && dist >= 0)
      ASSERT(len > 0);
      //   printf("Found %s -> %.*s -> %f, dist %d\n", prefixes[i], len, s,
      //   score,
      //          dist);
      matches++;
    }
    ASSERT(matches > 0);

    TrieIterator_Free(it);
    free(runes);
  }

  TrieNode_Free(root, NULL);

  return 0;
}

int testNumDocs() {
  Trie *t = NewTrie(NULL, Trie_Sort_Score);
  ASSERT(t != NULL);
  TrieNode *node;

  // Allocate runes upfront
  size_t helpLen, helpingLen, helperLen, aLen, abLen, abcLen;
  rune *helpRunes = strToRunes("help", &helpLen);
  rune *helpingRunes = strToRunes("helping", &helpingLen);
  rune *helperRunes = strToRunes("helper", &helperLen);
  rune *aRunes = strToRunes("A", &aLen);
  rune *abRunes = strToRunes("AB", &abLen);
  rune *abcRunes = strToRunes("ABC", &abcLen);

  // Insert "help"
  int rc = Trie_InsertStringBuffer(t, "help", 4, 1.0, 0, NULL, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, helpRunes, helpLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  // Insert "helping" - "help" is a prefix of "helping"
  rc = Trie_InsertStringBuffer(t, "helping", 7, 1.0, 0, NULL, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, helpingRunes, helpingLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  // Insert "helper" - shares "help" prefix
  rc = Trie_InsertStringBuffer(t, "helper", 6, 1.0, 0, NULL, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, helperRunes, helperLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  // Insert chain: A -> AB -> ABC (each is prefix of the next)
  rc = Trie_InsertStringBuffer(t, "A", 1, 1.0, 0, NULL, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, aRunes, aLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  rc = Trie_InsertStringBuffer(t, "AB", 2, 1.0, 0, NULL, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, abRunes, abLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  rc = Trie_InsertStringBuffer(t, "ABC", 3, 1.0, 0, NULL, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, abcRunes, abcLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  // Increment numDocs for "help" multiple times
  rc = Trie_InsertStringBuffer(t, "help", 4, 1.0, 0, NULL, 1);
  ASSERT_EQUAL(0, rc);
  rc = Trie_InsertStringBuffer(t, "help", 4, 1.0, 0, NULL, 1);
  ASSERT_EQUAL(0, rc);
  node = TrieNode_Get(t->root, helpRunes, helpLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(3, node->numDocs);

  // Increment numDocs for "AB" (middle of chain)
  rc = Trie_InsertStringBuffer(t, "AB", 2, 1.0, 0, NULL, 1);
  ASSERT_EQUAL(0, rc);
  node = TrieNode_Get(t->root, abRunes, abLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(2, node->numDocs);

  // Final verification: check all values
  node = TrieNode_Get(t->root, helpRunes, helpLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(3, node->numDocs);

  node = TrieNode_Get(t->root, helpingRunes, helpingLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  node = TrieNode_Get(t->root, helperRunes, helperLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  node = TrieNode_Get(t->root, aRunes, aLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  node = TrieNode_Get(t->root, abRunes, abLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(2, node->numDocs);

  node = TrieNode_Get(t->root, abcRunes, abcLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  // Verify numDocs via iterator
  TrieIterator *it = TrieNode_Iterate(t->root, NULL, NULL, NULL);
  rune *s;
  t_len iterLen;
  float score;
  size_t numDocs;
  RSPayload payload = {.data = NULL, .len = 0};
  int count = 0;
  while (TrieIterator_Next(it, &s, &iterLen, &payload, &score, &numDocs, NULL)) {
    count++;
    // Convert runes to string for comparison
    size_t slen;
    char *term = runesToStr(s, iterLen, &slen);
    if (strcmp(term, "help") == 0) {
      ASSERT_EQUAL(3, numDocs);
    } else if (strcmp(term, "helping") == 0) {
      ASSERT_EQUAL(1, numDocs);
    } else if (strcmp(term, "helper") == 0) {
      ASSERT_EQUAL(1, numDocs);
    } else if (strcmp(term, "A") == 0) {
      ASSERT_EQUAL(1, numDocs);
    } else if (strcmp(term, "AB") == 0) {
      ASSERT_EQUAL(2, numDocs);
    } else if (strcmp(term, "ABC") == 0) {
      ASSERT_EQUAL(1, numDocs);
    }
    free(term);
  }
  ASSERT_EQUAL(6, count);  // Should have iterated over all 6 terms
  TrieIterator_Free(it);

  // Cleanup
  free(helpRunes);
  free(helpingRunes);
  free(helperRunes);
  free(aRunes);
  free(abRunes);
  free(abcRunes);
  TrieType_Free(t);
  return 0;
}

int testDecrementNumDocs() {
  Trie *t = NewTrie(NULL, Trie_Sort_Score);
  ASSERT(t != NULL);
  TrieNode *node;

  // Allocate runes for lookups
  size_t helloLen, worldLen, cafeLen;
  rune *helloRunes = strToRunes("hello", &helloLen);
  rune *worldRunes = strToRunes("world", &worldLen);

  // Test 1: Decrement non-existent term
  TrieDecrResult rc = Trie_DecrementNumDocs(t, "nonexistent", 11, 1);
  ASSERT_EQUAL(TRIE_DECR_NOT_FOUND, rc);

  // Test 2: Insert term and decrement partially
  int insertRc = Trie_InsertStringBuffer(t, "hello", 5, 1.0, 0, NULL, 10);
  ASSERT_EQUAL(1, insertRc);
  node = TrieNode_Get(t->root, helloRunes, helloLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(10, node->numDocs);

  rc = Trie_DecrementNumDocs(t, "hello", 5, 3);
  ASSERT_EQUAL(TRIE_DECR_UPDATED, rc);
  node = TrieNode_Get(t->root, helloRunes, helloLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(7, node->numDocs);

  // Test 3: Decrement to exactly zero (should delete)
  rc = Trie_DecrementNumDocs(t, "hello", 5, 7);
  ASSERT_EQUAL(TRIE_DECR_DELETED, rc);
  node = TrieNode_Get(t->root, helloRunes, helloLen, true, NULL);
  ASSERT(node == NULL);  // Node should be deleted
  ASSERT_EQUAL(0, t->size);  // Trie size should be 0

  // Test 4: Decrement with delta > numDocs (should clamp and delete)
  insertRc = Trie_InsertStringBuffer(t, "world", 5, 1.0, 0, NULL, 5);
  ASSERT_EQUAL(1, insertRc);
  node = TrieNode_Get(t->root, worldRunes, worldLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(5, node->numDocs);

  rc = Trie_DecrementNumDocs(t, "world", 5, 100);  // delta > numDocs
  ASSERT_EQUAL(TRIE_DECR_DELETED, rc);
  node = TrieNode_Get(t->root, worldRunes, worldLen, true, NULL);
  ASSERT(node == NULL);  // Node should be deleted

  // Test 5: Unicode string - "café" (UTF-8: 0x63 0x61 0x66 0xC3 0xA9)
  const char *cafe = "caf\xc3\xa9";  // café in UTF-8
  size_t cafeUtf8Len = 5;  // 5 bytes in UTF-8
  rune *cafeRunes = strToRunes(cafe, &cafeLen);

  insertRc = Trie_InsertStringBuffer(t, cafe, cafeUtf8Len, 1.0, 0, NULL, 8);
  ASSERT_EQUAL(1, insertRc);
  node = TrieNode_Get(t->root, cafeRunes, cafeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(8, node->numDocs);

  rc = Trie_DecrementNumDocs(t, cafe, cafeUtf8Len, 3);
  ASSERT_EQUAL(TRIE_DECR_UPDATED, rc);
  node = TrieNode_Get(t->root, cafeRunes, cafeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(5, node->numDocs);

  // Test 6: Multiple terms with shared prefix
  insertRc = Trie_InsertStringBuffer(t, "help", 4, 1.0, 0, NULL, 10);
  ASSERT_EQUAL(1, insertRc);
  insertRc = Trie_InsertStringBuffer(t, "helper", 6, 1.0, 0, NULL, 5);
  ASSERT_EQUAL(1, insertRc);
  insertRc = Trie_InsertStringBuffer(t, "helping", 7, 1.0, 0, NULL, 3);
  ASSERT_EQUAL(1, insertRc);

  // Decrement "help" - should not affect "helper" or "helping"
  size_t helpLen, helperLen, helpingLen;
  rune *helpRunes = strToRunes("help", &helpLen);
  rune *helperRunes = strToRunes("helper", &helperLen);
  rune *helpingRunes = strToRunes("helping", &helpingLen);

  rc = Trie_DecrementNumDocs(t, "help", 4, 5);
  ASSERT_EQUAL(TRIE_DECR_UPDATED, rc);

  node = TrieNode_Get(t->root, helpRunes, helpLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(5, node->numDocs);

  node = TrieNode_Get(t->root, helperRunes, helperLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(5, node->numDocs);  // Unchanged

  node = TrieNode_Get(t->root, helpingRunes, helpingLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(3, node->numDocs);  // Unchanged

  // Cleanup
  free(helloRunes);
  free(worldRunes);
  free(cafeRunes);
  free(helpRunes);
  free(helperRunes);
  free(helpingRunes);
  TrieType_Free(t);
  return 0;
}

/**
 * Test a complex trie scenario simulating GC-like batch decrements.
 *
 * Scenario: We have an index with documents containing various terms.
 * A compaction/GC run determines that certain documents were deleted,
 * and we need to decrement the term counts accordingly.
 */
int testDecrementNumDocsComplex() {
  Trie *t = NewTrie(NULL, Trie_Sort_Score);
  ASSERT(t != NULL);
  TrieNode *node;

  // Build a trie representing terms from a search index:
  // Terms and their initial document counts (how many docs contain each term)
  //
  // Structure (with shared prefixes, including Unicode):
  //   "apple"       -> 100 docs
  //   "application" -> 50 docs
  //   "apply"       -> 30 docs
  //   "banana"      -> 80 docs
  //   "band"        -> 25 docs
  //   "bandana"     -> 10 docs
  //   "cat"         -> 200 docs
  //   "car"         -> 150 docs
  //   "card"        -> 75 docs
  //   "redis"       -> 500 docs
  //   "redisearch"  -> 300 docs
  //   "red"         -> 1000 docs
  //
  // Unicode terms (UTF-8):
  //   "café"        -> 120 docs  (French: coffee shop)
  //   "caféine"     -> 45 docs   (French: caffeine, shares prefix with café)
  //   "naïve"       -> 60 docs   (has ï = 0xC3 0xAF)
  //   "日本"         -> 200 docs  (Japanese: Japan, 2 chars, 6 bytes)
  //   "日本語"       -> 150 docs  (Japanese: Japanese language, shares prefix)
  //   "東京"         -> 180 docs  (Japanese: Tokyo)
  //   "München"     -> 90 docs   (German: Munich, has ü)
  //   "Zürich"      -> 70 docs   (German: Zurich, has ü)
  //
  typedef struct {
    const char *term;
    size_t len;
    size_t numDocs;
  } TermEntry;

  // UTF-8 encoded strings
  const char *cafe = "caf\xc3\xa9";           // café (5 bytes)
  const char *cafeine = "caf\xc3\xa9ine";     // caféine (8 bytes)
  const char *naive = "na\xc3\xafve";         // naïve (6 bytes)
  const char *nihon = "\xe6\x97\xa5\xe6\x9c\xac";           // 日本 (6 bytes)
  const char *nihongo = "\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e";  // 日本語 (9 bytes)
  const char *tokyo = "\xe6\x9d\xb1\xe4\xba\xac";           // 東京 (6 bytes)
  const char *munchen = "M\xc3\xbcnchen";     // München (8 bytes)
  const char *zurich = "Z\xc3\xbcrich";       // Zürich (7 bytes)

  TermEntry initialTerms[] = {
    // ASCII terms
    {"apple", 5, 100},
    {"application", 11, 50},
    {"apply", 5, 30},
    {"banana", 6, 80},
    {"band", 4, 25},
    {"bandana", 7, 10},
    {"cat", 3, 200},
    {"car", 3, 150},
    {"card", 4, 75},
    {"redis", 5, 500},
    {"redisearch", 10, 300},
    {"red", 3, 1000},
    // Unicode terms
    {cafe, 5, 120},
    {cafeine, 8, 45},
    {naive, 6, 60},
    {nihon, 6, 200},
    {nihongo, 9, 150},
    {tokyo, 6, 180},
    {munchen, 8, 90},
    {zurich, 7, 70},
  };
  size_t numTerms = sizeof(initialTerms) / sizeof(initialTerms[0]);

  // Insert all terms
  for (size_t i = 0; i < numTerms; i++) {
    int rc = Trie_InsertStringBuffer(t, initialTerms[i].term, initialTerms[i].len,
                                     1.0, 0, NULL, initialTerms[i].numDocs);
    ASSERT_EQUAL(1, rc);
  }
  ASSERT_EQUAL(numTerms, t->size);

  // Verify initial state
  size_t runeLen;
  rune *runes;

  runes = strToRunes("redis", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(500, node->numDocs);
  free(runes);

  runes = strToRunes("banana", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(80, node->numDocs);
  free(runes);

  // ========================================
  // Documents 1-10 were deleted.
  // These documents contained the following terms:
  //   "apple"      appeared in 5 of them   -> decrement by 5
  //   "banana"     appeared in 3 of them   -> decrement by 3
  //   "redis"      appeared in 10 of them  -> decrement by 10
  //   "bandana"    appeared in 10 of them  -> decrement by 10 (will reach 0, delete)
  //   "cat"        appeared in 0 of them   -> no change
  // ========================================

  typedef struct {
    const char *term;
    size_t len;
    size_t delta;
    TrieDecrResult expectedResult;
    size_t expectedNumDocsAfter;  // 0 means node should be deleted
  } DecrementOp;

  DecrementOp decrements[] = {
    // ASCII terms
    {"apple", 5, 5, TRIE_DECR_UPDATED, 95},
    {"banana", 6, 3, TRIE_DECR_UPDATED, 77},
    {"redis", 5, 10, TRIE_DECR_UPDATED, 490},
    {"bandana", 7, 10, TRIE_DECR_DELETED, 0},  // exactly reaches 0
    // Unicode terms
    {cafe, 5, 20, TRIE_DECR_UPDATED, 100},       // café: 120 -> 100
    {cafeine, 8, 45, TRIE_DECR_DELETED, 0},     // caféine: 45 -> 0 (delete)
    {naive, 6, 10, TRIE_DECR_UPDATED, 50},      // naïve: 60 -> 50
    {nihon, 6, 50, TRIE_DECR_UPDATED, 150},     // 日本: 200 -> 150
    {tokyo, 6, 180, TRIE_DECR_DELETED, 0},      // 東京: 180 -> 0 (delete)
    {munchen, 8, 30, TRIE_DECR_UPDATED, 60},    // München: 90 -> 60
  };
  size_t numDecrements = sizeof(decrements) / sizeof(decrements[0]);

  // Apply decrements and verify results
  for (size_t i = 0; i < numDecrements; i++) {
    TrieDecrResult rc = Trie_DecrementNumDocs(t, decrements[i].term,
                                              decrements[i].len,
                                              decrements[i].delta);
    ASSERT_EQUAL(decrements[i].expectedResult, rc);

    runes = strToRunes(decrements[i].term, &runeLen);
    node = TrieNode_Get(t->root, runes, runeLen, true, NULL);

    if (decrements[i].expectedNumDocsAfter == 0) {
      ASSERT(node == NULL);  // Node should be deleted
    } else {
      ASSERT(node != NULL);
      ASSERT_EQUAL(decrements[i].expectedNumDocsAfter, node->numDocs);
    }
    free(runes);
  }

  // Verify that "bandana" was deleted but "band" and "banana" still exist
  runes = strToRunes("bandana", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node == NULL);
  free(runes);

  runes = strToRunes("band", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(25, node->numDocs);  // Unchanged
  free(runes);

  runes = strToRunes("banana", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(77, node->numDocs);  // Was decremented
  free(runes);

  // Verify Unicode terms: caféine and 東京 were deleted, café still exists
  runes = strToRunes(cafeine, &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node == NULL);  // caféine was deleted
  free(runes);

  runes = strToRunes(tokyo, &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node == NULL);  // 東京 was deleted
  free(runes);

  runes = strToRunes(cafe, &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(100, node->numDocs);  // café: was decremented from 120 to 100
  free(runes);

  // Verify 日本語 is unchanged (shares prefix with 日本 which was decremented)
  runes = strToRunes(nihongo, &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(150, node->numDocs);  // 日本語 unchanged
  free(runes);

  // Verify 日本 was decremented
  runes = strToRunes(nihon, &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(150, node->numDocs);  // 日本: 200 -> 150
  free(runes);

  // Verify Zürich is unchanged (different prefix from München which was decremented)
  runes = strToRunes(zurich, &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(70, node->numDocs);  // Zürich unchanged
  free(runes);

  // Verify terms that were not touched remain unchanged
  runes = strToRunes("cat", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(200, node->numDocs);
  free(runes);

  runes = strToRunes("redisearch", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(300, node->numDocs);
  free(runes);

  runes = strToRunes("red", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1000, node->numDocs);
  free(runes);

  // Trie size should be numTerms - 3 (bandana, caféine, 東京 were deleted)
  ASSERT_EQUAL(numTerms - 3, t->size);

  // ========================================
  // Simulate another GC pass: more aggressive cleanup
  // Delete all terms starting with "app" by decrementing to 0
  // ========================================

  // Decrement "apple" by remaining 95 -> delete
  TrieDecrResult rc = Trie_DecrementNumDocs(t, "apple", 5, 95);
  ASSERT_EQUAL(TRIE_DECR_DELETED, rc);

  // Decrement "application" by remaining 50 -> delete
  rc = Trie_DecrementNumDocs(t, "application", 11, 50);
  ASSERT_EQUAL(TRIE_DECR_DELETED, rc);

  // Decrement "apply" by remaining 30 -> delete
  rc = Trie_DecrementNumDocs(t, "apply", 5, 30);
  ASSERT_EQUAL(TRIE_DECR_DELETED, rc);

  // Verify all "app*" terms are gone
  runes = strToRunes("apple", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node == NULL);
  free(runes);

  runes = strToRunes("application", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node == NULL);
  free(runes);

  runes = strToRunes("apply", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node == NULL);
  free(runes);

  // Trie size should now be numTerms - 6 (3 from first pass + 3 app* terms)
  ASSERT_EQUAL(numTerms - 6, t->size);

  // ========================================
  // Test decrementing a non-existent term (already deleted)
  // ========================================
  rc = Trie_DecrementNumDocs(t, "bandana", 7, 1);
  ASSERT_EQUAL(TRIE_DECR_NOT_FOUND, rc);

  rc = Trie_DecrementNumDocs(t, "apple", 5, 1);
  ASSERT_EQUAL(TRIE_DECR_NOT_FOUND, rc);

  // ========================================
  // Test underflow protection in batch scenario
  // ========================================
  // Decrement "redis" by more than it has
  runes = strToRunes("redis", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  size_t currentRedisCount = node->numDocs;  // Should be 490
  ASSERT_EQUAL(490, currentRedisCount);
  free(runes);

  // Try to decrement by 1000 (more than 490)
  rc = Trie_DecrementNumDocs(t, "redis", 5, 1000);
  ASSERT_EQUAL(TRIE_DECR_DELETED, rc);  // Should clamp to 0 and delete

  runes = strToRunes("redis", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node == NULL);  // Should be deleted
  free(runes);

  // But "redisearch" and "red" should still exist
  runes = strToRunes("redisearch", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(300, node->numDocs);
  free(runes);

  runes = strToRunes("red", &runeLen);
  node = TrieNode_Get(t->root, runes, runeLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1000, node->numDocs);
  free(runes);

  TrieType_Free(t);
  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testRuneUtil);
  TESTFUNC(testDFAFilter);
  TESTFUNC(testTrie);
  TESTFUNC(testPayload);
  TESTFUNC(testUnicode);
  TESTFUNC(testNumDocs);
  TESTFUNC(testDecrementNumDocs);
  TESTFUNC(testDecrementNumDocsComplex);
});
