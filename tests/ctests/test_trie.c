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
  int rc = TrieNode_Add(n, runes, rlen, &payload, sc, op, NULL, 0, 0);
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

  while (TrieIterator_Next(it, &s, &len, &payload, &score, &dist)) {
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
    int rc = TrieNode_Add(&root, runes, rlen, NULL, (float)score, ADD_REPLACE, NULL, 0, 0);
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
    while (TrieIterator_Next(it, &s, &len, NULL, &score, &dist)) {
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

    while (TrieIterator_Next(it, &s, &len, NULL, &score, &dist)) {
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

int testNumDocsWithAddition() {
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
  int rc = Trie_InsertStringBuffer(t, "help", 4, 1.0, 0, NULL, 0, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, helpRunes, helpLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  // Insert "helping" - "help" is a prefix of "helping"
  rc = Trie_InsertStringBuffer(t, "helping", 7, 2.0, 0, NULL, 0, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, helpingRunes, helpingLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  // Insert "helper" - shares "help" prefix
  rc = Trie_InsertStringBuffer(t, "helper", 6, 3.0, 0, NULL, 0, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, helperRunes, helperLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  // Insert chain: A -> AB -> ABC (each is prefix of the next)
  rc = Trie_InsertStringBuffer(t, "A", 1, 4.0, 0, NULL, 0, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, aRunes, aLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  rc = Trie_InsertStringBuffer(t, "AB", 2, 5.0, 0, NULL, 0, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, abRunes, abLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  rc = Trie_InsertStringBuffer(t, "ABC", 3, 6.0, 0, NULL, 0, 1);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, abcRunes, abcLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(1, node->numDocs);

  // Increment numDocs for "help" multiple times
  rc = Trie_InsertStringBuffer(t, "help", 4, 1.0, 0, NULL, 0, 1);
  ASSERT_EQUAL(0, rc);
  rc = Trie_InsertStringBuffer(t, "help", 4, 1.0, 0, NULL, 0, 1);
  ASSERT_EQUAL(0, rc);
  node = TrieNode_Get(t->root, helpRunes, helpLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(3, node->numDocs);

  // Increment numDocs for "AB" (middle of chain)
  rc = Trie_InsertStringBuffer(t, "AB", 2, 5.0, 0, NULL, 0, 1);
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

int testNumDocsWithSet() {
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

  // Insert "help" with numDocsToSet (simulating RDB load)
  int rc = Trie_InsertStringBuffer(t, "help", 4, 1.0, 0, NULL, 10, 0);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, helpRunes, helpLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(10, node->numDocs);

  // Insert "helping" - "help" is a prefix of "helping"
  rc = Trie_InsertStringBuffer(t, "helping", 7, 2.0, 0, NULL, 20, 0);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, helpingRunes, helpingLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(20, node->numDocs);

  // Insert "helper" - shares "help" prefix
  rc = Trie_InsertStringBuffer(t, "helper", 6, 3.0, 0, NULL, 30, 0);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, helperRunes, helperLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(30, node->numDocs);

  // Insert chain: A -> AB -> ABC (each is prefix of the next)
  rc = Trie_InsertStringBuffer(t, "A", 1, 4.0, 0, NULL, 100, 0);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, aRunes, aLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(100, node->numDocs);

  rc = Trie_InsertStringBuffer(t, "AB", 2, 5.0, 0, NULL, 200, 0);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, abRunes, abLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(200, node->numDocs);

  rc = Trie_InsertStringBuffer(t, "ABC", 3, 6.0, 0, NULL, 300, 0);
  ASSERT_EQUAL(1, rc);
  node = TrieNode_Get(t->root, abcRunes, abcLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(300, node->numDocs);

  // Override "AB" numDocs with a new value (middle of chain)
  rc = Trie_InsertStringBuffer(t, "AB", 2, 5.0, 0, NULL, 999, 0);
  ASSERT_EQUAL(0, rc);
  node = TrieNode_Get(t->root, abRunes, abLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(999, node->numDocs);

  // Final verification: check all values
  node = TrieNode_Get(t->root, helpRunes, helpLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(10, node->numDocs);

  node = TrieNode_Get(t->root, helpingRunes, helpingLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(20, node->numDocs);

  node = TrieNode_Get(t->root, helperRunes, helperLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(30, node->numDocs);

  node = TrieNode_Get(t->root, aRunes, aLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(100, node->numDocs);

  node = TrieNode_Get(t->root, abRunes, abLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(999, node->numDocs);

  node = TrieNode_Get(t->root, abcRunes, abcLen, true, NULL);
  ASSERT(node != NULL);
  ASSERT_EQUAL(300, node->numDocs);

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

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testRuneUtil);
  TESTFUNC(testDFAFilter);
  TESTFUNC(testTrie);
  TESTFUNC(testPayload);
  TESTFUNC(testUnicode);
  TESTFUNC(testNumDocsWithAddition);
  TESTFUNC(testNumDocsWithSet);
});
