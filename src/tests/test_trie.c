#include "../trie/trie.h"
#include "../trie/levenshtein.h"
#include "../trie/rune_util.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/param.h>
#include <time.h>
#include "test_util.h"
#include "../dep/libnu/libnu.h"

int count = 0;

FilterCode stepFilter(unsigned char b, void *ctx, int *matched,
                      void *matchCtx) {
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

int __trie_add(TrieNode **n, char *str, float sc, TrieAddOp op) {
  size_t rlen;
  rune *runes = __strToRunes(str, &rlen); 
  
   int rc = TrieNode_Add(n, runes, rlen, sc, op);
   free(runes);
   return rc;
}


int testTrie() {
  TrieNode *root = __newTrieNode(__strToRunes("", NULL), 0, 0, 0, 1, 0);
  ASSERT(root != NULL)

  int rc = __trie_add(&root, "hello", 1, ADD_REPLACE);
  ASSERT_EQUAL_INT(1, rc);
  rc = __trie_add(&root, "hello", 1, ADD_REPLACE);
  ASSERT_EQUAL_INT(0,
                   rc); // the second insert of the same term should result in 0
  rc = __trie_add(&root, "help", 2, ADD_REPLACE);
  ASSERT_EQUAL_INT(1, rc);

  __trie_add(&root, "helter skelter", 3, ADD_REPLACE);
  size_t rlen;
  rune *runes = __strToRunes("helter skelter", &rlen);
  float sc = TrieNode_Find(root, runes, rlen);
  ASSERT(sc == 3);

  __trie_add(&root, "heltar skelter", 4, ADD_REPLACE);
  __trie_add(&root, "helter shelter", 5, ADD_REPLACE);

  // replace the score
  __trie_add(&root, "helter skelter", 6, ADD_REPLACE);
  
  sc = TrieNode_Find(root, runes, rlen);
  ASSERT(sc == 6);

  /// add with increment
  __trie_add(&root, "helter skelter", 6, ADD_INCR);
  sc = TrieNode_Find(root, runes, rlen);
  ASSERT(sc == 12);


  rc = TrieNode_Delete(root,  runes, rlen);
  ASSERT(rc == 1);
  rc = TrieNode_Delete(root,  runes, rlen);
  ASSERT(rc == 0);
  sc = TrieNode_Find(root, runes, rlen);
  
  ASSERT(sc == 0);

  TrieNode_Free(root);
  free(runes);

  return 0;
}

int testUnicode() {

  char *str = "\xc4\x8c\xc4\x87";

  rune *rn = __strToRunes("", NULL);
  TrieNode *root = __newTrieNode(rn, 0, 0, 0, 1, 0);
  free(rn);
  ASSERT(root != NULL)

  int rc = __trie_add(&root, str, 1, ADD_REPLACE);
  ASSERT_EQUAL_INT(1, rc);
  rc = __trie_add(&root, str, 1, ADD_REPLACE);
  ASSERT_EQUAL_INT(0, rc);
  size_t rlen;
  rune *runes = __strToRunes(str, &rlen);
  float sc = TrieNode_Find(root, runes, rlen);
  free(runes);
  ASSERT(sc == 1);
  TrieNode_Free(root);
  return 0;
}

int testDFAFilter() {
  FILE *fp = fopen("./titles.csv", "r");
  assert(fp != NULL);

  char *line = NULL;

  size_t len = 0;
  ssize_t read;
  size_t rlen;
  rune *runes = __strToRunes("root", &rlen);
  TrieNode *root = __newTrieNode(runes, 0, rlen, 0, 0, 0);
  ASSERT(root != NULL)
  free(runes);
  int i = 0;
  while ((read = getline(&line, &len, fp)) != -1) {
    char *sep = strchr(line, ',');
    if (!sep)
      continue;

    *sep = 0;
    double score = atof(sep + 1) + 1;
    sep--;
    while (*sep == ' ') {
      *sep-- = 0;
    }

    runes = __strToRunes(line, &rlen);
    int rc = TrieNode_Add(&root, runes, rlen, (float)score, ADD_REPLACE);
    ASSERT(rc == 1);
    free(runes);

    i++;
  }

  fclose(fp);

  if (line)
    free(line);

  printf("loaded %d entries\n", i);

  char *terms[] = {"DostOEvsky", "dostoevski", "cbs",     "cbxs", "gangsta",
                   "geNGsta",    "jezebel",    "hezebel",  "\xd7\xa9\xd7\x9c\xd7\x95\xd7\x9d", "\xd7\xa9\xd7\x97\xd7\x95\xd7\x9d", NULL};
  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  unsigned long long totalns = 0;

  for (i = 0; terms[i] != NULL; i++) {
    runes = __strToFoldedRunes(terms[i], &rlen);
    DFAFilter fc = NewDFAFilter(runes, rlen, 2, 0);

    TrieIterator *it = TrieNode_Iterate(root, FilterFunc, StackPop, &fc);
    rune *s;
    t_len len;
    float score;
    int matches = 0;
    int dist = 0;

    clock_gettime(CLOCK_REALTIME, &start_time);
    while (TrieIterator_Next(it, &s, &len, &score, &dist)) {
      ASSERT(score > 0);
      ASSERT(dist <= 2 && dist >= 0)
      ASSERT(len > 0);

      // size_t ulen;
      // char *str = __runesToStr(s, len, &ulen);
      //   printf("Found %s -> %.*s -> %f, dist %d\n", terms[i], len, str, score,
      //           dist);
      matches++;
    }
    ASSERT(matches > 0);

    DFAFilter_Free(&fc);
    TrieIterator_Free(it);
    free(runes);
  }

  char *prefixes[] = {"dos", "cb", "gang", "jez", "של", "שח", NULL};
  for (i = 0; prefixes[i] != NULL; i++) {
    //printf("prefix %d: %s\n", i, prefixes[i]);
    runes = __strToRunes(prefixes[i], &rlen);
    DFAFilter fc = NewDFAFilter(runes, rlen, 1, 1);

    TrieIterator *it = TrieNode_Iterate(root, FilterFunc, StackPop, &fc);
    rune *s;
    t_len len;
    float score;
    int matches = 0;
    int dist = 0;

    while (TrieIterator_Next(it, &s, &len, &score, &dist)) {
      ASSERT(score > 0);
      ASSERT(dist <= 1 && dist >= 0)
      ASSERT(len > 0);
      //   printf("Found %s -> %.*s -> %f, dist %d\n", prefixes[i], len, s,
      //   score,
      //          dist);
      matches++;
    }
    ASSERT(matches > 0);

    DFAFilter_Free(&fc);
    TrieIterator_Free(it);
    free(runes);
  }

  TrieNode_Free(root);

  return 0;
}

int main(int argc, char **argv) {
  TESTFUNC(testDFAFilter);
  TESTFUNC(testTrie);
  TESTFUNC(testUnicode);
}
