#include "../trie/trie.h"
#include "../trie/levenshtein.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/param.h>
#include <time.h>
#include "test_util.h"

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

#define __trie_add(n, str, sc, op) TrieNode_Add(&n, str, strlen(str), sc, op);

int testTrie() {
  TrieNode *root = __newTrieNode("", 0, 0, 0, 1, 0);
  ASSERT(root != NULL)

  int rc = __trie_add(root, "hello", 1, ADD_REPLACE);
  ASSERT_EQUAL_INT(1, rc);
  rc = __trie_add(root, "hello", 1, ADD_REPLACE);
  ASSERT_EQUAL_INT(0,
                   rc); // the second insert of the same term should result in 0
  rc = __trie_add(root, "help", 2, ADD_REPLACE);
  ASSERT_EQUAL_INT(1, rc);

  __trie_add(root, "helter skelter", 3, ADD_REPLACE);
  float sc = TrieNode_Find(root, "helter skelter", 14);
  ASSERT(sc == 3);

  __trie_add(root, "heltar skelter", 4, ADD_REPLACE);
  __trie_add(root, "helter shelter", 5, ADD_REPLACE);

  // replace the score
  __trie_add(root, "helter skelter", 6, ADD_REPLACE);

  sc = TrieNode_Find(root, "helter skelter", 14);
  ASSERT(sc == 6);

  /// add with increment
  __trie_add(root, "helter skelter", 6, ADD_INCR);
  sc = TrieNode_Find(root, "helter skelter", 14);
  ASSERT(sc == 12);


  rc = TrieNode_Delete(root,  "helter skelter", 14);
  ASSERT(rc == 1);
  rc = TrieNode_Delete(root,  "helter skelter", 14);
  ASSERT(rc == 0);
  sc = TrieNode_Find(root, "helter skelter", 14);
  
  ASSERT(sc == 0);

  TrieNode_Free(root);

  return 0;
}

int testDFAFilter() {
  FILE *fp = fopen("./titles.csv", "r");
  assert(fp != NULL);

  char *line = NULL;

  size_t len = 0;
  ssize_t read;
  TrieNode *root = __newTrieNode("root", 0, 4, 0, 0, 0);
  ASSERT(root != NULL)
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

    int rc = TrieNode_Add(&root, line, strlen(line), (float)score, ADD_REPLACE);
    ASSERT(rc == 1);

    i++;
  }

  fclose(fp);

  if (line)
    free(line);

  printf("loaded %d entries\n", i);

  char *terms[] = {"dostoevsky", "dostoevski", "cbs",     "cbxs", "gangsta",
                   "gengsta",    "jezebel",    "hezebel", NULL};
  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  unsigned long long totalns = 0;

  for (i = 0; terms[i] != NULL; i++) {
    DFAFilter fc = NewDFAFilter(terms[i], strlen(terms[i]), 2, 0);

    TrieIterator *it = TrieNode_Iterate(root, FilterFunc, StackPop, &fc);
    char *s;
    t_len len;
    float score;
    int matches = 0;
    int dist = 0;

    clock_gettime(CLOCK_REALTIME, &start_time);

    while (TrieIterator_Next(it, &s, &len, &score, &dist)) {
      ASSERT(score > 0);
      ASSERT(dist <= 2 && dist >= 0)
      ASSERT(len > 0);
      //   printf("Found %s -> %.*s -> %f, dist %d\n", terms[i], len, s, score,
      //          dist);
      matches++;
    }
    ASSERT(matches > 0);

    DFAFilter_Free(&fc);
    TrieIterator_Free(it);
  }

  char *prefixes[] = {"dos", "cb", "gang", "jez", NULL};
  for (i = 0; prefixes[i] != NULL; i++) {
    DFAFilter fc = NewDFAFilter(prefixes[i], strlen(prefixes[i]), 1, 1);

    TrieIterator *it = TrieNode_Iterate(root, FilterFunc, StackPop, &fc);
    char *s;
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
  }

  TrieNode_Free(root);

  return 0;
}

int main(int argc, char **argv) {
  //TESTFUNC(testDFAFilter);
  TESTFUNC(testTrie);
}
