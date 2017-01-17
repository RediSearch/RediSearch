#include "../util/range_tree.h"
#include <stdio.h>
#include "test_util.h"
#include "../types.h"
double qselect(double *v, int len, int k) {
#define SWAP(a, b) \
  {                \
    tmp = v[a];    \
    v[a] = v[b];   \
    v[b] = tmp;    \
  }
  int i, st, tmp;

  for (st = i = 0; i < len - 1; i++) {
    if (v[i] > v[len - 1]) continue;
    SWAP(i, st);
    st++;
  }

  SWAP(len - 1, st);

  return k == st ? v[st] : st > k ? qselect(v, st, k) : qselect(v + st, len - st, k - st);
}

typedef struct {
  t_docId docId;
  double value;
} ScoredListEntry;

typedef struct {
  size_t size;
  size_t cap;
  int card;
  ScoredListEntry *entries;
} ScoredList;

ScoredList *NewScoredList(size_t cap) {
  // printf("created new docNode %d, son of %d\n", docId, parent ? parent->docId : -1);
  ScoredList *ret = malloc(sizeof(ScoredList));
  ret->cap = cap ? cap : 1;
  ret->entries = calloc(cap, sizeof(ScoredListEntry));
  ret->size = 0;
  ret->card = 0;

  return ret;
}

int ScoredList_Add(void *ctx, void *entry, double value) {
  ScoredListEntry *ent = entry;
  ScoredList *n = ctx;

  if (n->size >= n->cap) {
    n->cap *= 2;
    n->entries = realloc(n->entries, n->cap * sizeof(ScoredListEntry));
  }

  int add = 1;
  for (int i = 0; i < n->size; i++) {
    if (n->entries[i].value == value) {
      add = 0;
      break;
    }
  }
  n->card += add;

  n->entries[n->size++] = (ScoredListEntry){.docId = ent->docId, .value = ent->value};
  return n->card;
}

double ScoredList_Split(void *ctx, void **lp, void **rp) {
  ScoredList *n = ctx;

  double scores[n->size];
  for (size_t i = 0; i < n->size; i++) {
    scores[i] = n->entries[i].value;
  }

  double split = qselect(scores, n->size, n->size / 2);

  ScoredList *ll = NewScoredList(n->size / 2 + 1);
  ScoredList *rl = NewScoredList(n->size / 2 + 1);
  for (size_t i = 0; i < n->size; i++) {
    ScoredList_Add(n->entries[i].value < split ? ll : rl, &n->entries[i], n->entries[i].value);
  }
  *lp = ll;
  *rp = rl;

  return split;
}

int testRangeTree() {
  RangeTree *t = NewRangeTree(NewScoredList(100), ScoredList_Add, ScoredList_Split);
  ASSERT(t != NULL);
  int count = 0;
  srand(1337);
  for (int i = 0; i < 1000; i++) {
    ScoredListEntry ent = {i, (double)(rand() % 1000)};
    count += RangeTree_Add(t, &ent, ent.value);
  }
  printf("count: %d\n", count);
  return 0;
}
int main(int argc, char **argv) {
  TESTFUNC(testRangeTree);
}
