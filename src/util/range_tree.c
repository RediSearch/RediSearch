#include "range_tree.h"
#include "sys/param.h"
#include "../rmutil/vector.h"

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

int NumericRange_Add(NumericRange *n, t_docId docId, double value) {

  if (n->size >= n->cap) {
    n->cap = n->cap ? MAX(n->cap * 2, 1024 * 1024) : 2;
    n->entries = realloc(n->entries, n->cap * sizeof(NumericRangeEntry));
  }

  int add = 1;
  for (int i = 0; i < n->size; i++) {
    if (n->entries[i].value == value) {
      add = 0;
      break;
    }
  }
  if (add) {
    if (value < n->minVal) n->minVal = value;
    if (value > n->maxVal) n->maxVal = value;
    ++n->card;
  }

  n->entries[n->size++] = (NumericRangeEntry){.docId = docId, .value = value};
  return n->card;
}

double NumericRange_Split(NumericRange *n, RangeTreeNode **lp, RangeTreeNode **rp) {

  double scores[n->size];
  for (size_t i = 0; i < n->size; i++) {
    scores[i] = n->entries[i].value;
  }

  double split = qselect(scores, n->size, n->size / 2);
  *lp = NewNumericRangeNode(n->size / 2, n->minVal, split);
  *rp = NewNumericRangeNode(n->size / 2, split, n->maxVal);

  for (u_int32_t i = 0; i < n->size; i++) {
    NumericRange_Add(n->entries[i].value < split ? &(*lp)->range : &(*rp)->range,
                     n->entries[i].docId, n->entries[i].value);
  }

  return split;
}

RangeTreeNode *NewNumericRangeNode(size_t cap, double min, double max) {

  RangeTreeNode *n = malloc(sizeof(RangeTreeNode));
  n->isLeaf = 1;
  n->range = (NumericRange){.minVal = min,
                            .maxVal = max,
                            .cap = cap,
                            .size = 0,
                            .card = 0,
                            .entries = calloc(cap, sizeof(NumericRangeEntry))};
  return n;
}

void RangeTreNode_ToBranch(RangeTreeNode *n, double value, RangeTreeNode *left,
                           RangeTreeNode *right) {
  if (n->isLeaf) {
    free(n->range.entries);
  }
  n->isLeaf = 0;
  n->node.left = left;
  n->node.right = right;
  n->node.value = value;
}

#define __isLeaf(n) (n->isLeaf)

int RangeTreeNode_Add(RangeTreeNode *n, t_docId docId, double value) {

  while (n && !__isLeaf(n)) {
    n = value < n->node.value ? n->node.left : n->node.right;
  }

  // printf("Adding to RangeTreeLeaf %f..%f\n", n->RangeTreeLeaf->min, n->RangeTreeLeaf->max);
  int card = NumericRange_Add(&n->range, docId, value);
  // printf("%f..%f card after insertion: %d\n", n->leaf->min, n->leaf->max, card);
  if (card >= RT_LEAF_CARDINALITY_MAX) {
    printf("Splitting node with leaf %f..%f\n", n->range.minVal, n->range.maxVal);

    RangeTreeNode *rl, *ll;
    double split = NumericRange_Split(&n->range, &ll, &rl);
    RangeTreNode_ToBranch(n, split, ll, rl);
    return 1;
  }

  return 0;
}

Vector *RangeTreeNode_FindRange(RangeTreeNode *n, double min, double max) {

  Vector *leaves = NewVector(NumericRange *, 8);

  RangeTreeNode *vmin = n, *vmax = n;

  while (vmin == vmax && !__isLeaf(vmin)) {
    vmin = min < vmin->node.value ? vmin->node.left : vmin->node.right;
    vmax = max < vmax->node.value ? vmax->node.left : vmax->node.right;
  }

  Vector *stack = NewVector(RangeTreeNode *, 8);

  // put on the stack all right trees of our path to the minimum node
  while (!__isLeaf(vmin)) {
    if (min < vmin->node.value) {
      Vector_Push(stack, vmin->node.right);
    }
    vmin = min < vmin->node.value ? vmin->node.left : vmin->node.right;
  }
  // put on the stack all left trees of our path to the maximum node
  while (vmax && !__isLeaf(vmax)) {
    if (max >= vmax->node.value) {
      Vector_Push(stack, vmax->node.left);
    }
    vmax = max < vmax->node.value ? vmax->node.left : vmax->node.right;
  }

  Vector_Push(leaves, &vmin->range);
  if (vmin != vmax) Vector_Push(leaves, &vmax->range);

  while (Vector_Size(stack)) {
    RangeTreeNode *n;
    if (!Vector_Pop(stack, &n)) break;
    if (!n) continue;

    if (__isLeaf(n)) {
      Vector_Push(leaves, &n->range);
    } else {

      Vector_Push(stack, n->node.left);
      Vector_Push(stack, n->node.right);
    }
  }

  Vector_Free(stack);

  // printf("found %d leaves\n", Vector_Size(leaves));
  return leaves;
}

void RangeTreeNode_Free(RangeTreeNode *n) {
  if (__isLeaf(n)) {
    free(n->range.entries);
  } else {
    RangeTreeNode_Free(n->node.left);
    RangeTreeNode_Free(n->node.right);
  }
  free(n);
}

RangeTree *NewRangeTree(void *root) {
  RangeTree *ret = malloc(sizeof(RangeTree));

  ret->root = NewNumericRangeNode(RT_LEAF_CARDINALITY_MAX, 0, 0);
  return ret;
}

int RangeTree_Add(RangeTree *t, t_docId docId, double value) {
  if (!value) return 0;
  return RangeTreeNode_Add(t->root, docId, value);
}

Vector *RangeTree_Find(RangeTree *t, double min, double max) {
  return RangeTreeNode_FindRange(t->root, min, max);
}

void RangeTree_Free(RangeTree *t) {
  RangeTreeNode_Free(t->root);
  free(t);
}