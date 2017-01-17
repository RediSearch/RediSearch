#include "range_tree.h"

RangeTreeLeaf *NewRangeTreeLeaf(void *entries, double min, double max) {
  RangeTreeLeaf *ret = malloc(sizeof(RangeTreeLeaf));
  ret->entries = entries;

  ret->min = min;
  ret->max = max;

  return ret;
}

void RangeTreeLeaf_Split(RangeTreeLeaf *l, RangeTreeLeaf **left, RangeTreeLeaf **right,
                         RangeTreeSplitFunc sf) {

  void *lval, *rval;
  double split = sf(l->entries, &lval, &rval);

  *left = NewRangeTreeLeaf(lval, l->min, split);
  *left = NewRangeTreeLeaf(lval, l->min, split);
}

int RangeTreeLeaf_Add(RangeTreeLeaf *l, void *entry, double value, RangeTreeValueAddFunc f) {

  int card = f(l->entries, entry);

  if (value < l->min) l->min = value;
  if (value > l->max) l->max = value;
  return card;
}

RangeTreeNode *NewRangeTreeNode(RangeTreeLeaf *l) {
  RangeTreeNode *ret = malloc(sizeof(RangeTreeNode));
  ret->leaf = l;
  // ret->left = NULL;
  // ret->right = NULL;
  ret->value = 0;
  return ret;
}

#define __isLeaf(n) (n->value == 0)

int RangeTreeNode_Add(RangeTreeNode *n, void *entry, double value, RangeTreeValueAddFunc f,
                      RangeTreeSplitFunc sf) {
  while (n && !__isLeaf(n)) {
    n = value < n->value ? n->left : n->right;
  }

  // printf("Adding to RangeTreeLeaf %f..%f\n", n->RangeTreeLeaf->min, n->RangeTreeLeaf->max);
  int card = RangeTreeLeaf_Add(n->leaf, entry, value, f);
  printf("%f..%f card after insertion: %d\n", n->leaf->min, n->leaf->max, card);
  if (card >= RT_LEAF_CARDINALITY_MAX) {
    printf("Splitting node with leaf %f..%f\n", n->leaf->min, n->leaf->max);

    RangeTreeLeaf *rl, *ll;
    RangeTreeLeaf_Split(n->leaf, &ll, &rl, sf);
    free(n->leaf);

    n->value = ll->max;

    n->right = NewRangeTreeNode(rl);
    n->left = NewRangeTreeNode(ll);
    return 1;
  }

  return 0;
}

Vector *RangeTreeNode_FindRange(RangeTreeNode *n, double min, double max) {
  Vector *leaves = NewVector(RangeTreeLeaf *, 8);

  RangeTreeNode *vmin = n, *vmax = n;

  while (vmin == vmax && !__isLeaf(vmin)) {
    vmin = min < vmin->value ? vmin->left : vmin->right;
    vmax = max < vmax->value ? vmax->left : vmax->right;
  }

  Vector *stack = NewVector(RangeTreeNode *, 8);

  // put on the stack all right trees of our path to the minimum node
  while (!__isLeaf(vmin)) {
    if (vmin->right && min < vmin->value) {
      Vector_Push(stack, vmin->right);
    }
    vmin = min < vmin->value ? vmin->left : vmin->right;
  }
  // put on the stack all left trees of our path to the maximum node
  while (vmax && !__isLeaf(vmax)) {
    if (vmax->left && max >= vmax->value) {
      Vector_Push(stack, vmax->left);
    }
    vmax = max < vmax->value ? vmax->left : vmax->right;
  }

  Vector_Push(leaves, vmin->leaf);
  if (vmin != vmax) Vector_Push(leaves, vmax->leaf);

  while (Vector_Size(stack)) {
    RangeTreeNode *n;
    if (!Vector_Pop(stack, &n)) break;
    if (!n) continue;

    if (__isLeaf(n)) Vector_Push(leaves, n->leaf);

    if (n->left) Vector_Push(stack, n->left);
    if (n->right) Vector_Push(stack, n->right);
  }

  Vector_Free(stack);

  // printf("found %d leaves\n", Vector_Size(leaves));
  return leaves;
}

void RangeTreeNode_Free(RangeTreeNode *n) {
  if (__isLeaf(n)) {
    free(n->leaf);
  } else {
    RangeTreeNode_Free(n->left);
    RangeTreeNode_Free(n->right);
  }
}

RangeTree *NewRangeTree(void *root, RangeTreeValueAddFunc af, RangeTreeSplitFunc sf) {
  RangeTree *ret = malloc(sizeof(RangeTree));
  ret->addFunc = af;
  ret->splitFunc = sf;
  ret->root = NewRangeTreeNode(NewRangeTreeLeaf(root, 0, 0));
  return ret;
}

int RangeTree_Add(RangeTree *t, void *entry, double value) {
  if (!value) return 0;
  return RangeTreeNode_Add(t->root, entry, value, t->addFunc, t->splitFunc);
}

Vector *RangeTree_Find(RangeTree *t, double min, double max) {
  return RangeTreeNode_FindRange(t->root, min, max);
}