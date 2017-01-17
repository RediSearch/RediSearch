#ifndef __RANGE_TREE_H__
#define __RANGE_TREE_H__

#include <stdio.h>
#include <stdlib.h>
#include "../rmutil/vector.h"

#define RT_LEAF_CARDINALITY_MAX 500

typedef struct {
  double min;
  double max;

  void *entries;
} RangeTreeLeaf;

typedef struct rtNode {
  double value;
  union {
    struct {
      struct rtNode *left;
      struct rtNode *right;
    };
    RangeTreeLeaf *leaf;
  };
} RangeTreeNode;

typedef int (*RangeTreeValueAddFunc)(void *ctx, void *newval);
typedef double (*RangeTreeSplitFunc)(void *ctx, void **left, void **right);

typedef struct {
  RangeTreeNode *root;
  RangeTreeValueAddFunc addFunc;
  RangeTreeSplitFunc splitFunc;
} RangeTree;

RangeTreeLeaf *NewRangeTreeLeaf(void *values, double min, double max);
void RangeTreeLeaf_Split(RangeTreeLeaf *l, RangeTreeLeaf **left, RangeTreeLeaf **right,
                         RangeTreeSplitFunc sf);
int RangeTreeLeaf_Add(RangeTreeLeaf *l, void *entry, double value, RangeTreeValueAddFunc f);

RangeTreeNode *NewRangeTreeNode(RangeTreeLeaf *l);
int RangeTreeNode_Add(RangeTreeNode *n, void *entry, double value, RangeTreeValueAddFunc f,
                      RangeTreeSplitFunc sf);
Vector *RangeTreeNode_FindRange(RangeTreeNode *n, double min, double max);
void RangeTreeNode_Free(RangeTreeNode *n);

RangeTree *NewRangeTree(void *root, RangeTreeValueAddFunc af, RangeTreeSplitFunc sf);
int RangeTree_Add(RangeTree *t, void *entry, double value);
Vector *RangeTree_Find(RangeTree *t, double min, double max);

#endif