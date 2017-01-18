#ifndef __RANGE_TREE_H__
#define __RANGE_TREE_H__

#include <stdio.h>
#include <stdlib.h>
#include "../rmutil/vector.h"
#include "../types.h"
#define RT_LEAF_CARDINALITY_MAX 500

typedef struct {
  t_docId docId;
  double value;
} NumericRangeEntry;

typedef struct {
  double minVal;
  double maxVal;

  u_int32_t size;
  u_int32_t cap;
  u_int16_t card;
  NumericRangeEntry *entries;
} NumericRange;

struct rtNode;

typedef struct rtBranchNode {
  double value;
  struct rtNode *left;
  struct rtNode *right;
} RangeTreeBranchNode;

typedef struct rtNode {
  union {
    RangeTreeBranchNode node;
    NumericRange range;
  };
  u_char isLeaf;
} RangeTreeNode;

typedef struct {
  RangeTreeNode *root;
  size_t numRanges;
  size_t numEntries;
} RangeTree;

int NumericRange_Add(NumericRange *r, t_docId docId, double value);
double NumericRange_Split(NumericRange *n, RangeTreeNode **lp, RangeTreeNode **rp);

RangeTreeNode *NewNumericRangeNode(size_t cap, double min, double max);
void RangeTreNode_ToBranch(RangeTreeNode *n, double value, RangeTreeNode *left,
                           RangeTreeNode *right);

int RangeTreeNode_Add(RangeTreeNode *n, t_docId docId, double value);
Vector *RangeTreeNode_FindRange(RangeTreeNode *n, double min, double max);
void RangeTreeNode_Free(RangeTreeNode *n);

RangeTree *NewRangeTree();
int RangeTree_Add(RangeTree *t, t_docId docId, double value);
Vector *RangeTree_Find(RangeTree *t, double min, double max);
void RangeTree_Free(RangeTree *t);

#endif