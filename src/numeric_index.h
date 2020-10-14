#ifndef __NUMERIC_INDEX_H__
#define __NUMERIC_INDEX_H__

#include <stdio.h>
#include <stdlib.h>
#include "rmutil/vector.h"
#include "redisearch.h"
#include "index_result.h"
#include "redismodule.h"
#include "search_ctx.h"
#include "concurrent_ctx.h"
#include "inverted_index.h"
#include "numeric_filter.h"

#define RT_LEAF_CARDINALITY_MAX 500

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  double value;
  size_t appearances;
} CardinalityValue;

/* A numeric range is a node in a numeric range tree, representing a range of values bunched
 * toghether.
 * Since we do not know the distribution of scores ahead, we use a splitting approach - we start
 * with single value nodes, and when a node passes some cardinality we split it.
 * We save the minimum and maximum values inside the node, and when we split we split by finding the
 * median value */
typedef struct {
  double minVal;
  double maxVal;

  double unique_sum;

  size_t invertedIndexSize;

  u_int16_t card;
  uint32_t splitCard;
  CardinalityValue *values;
  InvertedIndex *entries;
} NumericRange;

/* NumericRangeNode is a node in the range tree that can have a range in it or not, and can be a
 * leaf or not */
typedef struct rtNode {
  double value;
  int maxDepth;
  struct rtNode *left;
  struct rtNode *right;

  NumericRange *range;
} NumericRangeNode;

typedef struct {
  int sz;
  int numRecords;
  uint32_t changed;
} NRN_AddRv;

typedef struct {
  NumericRangeNode **nodesStack;
} NumericRangeTreeIterator;

/* The root tree and its metadata */
typedef struct {
  NumericRangeNode *root;
  size_t numRanges;
  size_t numEntries;
  size_t card;
  t_docId lastDocId;

  uint32_t revisionId;

  uint32_t uniqueId;

} NumericRangeTree;

#define NumericRangeNode_IsLeaf(n) (n->left == NULL && n->right == NULL)

struct indexIterator *NewNumericRangeIterator(const IndexSpec *sp, NumericRange *nr,
                                              const NumericFilter *f);

struct indexIterator *NewNumericFilterIterator(RedisSearchCtx *ctx, const NumericFilter *flt,
                                               ConcurrentSearchCtx *csx, FieldType forType);

/* Add an entry to a numeric range node. Returns the cardinality of the range after the
 * inserstion.
 * No deduplication is done */
size_t NumericRange_Add(NumericRange *r, t_docId docId, double value, int checkCard);

/* Split n into two ranges, lp for left, and rp for right. We split by the median score */
double NumericRange_Split(NumericRange *n, NumericRangeNode **lp, NumericRangeNode **rp,
                          NRN_AddRv *rv);

/* Create a new range node with the given capacity, minimum and maximum values */
NumericRangeNode *NewLeafNode(size_t cap, double min, double max, size_t splitCard);

/* Add a value to a tree node or its children recursively. Splits the relevant node if needed.
 * Returns 0 if no nodes were split, 1 if we splitted nodes */
NRN_AddRv NumericRangeNode_Add(NumericRangeNode *n, t_docId docId, double value);

/* Recursively find all the leaves under a node that correspond to a given min-max range. Returns a
 * vector with range node pointers.  */
Vector *NumericRangeNode_FindRange(NumericRangeNode *n, double min, double max);

/* Recursively free a node and its children */
void NumericRangeNode_Free(NumericRangeNode *n);

/* Create a new tree */
NumericRangeTree *NewNumericRangeTree();

/* Add a value to a tree. Returns 0 if no nodes were split, 1 if we splitted nodes */
NRN_AddRv NumericRangeTree_Add(NumericRangeTree *t, t_docId docId, double value);

/* Recursively find all the leaves under tree's root, that correspond to a given min-max range.
 * Returns a vector with range node pointers. */
Vector *NumericRangeTree_Find(NumericRangeTree *t, double min, double max);

/* Free the tree and all nodes */
void NumericRangeTree_Free(NumericRangeTree *t);

extern RedisModuleType *NumericIndexType;

NumericRangeTree *OpenNumericIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey);

int NumericIndexType_Register(RedisModuleCtx *ctx);
void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver);
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value);
void NumericIndexType_Digest(RedisModuleDigest *digest, void *value);
void NumericIndexType_Free(void *value);

NumericRangeTreeIterator *NumericRangeTreeIterator_New(NumericRangeTree *t);
NumericRangeNode *NumericRangeTreeIterator_Next(NumericRangeTreeIterator *iter);
void NumericRangeTreeIterator_Free(NumericRangeTreeIterator *iter);

#ifdef __cplusplus
}
#endif
#endif
