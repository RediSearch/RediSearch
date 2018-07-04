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
#include "util/khash.h"

/* A numeric range is a node in a numeric range tree, representing a range of values bunched
 * toghether.
 * Since we do not know the distribution of scores ahead, we use a splitting approach - we start
 * with single value nodes, and when a node passes some cardinality we split it.
 * We save the minimum and maximum values inside the node, and when we split we split by finding the
 * median value */
KHASH_MAP_INIT_INT64(rangeVals, uint16_t)

typedef struct {
  double minVal;
  double maxVal;
  uint32_t splitCard;   // The maximum number unique values this range can contain before split
  uint32_t numEntries;  // Number of actual docs
  khash_t(rangeVals) * htvals;
  InvertedIndex *entries;
  double unique_sum;
} NumericRange;

#define NumericRange_GetCardinality(nr) ((nr)->htvals ? kh_size((nr)->htvals) : 0)
#define NumericRange_GetDocCount(nr) ((nr)->numEntries)

/* NumericRangeNode is a node in the range tree that can have a range in it or not, and can be a
 * leaf or not */
typedef struct rtNode {
  double value;
  int maxDepth;
  unsigned level;
  struct rtNode *left;
  struct rtNode *right;
  NumericRange *range;
} NumericRangeNode;

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

} NumericRangeTree;

#define NumericRangeNode_IsLeaf(n) (n->left == NULL && n->right == NULL)

struct indexIterator *NewNumericRangeIterator(NumericRange *nr, NumericFilter *f);

struct indexIterator *NewNumericFilterIterator(RedisSearchCtx *ctx, NumericFilter *flt,
                                               ConcurrentSearchCtx *csx);

/* Add an entry to a numeric range node. Returns the cardinality of the range after the
 * inserstion.
 * No deduplication is done */
// int NumericRange_Add(NumericRange *r, t_docId docId, double value, int checkCard);

/* Split n into two ranges, lp for left, and rp for right. We split by the median score */
double NumericRange_Split(NumericRange *n, NumericRangeNode **lp, NumericRangeNode **rp);

#define NR_DELETE_NOTFOUND 0
#define NR_DELETE_REMOVED 1
#define NR_DELETE_REMAINING 2
/**
 * Removes a value from a numeric range. Used in conjunction with garbage collection.
 * Returns NOTFOUND if the entry is not present, REMOVED if the value has been
 * removed (i.e. no other entry contains this value), or REMAINING if another ID
 * contains this value.
 */
int NumericRange_RemoveEntry(NumericRange *n, double value);

/* Create a new range node with the given capacity, minimum and maximum values */
NumericRangeNode *NewLeafNode(size_t cap, double min, double max, size_t splitCard);

/* Add a value to a tree node or its children recursively. Splits the relevant node if needed.
 * Returns 0 if no nodes were split, 1 if we splitted nodes */
// int NumericRangeNode_Add(NumericRangeNode *n, t_docId docId, double value);

/* Recursively find all the leaves under a node that correspond to a given min-max range. Returns a
 * vector with range node pointers.  */
Vector *NumericRangeNode_FindRange(NumericRangeNode *n, double min, double max);

/* Recursively free a node and its children */
void NumericRangeNode_Free(NumericRangeNode *n);

/* Create a new tree */
NumericRangeTree *NewNumericRangeTree();

void NumericRangeTree_RemoveValue(NumericRangeTree *t, double value);

/* Add a value to a tree. Returns 0 if no nodes were split, 1 if we splitted nodes */
int NumericRangeTree_Add(NumericRangeTree *t, t_docId docId, double value);

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
void NumericRangeTree_Dump(const NumericRangeTree *t);
#endif
