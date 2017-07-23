#ifndef __NUMERIC_INDEX_H__
#define __NUMERIC_INDEX_H__

#include <stdio.h>
#include <stdlib.h>
#include "rmutil/vector.h"
#include "redisearch.h"
#include "index_result.h"
#include "redismodule.h"
#include "search_ctx.h"
#include "inverted_index.h"
#include "numeric_filter.h"

#define RT_LEAF_CARDINALITY_MAX 500

/* A numeric range is a node in a numeric range tree, representing a range of values bunched
 * toghether.
 * Since we do not know the distribution of scores ahead, we use a splitting approach - we start
 * with single value nodes, and when a node passes some cardinality we split it.
 * We save the minimum and maximum values inside the node, and when we split we split by finding the
 * median value */
typedef struct {
  double minVal;
  double maxVal;

  uint32_t size;
  uint32_t cap;
  u_int16_t card;
  uint32_t splitCard;
  float *values;
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

/* The root tree and its metadata */
typedef struct {
  NumericRangeNode *root;
  size_t numRanges;
  size_t numEntries;
  size_t card;
} NumericRangeTree;

/* NumericRangeIterator is the index iterator responsible for iterating a single numeric range. When
 * we perform a query we union multiple such ranges */
typedef struct {
  NumericRange *rng;
  NumericFilter *nf;
  t_docId lastDocId;
  u_int offset;
  int atEOF;
  RSIndexResult *rec;

} NumericRangeIterator;

/* Read the next entry from the iterator, into hit *e.
  *  Returns INDEXREAD_EOF if at the end */
int NR_Read(void *ctx, RSIndexResult **e);

RSIndexResult *NR_Current(void *ctx);

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int NR_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit);

/* the last docId read */
t_docId NR_LastDocId(void *ctx);

/* can we continue iteration? */
int NR_HasNext(void *ctx);

struct indexIterator;
/* release the iterator's context and free everything needed */
void NR_Free(struct indexIterator *self);

/* Return the number of results in this iterator. Used by the query execution
 * on the top iterator */
size_t NR_Len(void *ctx);

struct indexIterator *NewNumericRangeIterator(NumericRange *nr, NumericFilter *f);

struct indexIterator *NewNumericFilterIterator(NumericRangeTree *t, NumericFilter *f);

/* Add an entry to a numeric range node. Returns the cardinality of the range after the
 * inserstion.
 * No deduplication is done */
int NumericRange_Add(NumericRange *r, t_docId docId, double value, int checkCard);

/* Split n into two ranges, lp for left, and rp for right. We split by the median score */
double NumericRange_Split(NumericRange *n, NumericRangeNode **lp, NumericRangeNode **rp);

/* Create a new range node with the given capacity, minimum and maximum values */
NumericRangeNode *NewLeafNode(size_t cap, double min, double max, size_t splitCard);

/* Add a value to a tree node or its children recursively. Splits the relevant node if needed.
 * Returns 0 if no nodes were split, 1 if we splitted nodes */
int NumericRangeNode_Add(NumericRangeNode *n, t_docId docId, double value);

/* Recursively find all the leaves under a node that correspond to a given min-max range. Returns a
 * vector with range node pointers.  */
Vector *NumericRangeNode_FindRange(NumericRangeNode *n, double min, double max);

/* Recursively free a node and its children */
void NumericRangeNode_Free(NumericRangeNode *n);

/* Create a new tree */
NumericRangeTree *NewNumericRangeTree();

/* Add a value to a tree. Returns 0 if no nodes were split, 1 if we splitted nodes */
int NumericRangeTree_Add(NumericRangeTree *t, t_docId docId, double value);

/* Recursively find all the leaves under tree's root, that correspond to a given min-max range.
 * Returns a vector with range node pointers. */
Vector *NumericRangeTree_Find(NumericRangeTree *t, double min, double max);

/* Free the tree and all nodes */
void NumericRangeTree_Free(NumericRangeTree *t);

extern RedisModuleType *NumericIndexType;

NumericRangeTree *OpenNumericIndex(RedisSearchCtx *ctx, const char *fname);
int NumericIndexType_Register(RedisModuleCtx *ctx);
void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver);
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value);
void NumericIndexType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value);
void NumericIndexType_Digest(RedisModuleDigest *digest, void *value);
void NumericIndexType_Free(void *value);
#endif