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
#include "util/skiplist.h"

#define RT_LEAF_CARDINALITY_MAX 500

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  double value;
  size_t appearances;
} CardinalityValue;

/* A numeric skiplist holds inverted indexes as elements in nodes.
 * The comparison function is used to find the largest inverted index that is smaller then
 * the inserted value */
// TODO: remove cardinality and use average instead. Equal distribution will remain similar 
// while saving the memory and cpu used to calculate accurate median. 
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

typedef struct {
  int sz;
  int numRecords;
  uint32_t changed;
} NRN_AddRv;

typedef struct {
  struct skiplist *sl;  // skiplist
  size_t numEntries;    // Number of entries
  size_t numRanges;     // TODO: same as invidx??
  t_docId lastDocId;    // Last docID in the 
  uint32_t revisionId;
  uint32_t uniqueId;
} NumericRangeSkiplist;

typedef struct {
  double value;
  struct InvertedIndex *invidx;
} NumericSkiplistNode;

typedef skiplistIterator NumericSkiplistIterator;

struct indexIterator *NewNumericRangeIterator(const IndexSpec *sp, NumericRange *nr,
                                              const NumericFilter *f);

struct indexIterator *NewNumericFilterIterator(RedisSearchCtx *ctx, const NumericFilter *flt,
                                               ConcurrentSearchCtx *csx, FieldType forType);

/* Add an entry to a numeric range node. Returns the cardinality of the range after the
 * inserstion.
 * No deduplication is done */
size_t NumericRange_Add(NumericRange *r, t_docId docId, double value, int checkCard);

/* Split n into two ranges, lp for left, and rp for right. We split by the median score */
double NumericRange_Split(NumericRange *n, NumericRange **lp, NumericRange **rp,
                          NRN_AddRv *rv);

/* Create a new range node with the given capacity, minimum and maximum values */
NumericRange *NewLeafNode(size_t cap, double min, double max, size_t splitCard);

/* Recursively free a node and its children */
void NumericRange_Free(NumericRange *n);

/* Create a new skiplist */
NumericRangeSkiplist *NewNumericRangeSkiplist();

/* Add a value to a skiplist. Returns 0 if no nodes were split, 1 if we splitted nodes */
NRN_AddRv NumericRangeSkiplist_Add(NumericRangeSkiplist *t, t_docId docId, double value);

/* Find all the elements in skiplist, that correspond to a given min-max range.
 * Returns a vector with range node pointers. */
Vector *NumericRangeSkiplist_Find(NumericRangeSkiplist *t, double min, double max);

/* Free the Skiplist and all nodes */
void NumericRangeSkiplist_Free(NumericRangeSkiplist *t);

extern RedisModuleType *NumericIndexType;

NumericRangeSkiplist *OpenNumericIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey);

int NumericIndexType_Register(RedisModuleCtx *ctx);
void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver);
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value);
void NumericIndexType_Digest(RedisModuleDigest *digest, void *value);
void NumericIndexType_Free(void *value);

NumericSkiplistIterator *NumericSkiplistIterator_New(const NumericRangeSkiplist *t,
                                                     NumericRange *start);
NumericRange *NumericSkiplistIterator_Next(NumericSkiplistIterator *iter);
void NumericSkiplistIterator_Free(NumericSkiplistIterator *iter);

#ifdef __cplusplus
}
#endif
#endif
