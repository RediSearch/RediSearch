#ifndef __NUMERIC_SKIPLIST_INDEX_H__
#define __NUMERIC_SKIPLIST_INDEX_H__

#include <stdio.h>
#include <stdlib.h>
#include "rmutil/vector.h"
#include "redisearch.h"
#include "index_result.h"
#include "redismodule.h"
#include "search_ctx.h"
#include "concurrent_ctx.h"
#include "numeric_filter.h"

#define RT_LEAF_CARDINALITY_MAX 500

#ifdef __cplusplus
extern "C" {
#endif

/*
typedef struct {
  DecimalRangeNode **nodesStack;
} DecimalSkiplistIterator;
*/
struct skiplist;

/* The root tree and its metadata */
typedef struct {
  struct skiplist *sl;  // skiplist
  size_t numInvIdx;     // Number of nodes/Inverted Indicies
  size_t numEntries;    // Number of entries
  t_docId lastDocId;    // Last docID in the 
  uint32_t revisionId;
  uint32_t uniqueId;
} DecimalSkiplist;

struct InvertedIndex;
typedef struct {
  double value;
  size_t invertedIndexSize;
  struct InvertedIndex *invidx;
} DecimalSkiplistNode;

typedef struct {
  DecimalSkiplistNode *nsn;
  const NumericFilter *f;
} DecimalSkiplistReaderCtx;

struct indexIterator *NewDecimalSkiplistIterator(RedisSearchCtx *ctx, NumericFilter *flt,
                                         ConcurrentSearchCtx *csx, FieldType forType);

/* Add a value to a tree. Returns 0 if no nodes were split, 1 if we splitted nodes */
NRN_AddRv DecimalSkiplist_Add(DecimalSkiplist *t, t_docId docId, double value);

/* Recursively find all the leaves under tree's root, that correspond to a given min-max range.
 * Returns a vector with range node pointers. */
Vector *DecimalSkiplist_Find(DecimalSkiplist *t, double min, double max);

DecimalSkiplist *OpenDecimalSkiplistIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey);

int DecimalIndexType_Register(RedisModuleCtx *ctx);
void *DecimalIndexType_RdbLoad(RedisModuleIO *rdb, int encver);
void DecimalIndexType_RdbSave(RedisModuleIO *rdb, void *value);
void DecimalIndexType_Digest(RedisModuleDigest *digest, void *value);
void DecimalIndexType_Free(void *value);


// TODO: move to .c file
#include "util/skiplist.h"
typedef skiplistIterator DecimalSkiplistIterator;

DecimalSkiplistIterator *DecimalSkiplistIterator_New(DecimalSkiplist *t, void *start);
DecimalSkiplistNode *DecimalSkiplistIterator_Next(DecimalSkiplistIterator *iter);
void DecimalSkiplistIterator_Free(DecimalSkiplistIterator *iter);

#ifdef __cplusplus
}
#endif
#endif
