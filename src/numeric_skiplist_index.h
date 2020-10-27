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
  NumericRangeNode **nodesStack;
} NumericSkiplistIterator;
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
} NumericSkiplist;

struct InvertedIndex;
typedef struct {
  double value;
  struct InvertedIndex *invidx;
} NumericSkiplistNode;

typedef struct {
  NumericSkiplistNode *nsn;
  const NumericFilter *f;
} NumericSkiplistReaderCtx;

#define NumericRangeNode_IsLeaf(n) (n->left == NULL && n->right == NULL)

struct indexIterator *NewNumericSkiplistIterator(RedisSearchCtx *ctx, const NumericFilter *flt,
                                         ConcurrentSearchCtx *csx, FieldType forType);

/* Add a value to a tree. Returns 0 if no nodes were split, 1 if we splitted nodes */
NRN_AddRv NumericSkiplist_Add(NumericSkiplist *t, t_docId docId, double value);

/* Recursively find all the leaves under tree's root, that correspond to a given min-max range.
 * Returns a vector with range node pointers. */
Vector *NumericSkiplist_Find(NumericSkiplist *t, double min, double max);

NumericSkiplist *OpenNumericSkiplistIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey);

int NumericIndexType_Register(RedisModuleCtx *ctx);
void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver);
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value);
void NumericIndexType_Digest(RedisModuleDigest *digest, void *value);
void NumericIndexType_Free(void *value);


// TODO: move to .c file
#include "dep/skiplist/skiplist.h"
typedef skiplistIterator NumericSkiplistIterator;

NumericSkiplistIterator *NumericSkiplistIterator_New(NumericSkiplist *t);
NumericSkiplistNode *NumericSkiplistIterator_Next(NumericSkiplistIterator *iter);
void NumericSkiplistIterator_Free(NumericSkiplistIterator *iter);

#ifdef __cplusplus
}
#endif
#endif
