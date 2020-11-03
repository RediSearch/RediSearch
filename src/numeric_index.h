
#pragma once

#include "object.h"
#include "redisearch.h"
#include "index_result.h"
#include "redismodule.h"
#include "search_ctx.h"
#include "concurrent_ctx.h"
#include "inverted_index.h"
#include "numeric_filter.h"

#include "rmutil/vector.h"

#include <stdio.h>
#include <stdlib.h>
#include <functional>

///////////////////////////////////////////////////////////////////////////////////////////////

#define RT_LEAF_CARDINALITY_MAX 500

struct CardinalityValue {
  CardinalityValue(double value, size_t appearances = 1) : value(value), appearances(appearances) {}

  double value;
  size_t appearances;
};

//---------------------------------------------------------------------------------------------

/* A numeric range is a node in a numeric range tree, representing a range of values bunched
 * toghether.
 * Since we do not know the distribution of scores ahead, we use a splitting approach - we start
 * with single value nodes, and when a node passes some cardinality we split it.
 * We save the minimum and maximum values inside the node, and when we split we split by finding the
 * median value */

struct NumericRangeNode;

struct NumericRange {
  double minVal;
  double maxVal;

  double unique_sum;

  u_int16_t card;
  uint32_t splitCard;
  arrayof(CardinalityValue) values;
  InvertedIndex entries;

  NumericRange(double min, double max, size_t splitCard);
  ~NumericRange();

  // Add an entry to a numeric range node. Returns the cardinality of the range after the inserstion.
  // No deduplication is done.
  size_t Add(t_docId docId, double value, int checkCard);

  // Split n into two ranges, lp for left, and rp for right. We split by the median score
  double Split(NumericRangeNode **lp, NumericRangeNode **rp);

  bool Contained(double min, double max) const;
  bool Contains(double min, double max) const;
  bool Overlaps(double min, double max) const;
};

//---------------------------------------------------------------------------------------------

struct NRN_AddRv {
  NRN_AddRv() : sz(0), changed(0) {}
  uint32_t sz;
  uint32_t changed;
};

//---------------------------------------------------------------------------------------------

// NumericRangeNode is a node in the range tree that can have a range in it or not, and can be a leaf or not

struct NumericRangeNode : public Object {
  // Create a new range node with the given capacity, minimum and maximum values
  NumericRangeNode(size_t cap, double min, double max, size_t splitCard);
  ~NumericRangeNode(); // Recursively free a node and its children

  double value;
  int maxDepth;
  struct NumericRangeNode *left;
  struct NumericRangeNode *right;

  NumericRange *range;

  // Add a value to a tree node or its children recursively. Splits the relevant node if needed.
  // Returns 0 if no nodes were split, 1 if we splitted nodes.
  NRN_AddRv Add(t_docId docId, double value);
  void AddChildren(Vector *v, double min, double max);

  // Recursively find all the leaves under a node that correspond to a given min-max range.
  // Returns a vector with range node pointers.
  Vector *FindRange(double min, double max);

  bool IsLeaf() const { return left == NULL && right == NULL; }

  //void Traverse(void (*callback)(NumericRangeNode *n, void *arg), void *arg);
  void Traverse(std::function<void(NumericRangeNode *, void *arg)> fn, void *arg);
};

//---------------------------------------------------------------------------------------------

struct NumericRangeTreeIterator : public Object {
  arrayof(NumericRangeNode*) nodesStack;

  NumericRangeTreeIterator(NumericRangeTree *t);
  ~NumericRangeTreeIterator();

  NumericRangeNode *Next();
};

//---------------------------------------------------------------------------------------------

// The root tree and its metadata
struct NumericRangeTree : public Object {
  NumericRangeNode *root;
  size_t numRanges;
  size_t numEntries;
  size_t card;
  t_docId lastDocId;
  uint32_t revisionId;
  uint32_t uniqueId;

  static uint16_t NumericRangeTree::UniqueId;

  NumericRangeTree();
  ~NumericRangeTree();
  static void Free(NumericRangeTree *p);

  // Add a value to a tree. Returns 0 if no nodes were split, 1 if we splitted nodes
  size_t Add(t_docId docId, double value);

  // Recursively find all the leaves under tree's root, that correspond to a given min-max range.
  // Returns a vector with range node pointers.
  Vector *Find(double min, double max);
};

//---------------------------------------------------------------------------------------------

NumericRangeTree *OpenNumericIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey);

//---------------------------------------------------------------------------------------------

class NumericFilterIterator : public IndexIterator {

};

struct IndexIterator *NewNumericRangeIterator(const IndexSpec *sp, NumericRange *nr,
                                              const NumericFilter *f);

struct IndexIterator *NewNumericFilterIterator(RedisSearchCtx *ctx, const NumericFilter *flt,
                                               ConcurrentSearchCtx *csx);

//---------------------------------------------------------------------------------------------

extern RedisModuleType *NumericIndexType;

int NumericIndexType_Register(RedisModuleCtx *ctx);
void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver);
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value);
void NumericIndexType_Digest(RedisModuleDigest *digest, void *value);
void NumericIndexType_Free(void *value);

///////////////////////////////////////////////////////////////////////////////////////////////
