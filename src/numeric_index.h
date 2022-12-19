
#pragma once

#include "object.h"
#include "redisearch.h"
#include "index_result.h"
#include "inverted_index.h"
#include "redismodule.h"
#include "search_ctx.h"
#include "concurrent_ctx.h"
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

// A numeric range is a node in a numeric range tree, representing a range of values bunched
// toghether.
// Since we do not know the distribution of scores ahead, we use a splitting approach - we start
// with single value nodes, and when a node passes some cardinality we split it.
// We save the minimum and maximum values inside the node, and when we split we split by finding the
// median value.

struct NumericRangeNode;

struct NumericRange {
  double minVal;
  double maxVal;

  double unique_sum;

  uint16_t card;
  uint32_t splitCard;
  arrayof(CardinalityValue) values;
  InvertedIndex entries;

  NumericRange(double min, double max, uint32_t splitCard);
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
  int sz;
  int changed;
};

//---------------------------------------------------------------------------------------------

// NumericRangeNode is a node in the range tree that can have a range in it or not, and can be a leaf or not

struct NumericRangeNode : public Object {
  // Create a new range node with the given capacity, minimum and maximum values
  NumericRangeNode(size_t cap, double min, double max, uint32_t splitCard);
  ~NumericRangeNode(); // Recursively free a node and its children

  double value;
  int maxDepth;
  NumericRangeNode *left;
  NumericRangeNode *right;

  NumericRange *range;

  // Add a value to a tree node or its children recursively. Splits the relevant node if needed.
  // Returns 0 if no nodes were split, 1 if we splitted nodes.
  NRN_AddRv Add(t_docId docId, double value);
  void AddChildren(Vector<NumericRange> &v, double min, double max);

  // Recursively find all the leaves under a node that correspond to a given min-max range.
  // Returns a vector with range node pointers.
  Vector<NumericRange> FindRange(double min, double max);

  bool IsLeaf() const { return left == nullptr && right == nullptr; }

  template <typename F>
  void Traverse(F fn) {
    fn(this);
    if (left) left->Traverse(fn);
    if (right) right->Traverse(fn);
  }
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
struct NumericRangeTree : public BaseIndex {
  NumericRangeNode *root;
  size_t numRanges;
  size_t numEntries;
  size_t card;
  t_docId lastDocId;
  uint32_t revisionId;
  uint32_t uniqueId;

  static uint16_t UniqueId;

  NumericRangeTree();
  ~NumericRangeTree();
  static void Free(NumericRangeTree *p);

  // Add a value to a tree. Returns 0 if no nodes were split, 1 if we splitted nodes
  int Add(t_docId docId, double value);

  // Recursively find all the leaves under tree's root, that correspond to a given min-max range.
  // Returns a vector with range node pointers.
  Vector<NumericRange> Find(double min, double max);
};

//---------------------------------------------------------------------------------------------

NumericRangeTree *OpenNumericIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey);

//---------------------------------------------------------------------------------------------

#define NR_EXPONENT 4
#define NR_MAXRANGE_CARD 2500
#define NR_MAXRANGE_SIZE 10000
#define NR_MAX_DEPTH 2

//---------------------------------------------------------------------------------------------

struct NumericUnion : Object {
  IndexIterator *it;
  uint32_t lastRevId;

  NumericUnion(IndexIterator *it, uint32_t lastRevId) : it(it), lastRevId(lastRevId) {}
  NumericUnion(NumericUnion &&nu) : it(nu.it), lastRevId(nu.lastRevId) {
    nu.it = nullptr;
  }
};

//---------------------------------------------------------------------------------------------

struct NumericUnionConcKey : ConcurrentKey {
  NumericUnionConcKey(RedisModuleKey *key, RedisModuleString *keyName, const NumericRangeTree &t, IndexIterator *it) :
    ConcurrentKey(key, keyName), nu(it, t.revisionId) {}
  NumericUnionConcKey(NumericUnionConcKey &&key) : ConcurrentKey(std::move(key)), nu(std::move(key.nu)) {}

  NumericUnion nu;

  void Reopen() override {
    NumericRangeTree *t = static_cast<NumericRangeTree *>(RedisModule_ModuleTypeGetValue(key));

    // If the key has been deleted we'll get a nullptr heere, so we just mark ourselves as EOF
    // We simply abort the root iterator which is either a union of many ranges or a single range.
    // If the numeric range tree has chained (split, nodes deleted, etc) since we last closed it,
    // We cannot continue iterating it, since the underlying pointers might be screwed.
    // For now we will just stop processing this query. This causes the query to return bad results,
    // so in the future we can try an reset the state here.
    if (key == nullptr || t == nullptr || t->revisionId != nu.lastRevId) {
      nu.it->Abort();
    }
  }
};

//---------------------------------------------------------------------------------------------

extern RedisModuleType *NumericIndexType;

struct IndexIterator *NewNumericFilterIterator(RedisSearchCtx *ctx, const NumericFilter *flt,
                                               ConcurrentSearch *csx);

int NumericIndexType_Register(RedisModuleCtx *ctx);
void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver);
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value);
void NumericIndexType_Digest(RedisModuleDigest *digest, void *value);
void NumericIndexType_Free(void *value);

///////////////////////////////////////////////////////////////////////////////////////////////
