/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "numeric_index.h"
#include "redis_index.h"
#include "sys/param.h"
#include "rmutil/vector.h"
#include "rmutil/util.h"
#include "index.h"
#include "util/arr.h"
#include <math.h>
#include "redismodule.h"
#include "util/misc.h"
#include "util/heap_doubles.h"

#define NR_MINRANGE_CARD 16
#define NR_MAXRANGE_CARD 2500
#define NR_MAXRANGE_SIZE 10000

#define _SPLIT_CARD_BY_DEPTH(depth) (NR_MINRANGE_CARD << ((depth) * 2)) // *2 to get exponential growth of 4

#define LAST_DEPTH_OF_NON_MAX_CARD 3 // Last depth to not have the max split cardinality
static_assert(NR_MAXRANGE_CARD < _SPLIT_CARD_BY_DEPTH(LAST_DEPTH_OF_NON_MAX_CARD + 1));
static_assert(NR_MAXRANGE_CARD >= _SPLIT_CARD_BY_DEPTH(LAST_DEPTH_OF_NON_MAX_CARD));

static inline size_t getSplitCardinality(size_t depth) {
  if (depth > LAST_DEPTH_OF_NON_MAX_CARD) return NR_MAXRANGE_CARD;
  return _SPLIT_CARD_BY_DEPTH(depth);
}

typedef struct {
  IndexIterator *it;
  uint32_t lastRevId;
  IndexSpec *sp;
  const FieldSpec *field;
} NumericUnionCtx;

void NumericRangeIterator_OnReopen(void *privdata);

/* Returns true if the entire numeric range is contained between min and max */
static inline bool NumericRange_Contained(NumericRange *n, double min, double max) {
  return n->minVal >= min && n->maxVal <= max;
}

/* Returns true if there is any overlap between the range and min/max */
static inline bool NumericRange_Overlaps(NumericRange *n, double min, double max) {
  return !(min > n->maxVal || max < n->minVal);
}

static inline void updateCardinality(NumericRange *n, double value) {
  hll_add(&n->hll, &value, sizeof(value));
}

static inline size_t getCardinality(const NumericRange *n) {
  return hll_count(&n->hll);
}

size_t NumericRange_GetCardinality(const NumericRange *n) {
  return getCardinality(n);
}

/*
 * Add a numeric entry to the range. Returns the additional memory used for the action.
 * This function DOES NOT update the cardinality of the range.
 * It is the caller's responsibility to update the cardinality if needed, by calling `updateCardinality`
 */
static size_t NumericRange_Add(NumericRange *n, t_docId docId, double value) {

  if (value < n->minVal) n->minVal = value;
  if (value > n->maxVal) n->maxVal = value;

  size_t size = InvertedIndex_WriteNumericEntry(n->entries, docId, value);
  n->invertedIndexSize += size;
  return size;
}

/**
 * Get the median from the given index reader.
 * Getting the median this way performs good enough today (the number of records is limited),
 * but if we see performance issues in the future, we can consider using another algorithm
 * like QuickSelect or an approximation algorithm for the median.
 */
static double NumericRange_GetMedian(IndexIterator *ir) {
  size_t median_idx = ((IndexReader *)ir)->idx->numEntries / 2;
  double_heap_t *low_half = double_heap_new(median_idx);
  RSIndexResult *cur;

  // Read the first half of the values into a heap
  for (size_t i = 0; i < median_idx; i++) {
    IR_Read(ir, &cur);
    double_heap_add_raw(low_half, cur->num.value);
  }
  double_heap_heapify(low_half);

  // Read the rest of the values, replacing the max value in the heap if the current value is smaller
  while (INDEXREAD_OK == IR_Read(ir, &cur)) {
    if (cur->num.value < double_heap_peek(low_half)) {
      double_heap_replace(low_half, cur->num.value);
    }
  }

  double median = double_heap_peek(low_half);

  double_heap_free(low_half);
  IR_Rewind(ir); // Rewind iterator
  return median;
}

static inline NumericRange *NumericRange_New() {
  NumericRange *ret = rm_new(NumericRange);
  ret->entries = NewInvertedIndex(Index_StoreNumeric, 1, &ret->invertedIndexSize);
  ret->minVal = INFINITY;
  ret->maxVal = -INFINITY;
  hll_init(&ret->hll, NR_BIT_PRECISION);
  return ret;
}

static NumericRangeNode *NewLeafNode() {
  NumericRangeNode *n = rm_new(NumericRangeNode);
  n->left = NULL;
  n->right = NULL;
  n->value = 0;
  n->maxDepth = 0;
  n->range = NumericRange_New();
  return n;
}

static void NumericRangeNode_Split(NumericRangeNode *n, NRN_AddRv *rv) {
  NumericRange *r = n->range;

  n->left  = NewLeafNode();
  n->right = NewLeafNode();

  NumericRange *lr = n->left->range;
  NumericRange *rr = n->right->range;

  rv->sz += lr->invertedIndexSize + rr->invertedIndexSize;

  RSIndexResult *res = NULL;
  IndexIterator *ir = NewReadIterator(NewMinimalNumericReader(r->entries, false));
  double split = NumericRange_GetMedian(ir);
  if (split == r->minVal) {
    // make sure the split is not the same as the min value
    split = nextafter(split, INFINITY);
  }
  while (INDEXREAD_OK == IR_Read(ir, &res)) {
    NumericRange *cur = res->num.value < split ? lr : rr;
    updateCardinality(cur, res->num.value);
    rv->sz += NumericRange_Add(cur, res->docId, res->num.value);
    ++rv->numRecords;
  }
  ReadIterator_Free(ir);

  n->maxDepth = 1;
  n->value = split;
  rv->changed = 1;
  rv->numRanges += 2;
  rv->numLeaves += 1; // We split a single leaf into two, we got a single additional leaf
}

static void removeRange(NumericRangeNode *n, NRN_AddRv *rv) {
  if (!n || !n->range) {
    return;
  }

  // first change pointer to null
  NumericRange *temp = n->range;
  n->range = NULL;

  // free resources
  rv->sz -= temp->invertedIndexSize;
  rv->numRecords -= temp->entries->numEntries;
  InvertedIndex_Free(temp->entries);
  hll_destroy(&temp->hll);
  rm_free(temp);

  rv->numRanges--;
}

static void NumericRangeNode_Balance(NumericRangeNode **n) {
  NumericRangeNode *node = *n;
  // check if we need to rebalance.
  // To ease the rebalance we don't rebalance nodes that are with ranges (node->maxDepth > NR_MAX_DEPTH)
  if ((node->right->maxDepth - node->left->maxDepth) > NR_MAX_DEPTH_BALANCE) {
    // rotate to the left
    NumericRangeNode *right = node->right;
    node->right = right->left;
    right->left = node;
    node->maxDepth = MAX(node->left->maxDepth, node->right->maxDepth) + 1;
    *n = right;
  } else if ((node->left->maxDepth - node->right->maxDepth) > NR_MAX_DEPTH_BALANCE) {
    // rotate to the right
    NumericRangeNode *left = node->left;
    node->left = left->right;
    left->right = node;
    node->maxDepth = MAX(node->left->maxDepth, node->right->maxDepth) + 1;
    *n = left;
  }
  (*n)->maxDepth = MAX((*n)->left->maxDepth, (*n)->right->maxDepth) + 1;
}

static void NumericRangeNode_Add(NumericRangeNode **np, t_docId docId, double value, NRN_AddRv *rv, size_t depth) {
  NumericRangeNode *n = *np;
  if (!NumericRangeNode_IsLeaf(n)) {
    // recursively add to its left or right child.
    NumericRangeNode **childP = value < n->value ? &n->left : &n->right;
    NumericRangeNode_Add(childP, docId, value, rv, depth + 1);

    if (n->range) {
      // if this inner node retains a range, add the value to the range without
      // updating the cardinality
      rv->sz += NumericRange_Add(n->range, docId, value);
      rv->numRecords++;
    }

    if (rv->changed) {
      NumericRangeNode_Balance(np);
      n = *np; // rebalance might have changed the root
      if (n->maxDepth > RSGlobalConfig.numericTreeMaxDepthRange) {
        // we are too high up - we don't retain this node's range anymore.
        removeRange(n, rv);
      }
    }

  } else { // a leaf node

    // if this node is a leaf - we add AND check the cardinality. We only split leaf nodes
    updateCardinality(n->range, value);
    *rv = (NRN_AddRv){
      .sz = (uint32_t)NumericRange_Add(n->range, docId, value),
      .numRecords = 1,
      .changed = 0,
      .numRanges = 0,
      .numLeaves = 0,
    };

    size_t card = getCardinality(n->range);
    if (card >= getSplitCardinality(depth) ||
        (n->range->entries->numEntries > NR_MAXRANGE_SIZE && card > 1)) {

      // split this node but don't delete its range
      NumericRangeNode_Split(n, rv);

      if (n->maxDepth > RSGlobalConfig.numericTreeMaxDepthRange) {
        removeRange(n, rv);
      }
    }
  }
}

/* Recursively add a node's children to the range. */
void __recursiveAddRange(Vector *v, NumericRangeNode *n, const NumericFilter *nf, size_t *total) {
  if (!n || (nf->limit && (*total >= nf->offset + nf->limit))) return;
  double min = nf->min;
  double max = nf->max;
  if (n->range) {
    // if the range is completely contained in the search, we can just add it and not inspect any
    // downwards
    if (NumericRange_Contained(n->range, min, max)) {
      if (!nf->offset) {
        *total += n->range->entries->numDocs;
        Vector_Push(v, n->range);
      } else {
        *total += n->range->entries->numDocs;
        if (*total > nf->offset) {
          Vector_Push(v, n->range);
        }
      }
      return;
    }
    // No overlap at all - no need to do anything
    if (!NumericRange_Overlaps(n->range, min, max)) {
      return;
    }
  }

  // for non leaf nodes - we try to descend into their children.
  // we do it in direction of sorting
  if (!NumericRangeNode_IsLeaf(n)) {
    if (nf->asc) {
      if(min <= n->value) {
        __recursiveAddRange(v, n->left, nf, total);
      }
      if(max >= n->value) {
        __recursiveAddRange(v, n->right, nf, total);
      }
    } else { //descending
      if(max >= n->value) {
        __recursiveAddRange(v, n->right, nf, total);
      }
      if(min <= n->value) {
        __recursiveAddRange(v, n->left, nf, total);
      }
    }
  } else if (NumericRange_Overlaps(n->range, min, max)) {
    *total += (*total == 0 && nf->offset == 0) ? 1 : n->range->entries->numDocs;
    if (*total > nf->offset) {
      Vector_Push(v, n->range);
    }
    return;
  }
}

/* Find the numeric ranges that fit the range we are looking for. We try to minimize the number of
 * nodes we'll later need to union */
Vector *NumericRangeNode_FindRange(NumericRangeNode *n, const NumericFilter *nf) {

  Vector *leaves = NewVector(NumericRange *, 8);
  size_t total = 0;
  __recursiveAddRange(leaves, n, nf, &total);

  return leaves;
}

void NumericRangeNode_Free(NumericRangeNode *n, NRN_AddRv *rv) {
  if (!n) return;

  if (NumericRangeNode_IsLeaf(n)) rv->numLeaves--;
  removeRange(n, rv);
  NumericRangeNode_Free(n->left, rv);
  NumericRangeNode_Free(n->right, rv);

  rm_free(n);
}

uint16_t numericTreesUniqueId = 0;

/* Create a new numeric range tree */
NumericRangeTree *NewNumericRangeTree() {
  NumericRangeTree *ret = rm_malloc(sizeof(NumericRangeTree));

  ret->root = NewLeafNode();
  ret->invertedIndexesSize = ret->root->range->invertedIndexSize;
  ret->numEntries = 0;
  ret->numLeaves = 1;
  ret->numRanges = 1;
  ret->revisionId = 0;
  ret->lastDocId = 0;
  ret->emptyLeaves = 0;
  ret->uniqueId = numericTreesUniqueId++;
  return ret;
}

NRN_AddRv NumericRangeTree_Add(NumericRangeTree *t, t_docId docId, double value, int isMulti) {

  if (docId <= t->lastDocId && !isMulti) {
    // When not handling multi values - do not allow duplicate entries. This might happen due to indexer bugs and we need to protect
    // from it
    return (NRN_AddRv){0};
  }
  t->lastDocId = docId;

  NRN_AddRv rv;
  NumericRangeNode_Add(&t->root, docId, value, &rv, 0);

  // rv != 0 means the tree nodes have changed, and concurrent iteration is not allowed now
  // we increment the revision id of the tree, so currently running query iterators on it
  // will abort the next time they get execution context
  if (rv.changed) {
    t->revisionId++;
  }
  t->numRanges += rv.numRanges;
  t->numLeaves += rv.numLeaves;
  t->numEntries++;
  t->invertedIndexesSize += rv.sz;

  return rv;
}

Vector *NumericRangeTree_Find(NumericRangeTree *t, const NumericFilter *nf) {
  return NumericRangeNode_FindRange(t->root, nf);
}

void NumericRangeNode_Traverse(NumericRangeNode *n,
                               void (*callback)(NumericRangeNode *n, void *ctx), void *ctx) {

  callback(n, ctx);

  if (n->left) {
    NumericRangeNode_Traverse(n->left, callback, ctx);
  }
  if (n->right) {
    NumericRangeNode_Traverse(n->right, callback, ctx);
  }
}

#define CHILD_EMPTY 1
#define CHILD_NOT_EMPTY 0

bool NumericRangeNode_RemoveChild(NumericRangeNode **node, NRN_AddRv *rv) {
  NumericRangeNode *n = *node;
  // stop condition - we are at leaf
  if (NumericRangeNode_IsLeaf(n)) {
    if (n->range->entries->numDocs == 0) {
      return CHILD_EMPTY;
    } else {
      return CHILD_NOT_EMPTY;
    }
  }

  // run recursively on both children
  const bool rvRight = NumericRangeNode_RemoveChild(&n->right, rv);
  const bool rvLeft = NumericRangeNode_RemoveChild(&n->left, rv);

  // balance if required
  if (rvRight == CHILD_NOT_EMPTY && rvLeft == CHILD_NOT_EMPTY) {
    if (rv->changed) {
      NumericRangeNode_Balance(node);
    }
    return CHILD_NOT_EMPTY;
  }

  if (n->range && n->range->entries->numDocs != 0) {
    // We are on a non-leaf node, with some data in it but some of its children are empty.
    // Ideally we would like to trim the empty children, but today we don't fix missing ranges
    // of inner nodes, so we better keep the node as is.
    // TODO: remove this block when we fix the missing ranges issue.
    return CHILD_NOT_EMPTY;
  }

  rv->changed = 1;

  // at least one child is empty. keep an empty child and replace the parent with the other child
  if (rvRight == CHILD_EMPTY) {
    // right child is empty, save left as parent (might be empty)
    *node = n->left;
    n->left = NULL; // avoid freeing it
  } else {
    // left child is empty, save right as parent
    *node = n->right;
    n->right = NULL; // avoid freeing it
  }
  NumericRangeNode_Free(n, rv); // free the current node and its potential subtree
  return (rvRight == CHILD_NOT_EMPTY || rvLeft == CHILD_NOT_EMPTY) ? CHILD_NOT_EMPTY : CHILD_EMPTY;
}

NRN_AddRv NumericRangeTree_TrimEmptyLeaves(NumericRangeTree *t) {
  NRN_AddRv rv = {0};
  NumericRangeNode_RemoveChild(&t->root, &rv);
  if (rv.changed) {
    // Update the NumericTree
    t->revisionId++;
    t->numRanges += rv.numRanges;
    t->emptyLeaves += rv.numLeaves;
    t->numLeaves += rv.numLeaves;
    t->invertedIndexesSize += rv.sz;
  }
  return rv;
}

void NumericRangeTree_Free(NumericRangeTree *t) {
  NRN_AddRv rv = {0};
  NumericRangeNode_Free(t->root, &rv);
  rm_free(t);
}

IndexIterator *NewNumericRangeIterator(const RedisSearchCtx *sctx, NumericRange *nr,
                                       const NumericFilter *f, int skipMulti,
                                       const FieldFilterContext* filterCtx) {

  // for numeric, if this range is at either end of the filter, we need
  // to check each record.
  // for geo, we always keep the filter to check the distance
  if (NumericFilter_IsNumeric(f) &&
      NumericFilter_Match(f, nr->minVal) && NumericFilter_Match(f, nr->maxVal)) {
    // make the filter NULL so the reader will ignore it
    f = NULL;
  }
  IndexReader *ir = NewNumericReader(sctx, nr->entries, f, nr->minVal, nr->maxVal, skipMulti, filterCtx);

  return NewReadIterator(ir);
}

/* Create a union iterator from the numeric filter, over all the sub-ranges in the tree that fit
 * the filter */
IndexIterator *createNumericIterator(const RedisSearchCtx *sctx, NumericRangeTree *t,
                                     const NumericFilter *f, IteratorsConfig *config,
                                     const FieldFilterContext* filterCtx) {

  Vector *v = NumericRangeTree_Find(t, f);
  if (!v || Vector_Size(v) == 0) {
    if (v) {
      Vector_Free(v);
    }
    return NULL;
  }

  int n = Vector_Size(v);
  // if we only selected one range - we can just iterate it without union or anything
  if (n == 1) {
    NumericRange *rng;
    Vector_Get(v, 0, &rng);
    IndexIterator *it = NewNumericRangeIterator(sctx, rng, f, true, filterCtx);
    Vector_Free(v);
    return it;
  }

  // We create a  union iterator, advancing a union on all the selected range,
  // treating them as one consecutive range
  IndexIterator **its = rm_calloc(n, sizeof(IndexIterator *));

  for (size_t i = 0; i < n; i++) {
    NumericRange *rng;
    Vector_Get(v, i, &rng);
    if (!rng) {
      continue;
    }

    its[i] = NewNumericRangeIterator(sctx, rng, f, true, filterCtx);
  }
  Vector_Free(v);

  QueryNodeType type = (!f || NumericFilter_IsNumeric(f)) ? QN_NUMERIC : QN_GEO;
  IndexIterator *it = NewUnionIterator(its, n, 1, 1, type, NULL, config);

  return it;
}

RedisModuleType *NumericIndexType = NULL;
#define NUMERICINDEX_KEY_FMT "nm:%s/%s"

RedisModuleString *fmtRedisNumericIndexKey(const RedisSearchCtx *ctx, const char *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, NUMERICINDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

NumericRangeTree *openNumericKeysDict(IndexSpec* spec, RedisModuleString *keyName,
                                             bool create_if_missing) {
  KeysDictValue *kdv = dictFetchValue(spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!create_if_missing) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->dtor = (void (*)(void *))NumericRangeTree_Free;
  kdv->p = NewNumericRangeTree();
  spec->stats.invertedSize += ((NumericRangeTree *)kdv->p)->root->range->invertedIndexSize;
  dictAdd(spec->keysDict, keyName, kdv);
  return kdv->p;
}

IndexIterator *NewNumericFilterIterator(const RedisSearchCtx *ctx, const NumericFilter *flt,
                                        ConcurrentSearchCtx *csx, FieldType forType, IteratorsConfig *config,
                                        const FieldFilterContext* filterCtx) {
  RedisModuleString *s = IndexSpec_GetFormattedKey(ctx->spec, flt->field, forType);
  if (!s) {
    return NULL;
  }
  RedisModuleKey *key = NULL;
  NumericRangeTree *t = NULL;
  if (!ctx->spec->keysDict) {
    key = RedisModule_OpenKey(ctx->redisCtx, s, REDISMODULE_READ);
    if (!key || RedisModule_ModuleTypeGetType(key) != NumericIndexType) {
      return NULL;
    }

    t = RedisModule_ModuleTypeGetValue(key);
  } else {
    t = openNumericKeysDict(ctx->spec, s, DONT_CREATE_INDEX);
  }

  if (!t) {
    return NULL;
  }

  IndexIterator *it = createNumericIterator(ctx, t, flt, config, filterCtx);
  if (!it) {
    return NULL;
  }

  if (csx) {
    NumericUnionCtx *uc = rm_malloc(sizeof(*uc));
    uc->lastRevId = t->revisionId;
    uc->it = it;
    uc->sp = ctx->spec;
    uc->field = flt->field;
    ConcurrentSearch_AddKey(csx, NumericRangeIterator_OnReopen, uc, rm_free);
  }
  return it;
}

static inline size_t NumericRangeNode_sizeof() {
  return sizeof(NumericRangeNode);
}

static inline size_t NumericRange_sizeof() {
  size_t size = sizeof(NumericRange);
  size += NR_REG_SIZE; // hll memory size
  return size;
}

unsigned long NumericIndexType_MemUsage(const void *value) {
  const NumericRangeTree *t = value;
  unsigned long ret = sizeof(NumericRangeTree);
  ret += t->invertedIndexesSize;
  ret += t->numRanges * NumericRange_sizeof();
  // Our tree is a full binary tree, so `#nodes = 2 * #leaves - 1`
  ret += (2 * t->numLeaves - 1) * NumericRangeNode_sizeof();
  return ret;
}

#define NUMERIC_INDEX_ENCVER 1

int NumericIndexType_Register(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = NumericIndexType_RdbLoad,
                               .rdb_save = NumericIndexType_RdbSave,
                               .aof_rewrite = GenericAofRewrite_DisabledHandler,
                               .free = NumericIndexType_Free,
                               .mem_usage = NumericIndexType_MemUsage};

  NumericIndexType = RedisModule_CreateDataType(ctx, "numericdx", NUMERIC_INDEX_ENCVER, &tm);
  if (NumericIndexType == NULL) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

/* A single entry in a numeric index's single range. Since entries are binned together, each needs
 * to have the exact value */
typedef struct {
  t_docId docId;
  double value;
} NumericRangeEntry;

static int cmpdocId(const void *p1, const void *p2) {
  NumericRangeEntry *e1 = (NumericRangeEntry *)p1;
  NumericRangeEntry *e2 = (NumericRangeEntry *)p2;

  return (int)e1->docId - (int)e2->docId;
}

/** Version 0 stores the number of entries beforehand, and then loads them */
static size_t loadV0(RedisModuleIO *rdb, NumericRangeEntry **entriespp) {
  uint64_t num = RedisModule_LoadUnsigned(rdb);
  if (!num) {
    return 0;
  }

  *entriespp = array_newlen(NumericRangeEntry, num);
  NumericRangeEntry *entries = *entriespp;
  for (size_t ii = 0; ii < num; ++ii) {
    entries[ii].docId = RedisModule_LoadUnsigned(rdb);
    entries[ii].value = RedisModule_LoadDouble(rdb);
  }
  return num;
}

#define NUMERIC_IDX_INITIAL_LOAD_SIZE 1 << 16
/** Version 0 stores (id,value) pairs, with a final 0 as a terminator */
static size_t loadV1(RedisModuleIO *rdb, NumericRangeEntry **entriespp) {
  NumericRangeEntry *entries = array_new(NumericRangeEntry, NUMERIC_IDX_INITIAL_LOAD_SIZE);
  while (1) {
    NumericRangeEntry cur;
    cur.docId = RedisModule_LoadUnsigned(rdb);
    if (!cur.docId) {
      break;
    }
    cur.value = RedisModule_LoadDouble(rdb);
    array_append(entries, cur);
  }
  *entriespp = entries;
  return array_len(entries);
}

void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver > NUMERIC_INDEX_ENCVER) {
    return NULL;
  }

  NumericRangeEntry *entries = NULL;
  size_t numEntries = 0;
  if (encver == 0) {
    numEntries = loadV0(rdb, &entries);
  } else if (encver == 1) {
    numEntries = loadV1(rdb, &entries);
  } else {
    return NULL;  // Unknown version
  }

  // sort the entries by doc id, as they were not saved in this order
  qsort(entries, numEntries, sizeof(NumericRangeEntry), cmpdocId);
  NumericRangeTree *t = NewNumericRangeTree();

  // now push them in order into the tree
  for (size_t i = 0; i < numEntries; i++) {
    NumericRangeTree_Add(t, entries[i].docId, entries[i].value, true);
  }
  array_free(entries);
  return t;
}

struct niRdbSaveCtx {
  RedisModuleIO *rdb;
};

static void numericIndex_rdbSaveCallback(NumericRangeNode *n, void *ctx) {
  struct niRdbSaveCtx *rctx = ctx;

  if (NumericRangeNode_IsLeaf(n) && n->range) {
    NumericRange *rng = n->range;
    RSIndexResult *res = NULL;
    IndexIterator *ir = NewReadIterator(NewMinimalNumericReader(rng->entries, false));

    while (INDEXREAD_OK == IR_Read(ir, &res)) {
      RedisModule_SaveUnsigned(rctx->rdb, res->docId);
      RedisModule_SaveDouble(rctx->rdb, res->num.value);
    }
    ReadIterator_Free(ir);
  }
}
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value) {

  NumericRangeTree *t = value;
  struct niRdbSaveCtx ctx = {rdb};

  NumericRangeNode_Traverse(t->root, numericIndex_rdbSaveCallback, &ctx);
  // Save the final record
  RedisModule_SaveUnsigned(rdb, 0);
}

void NumericIndexType_Digest(RedisModuleDigest *digest, void *value) {
}

void NumericIndexType_Free(void *value) {
  NumericRangeTree *t = value;
  NumericRangeTree_Free(t);
}

NumericRangeTreeIterator *NumericRangeTreeIterator_New(NumericRangeTree *t) {
#define NODE_STACK_INITIAL_SIZE 4
  NumericRangeTreeIterator *iter = rm_malloc(sizeof(NumericRangeTreeIterator));
  iter->nodesStack = array_new(NumericRangeNode *, NODE_STACK_INITIAL_SIZE);
  array_append(iter->nodesStack, t->root);
  return iter;
}

NumericRangeNode *NumericRangeTreeIterator_Next(NumericRangeTreeIterator *iter) {
  if (array_len(iter->nodesStack) == 0) {
    return NULL;
  }
  NumericRangeNode *ret = array_pop(iter->nodesStack);
  if (!NumericRangeNode_IsLeaf(ret)) {
    array_append(iter->nodesStack, ret->left);
    array_append(iter->nodesStack, ret->right);
  }

  return ret;
}

void NumericRangeTreeIterator_Free(NumericRangeTreeIterator *iter) {
  array_free(iter->nodesStack);
  rm_free(iter);
}

/* A callback called after a concurrent context regains execution context. When this happen we need
 * to make sure the key hasn't been deleted or its structure changed, which will render the
 * underlying iterators invalid */
void NumericRangeIterator_OnReopen(void *privdata) {
  NumericUnionCtx *nu = privdata;
  IndexSpec *sp = nu->sp;
  IndexIterator *it = nu->it;

  RedisModuleString *numField = IndexSpec_GetFormattedKey(sp, nu->field, INDEXFLD_T_NUMERIC);
  NumericRangeTree *rt = openNumericKeysDict(sp, numField, DONT_CREATE_INDEX);

  if (!rt || rt->revisionId != nu->lastRevId) {
    // The numeric tree was either completely deleted or a node was splitted or removed.
    // The cursor is invalidated.
    it->Abort(it);
    return;
  }

  if (it->type == READ_ITERATOR) {
    IndexReader_OnReopen((IndexReader *)it);
  } else if (it->type == UNION_ITERATOR) {
    UI_Foreach(it, IndexReader_OnReopen);
  } else {
    RS_LOG_ASSERT_FMT(0,
      "Unexpected iterator type %d. Expected `READ_ITERATOR` (%d) or `UNION_ITERATOR` (%d)",
      it->type, READ_ITERATOR, UNION_ITERATOR);
  }
}
