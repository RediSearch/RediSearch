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
//#include "tests/time_sample.h"
#define NR_EXPONENT 4
#define NR_MAXRANGE_CARD 2500
#define NR_MAXRANGE_SIZE 10000

typedef struct {
  IndexIterator *it;
  uint32_t lastRevId;
  IndexSpec *sp;
  const char *fieldName;
} NumericUnionCtx;

void NumericRangeIterator_OnReopen(void *privdata);

#ifdef _DEBUG
void NumericRangeTree_Dump(NumericRangeTree *t, int indent) {
  PRINT_INDENT(indent);
  printf("NumericRangeTree {\n");
  ++indent;

  PRINT_INDENT(indent);
  printf("numEntries %lu,  numRanges %lu, lastDocId %lu\n", t->numEntries, t->numRanges, t->lastDocId);
  NumericRangeNode_Dump(t->root, indent + 1);

  --indent;
  PRINT_INDENT(indent);
  printf("}\n");
}
void NumericRangeNode_Dump(NumericRangeNode *n, int indent) {
  PRINT_INDENT(indent);
  printf("NumericRangeNode {\n");
  ++indent;

  PRINT_INDENT(indent);
  printf("value %f, maxDepath %i\n", n->value, n->maxDepth);

  if (n->range) {
    PRINT_INDENT(indent);
    printf("range:\n");
    NumericRange_Dump(n->range, indent + 1);
  }

  if (n->left) {
    PRINT_INDENT(indent);
    printf("left:\n");
    NumericRangeNode_Dump(n->left, indent + 1);
  }
  if (n->right) {
    PRINT_INDENT(indent);
    printf("right:\n");
    NumericRangeNode_Dump(n->right, indent + 1);
  }

  --indent;
  PRINT_INDENT(indent);
  printf("}\n");
}

void NumericRange_Dump(NumericRange *r, int indent) {
  PRINT_INDENT(indent);
  printf("NumericRange {\n");
  ++indent;
  PRINT_INDENT(indent);
  printf("minVal %f, maxVal %f, unique_sum %f, invertedIndexSize %zu, card %hu, cardCheck %hu, splitCard %u\n", r->minVal, r->maxVal, r->unique_sum, r->invertedIndexSize, r->card, r->cardCheck, r->splitCard);
  InvertedIndex_Dump(r->entries, indent + 1);
  --indent;
  PRINT_INDENT(indent);
  printf("}\n");
}

#endif // #ifdef _DEBUG

/* Returns 1 if the entire numeric range is contained between min and max */
static inline int NumericRange_Contained(NumericRange *n, double min, double max) {
  if (!n) return 0;
  int rc = (n->minVal >= min && n->maxVal <= max);

  return rc;
}

/* Returns 1 if min and max are both inside the range. this is the opposite of _Within */
static inline int NumericRange_Contains(NumericRange *n, double min, double max) {
  if (!n) return 0;
  int rc = (n->minVal <= min && n->maxVal > max);

  return rc;
}

/* Returns 1 if there is any overlap between the range and min/max */
int NumericRange_Overlaps(NumericRange *n, double min, double max) {
  if (!n) return 0;
  int rc = (min >= n->minVal && min <= n->maxVal) || (max >= n->minVal && max <= n->maxVal);

  return rc;
}

static inline void checkCardinality(NumericRange *n, double value) {
  // skip
  if (--n->cardCheck != 0) {
    return;
  }
  n->cardCheck = NR_CARD_CHECK;

  // check if value exists and increase appearance
  uint32_t arrlen = array_len(n->values);
  for (int i = 0; i < arrlen; i++) {
    if (n->values[i].value == value) {
      n->values[i].appearances++;
      return;
    }
  }

  // add new value to cardinality values
  CardinalityValue val = {.value = value, .appearances = 1};
  array_append(n->values, val);
  n->unique_sum += value;
  ++n->card;
}

size_t NumericRange_Add(NumericRange *n, t_docId docId, double value, int checkCard) {
  int add = 0;
  if (checkCard) {
    checkCardinality(n, value);
  }

  if (value < n->minVal) n->minVal = value;
  if (value > n->maxVal) n->maxVal = value;

  size_t size = InvertedIndex_WriteNumericEntry(n->entries, docId, value);
  n->invertedIndexSize += size;
  return size;
}

double NumericRange_Split(NumericRange *n, NumericRangeNode **lp, NumericRangeNode **rp,
                          NRN_AddRv *rv) {

  double split = (n->unique_sum) / (double)n->card;

  *lp = NewLeafNode(n->entries->numDocs / 2 + 1,
                    MIN(NR_MAXRANGE_CARD, 1 + n->splitCard * NR_EXPONENT));
  *rp = NewLeafNode(n->entries->numDocs / 2 + 1,
                    MIN(NR_MAXRANGE_CARD, 1 + n->splitCard * NR_EXPONENT));
  rv->sz += (*lp)->range->invertedIndexSize + (*rp)->range->invertedIndexSize;

  RSIndexResult *res = NULL;
  IndexReader *ir = NewMinimalNumericReader(n->entries, false);
  while (INDEXREAD_OK == IR_Read(ir, &res)) {
    rv->sz += NumericRange_Add(res->num.value < split ? (*lp)->range : (*rp)->range, res->docId,
                               res->num.value, 1);
    ++rv->numRecords;
  }
  IR_Free(ir);

  return split;
}

NumericRangeNode *NewLeafNode(size_t cap, size_t splitCard) {

  NumericRangeNode *n = rm_malloc(sizeof(NumericRangeNode));
  n->left = NULL;
  n->right = NULL;
  n->value = 0;

  n->maxDepth = 0;
  n->range = rm_malloc(sizeof(NumericRange));
  size_t index_memsize;

  *n->range = (NumericRange){
      .minVal = __DBL_MAX__,
      .maxVal = NF_NEGATIVE_INFINITY,
      .unique_sum = 0,
      .card = 0,
      .cardCheck = NR_CARD_CHECK,
      .splitCard = splitCard,
      .values = array_new(CardinalityValue, 1),
      //.values = rm_calloc(splitCard, sizeof(CardinalityValue)),
      .entries = NewInvertedIndex(Index_StoreNumeric, 1, &index_memsize),
  };

  n->range->invertedIndexSize = index_memsize;
  return n;
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
  array_free(temp->values);
  rm_free(temp);

  rv->numRanges--;
}

static void NumericRangeNode_Balance(NumericRangeNode **n) {
  NumericRangeNode *node = *n;
  node->maxDepth = MAX(node->right->maxDepth, node->left->maxDepth) + 1;
  // check if we need to rebalance the child.
  // To ease the rebalance we don't rebalance the root
  // nor do we rebalance nodes that are with ranges (node->maxDepth > NR_MAX_DEPTH)
  if ((node->right->maxDepth - node->left->maxDepth) > NR_MAX_DEPTH_BALANCE) {  // role to the left
    NumericRangeNode *right = node->right;
    node->right = right->left;
    right->left = node;
    --node->maxDepth;
    *n = right;
  } else if ((node->left->maxDepth - node->right->maxDepth) >
              NR_MAX_DEPTH_BALANCE) {  // role to the right
    NumericRangeNode *left = node->left;
    node->left = left->right;
    left->right = node;
    --node->maxDepth;
    *n = left;
  }
}

NRN_AddRv NumericRangeNode_Add(NumericRangeNode *n, t_docId docId, double value) {
  NRN_AddRv rv = {.sz = 0, .changed = 0, .numRecords = 0, .numRanges = 0};
  if (!NumericRangeNode_IsLeaf(n)) {
    // if this node has already split but retains a range, just add to the range without checking
    // anything
    size_t s = 0;
    size_t nRecords = 0;
    if (n->range) {
      s += NumericRange_Add(n->range, docId, value, 0);
      ++nRecords;
    }

    // recursively add to its left or right child.
    NumericRangeNode **childP = value < n->value ? &n->left : &n->right;
    NumericRangeNode *child = *childP;
    // if the child has split we get 1 in return
    rv = NumericRangeNode_Add(child, docId, value);
    rv.sz += s;
    rv.numRecords += nRecords;

    if (rv.changed) {
      // if there was a split it means our max depth has increased.
      // we are too deep - we don't retain this node's range anymore.
      // this keeps memory footprint in check
      if (++n->maxDepth > RSGlobalConfig.numericTreeMaxDepthRange && n->range) {
        removeRange(n, &rv);
      }

      NumericRangeNode_Balance(childP);
    }
    // return 1 or 0 to our called, so this is done recursively
    return rv;
  }

  // if this node is a leaf - we add AND check the cardinality. We only split leaf nodes
  rv.sz = (uint32_t)NumericRange_Add(n->range, docId, value, 1);
  ++rv.numRecords;
  int card = n->range->card;

  if (card * NR_CARD_CHECK >= n->range->splitCard ||
      (n->range->entries->numEntries > NR_MAXRANGE_SIZE && card > 1)) {

    // split this node but don't delete its range
    double split = NumericRange_Split(n->range, &n->left, &n->right, &rv);
    rv.numRanges += 2;
    if (RSGlobalConfig.numericTreeMaxDepthRange == 0) {
      removeRange(n, &rv);
    }
    n->value = split;
    n->maxDepth = 1;
    rv.changed = 1;
  }

  return rv;
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

int NumericRangeTree_DeleteNode(NumericRangeTree *t, double value) {
  // TODO:
  return 0;
}

/* Find the numeric ranges that fit the range we are looking for. We try to minimize the number of
 * nodes we'll later need to union */
Vector *NumericRangeNode_FindRange(NumericRangeNode *n, const NumericFilter *nf) {

  Vector *leaves = NewVector(NumericRange *, 8);
  size_t total = 0;
  __recursiveAddRange(leaves, n, nf, &total);
  // printf("Found %zd ranges for %f...%f\n", leaves->top, min, max);
  // for (int i = 0; i < leaves->top; i++) {
  //   NumericRange *rng;
  //   Vector_Get(leaves, i, &rng);
  //   printf("%f...%f (%f). %d card, %d splitCard\n", rng->minVal, rng->maxVal,
  //          rng->maxVal - rng->minVal, rng->entries->numDocs, rng->splitCard);
  // }

  return leaves;
}

void NumericRangeNode_Free(NumericRangeNode *n, NRN_AddRv *rv) {
  if (!n) return;
  if (n->range) {
    rv->sz -= n->range->invertedIndexSize;
    InvertedIndex_Free(n->range->entries);
    array_free(n->range->values);
    rm_free(n->range);
    n->range = NULL;
    rv->numRanges--;
  }

  NumericRangeNode_Free(n->left, rv);
  NumericRangeNode_Free(n->right, rv);

  rm_free(n);
}

uint16_t numericTreesUniqueId = 0;

/* Create a new numeric range tree */
NumericRangeTree *NewNumericRangeTree() {
  NumericRangeTree *ret = rm_malloc(sizeof(NumericRangeTree));

  // updated value since splitCard should be >NR_CARD_CHECK
  ret->root = NewLeafNode(2, 16);
  ret->numEntries = 0;
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
    return (NRN_AddRv){0, 0, 0, 0};
  }
  t->lastDocId = docId;

  NumericRangeNode* root = t->root;

  NRN_AddRv rv = NumericRangeNode_Add(root, docId, value);

  // Since we never rebalance the root, we don't update its max depth.
  if (!NumericRangeNode_IsLeaf(root)) {
    root->maxDepth = MAX(root->right->maxDepth, root->left->maxDepth) + 1;
  }

  // rv != 0 means the tree nodes have changed, and concurrent iteration is not allowed now
  // we increment the revision id of the tree, so currently running query iterators on it
  // will abort the next time they get execution context
  if (rv.changed) {
    t->revisionId++;
  }
  t->numRanges += rv.numRanges;
  t->numEntries++;

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

int NumericRangeNode_RemoveChild(NumericRangeNode **node, NRN_AddRv *rv) {
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
  int rvRight = NumericRangeNode_RemoveChild(&n->right, rv);
  int rvLeft = NumericRangeNode_RemoveChild(&n->left, rv);
  NumericRangeNode *rightChild = n->right;
  NumericRangeNode *leftChild = n->left;

  // balance if required
  if (rvRight == CHILD_NOT_EMPTY && rvLeft == CHILD_NOT_EMPTY) {
    if (rv->changed) {
      NumericRangeNode_Balance(node);
    }
    return CHILD_NOT_EMPTY;
  }

  rv->changed = 1;

  // we can remove local and use child's instead
  if (n->range) {
    if (n->range->entries->numDocs != 0) {
      return CHILD_NOT_EMPTY;
    }
    removeRange(n, rv);
  }

  // both children are empty, save one as parent
  if (rvRight == CHILD_EMPTY && rvLeft == CHILD_EMPTY) {
    rm_free(n);
    *node = rightChild;
    NumericRangeNode_Free(leftChild, rv);

    return CHILD_EMPTY;
  }

  // one child is not empty, save copy as parent and free
  if (rvRight == CHILD_EMPTY) {
    // right child is empty, save left as parent
    rm_free(n);
    *node = leftChild;
    NumericRangeNode_Free(rightChild, rv);
  } else {
    // left child is empty, save right as parent
    rm_free(n);
    *node = rightChild;
    NumericRangeNode_Free(leftChild, rv);
  }
  return CHILD_NOT_EMPTY;
}

NRN_AddRv NumericRangeTree_TrimEmptyLeaves(NumericRangeTree *t) {
  NRN_AddRv rv = {.sz = 0, .changed = 0, .numRecords = 0, .numRanges = 0};
  NumericRangeNode_RemoveChild(&t->root, &rv);
  if (rv.changed) {
    // Update the NumericTree
    t->revisionId++;
    t->numRanges += rv.numRanges;
    t->emptyLeaves = 0;
  }
  return rv;
}

void NumericRangeTree_Free(NumericRangeTree *t) {
  NRN_AddRv rv = {.sz = 0, .changed = 0, .numRecords = 0, .numRanges = 0};
  NumericRangeNode_Free(t->root, &rv);
  rm_free(t);
}

IndexIterator *NewNumericRangeIterator(const IndexSpec *sp, NumericRange *nr,
                                       const NumericFilter *f, int skipMulti,
                                       const FieldIndexFilterContext* filterCtx) {

  // for numeric, if this range is at either end of the filter, we need
  // to check each record.
  // for geo, we always keep the filter to check the distance
  if (NumericFilter_IsNumeric(f) &&
      NumericFilter_Match(f, nr->minVal) && NumericFilter_Match(f, nr->maxVal)) {
    // make the filter NULL so the reader will ignore it
    f = NULL;
  }
  IndexReader *ir = NewNumericReader(sp, nr->entries, f, nr->minVal, nr->maxVal, skipMulti, filterCtx);

  return NewReadIterator(ir);
}

/* Create a union iterator from the numeric filter, over all the sub-ranges in the tree that fit
 * the filter */
IndexIterator *createNumericIterator(const IndexSpec *sp, NumericRangeTree *t,
                                     const NumericFilter *f, IteratorsConfig *config,
                                     const FieldIndexFilterContext* filterCtx) {

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
    IndexIterator *it = NewNumericRangeIterator(sp, rng, f, true, filterCtx);
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

    its[i] = NewNumericRangeIterator(sp, rng, f, true, filterCtx);
  }
  Vector_Free(v);

  QueryNodeType type = (!f || NumericFilter_IsNumeric(f)) ? QN_NUMERIC : QN_GEO;
  IndexIterator *it = NewUnionIterator(its, n, 1, 1, type, NULL, config);

  return it;
}

RedisModuleType *NumericIndexType = NULL;
#define NUMERICINDEX_KEY_FMT "nm:%s/%s"

RedisModuleString *fmtRedisNumericIndexKey(RedisSearchCtx *ctx, const char *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, NUMERICINDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

static NumericRangeTree *openNumericKeysDict(IndexSpec* spec, RedisModuleString *keyName,
                                             int write) {
  KeysDictValue *kdv = dictFetchValue(spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->dtor = (void (*)(void *))NumericRangeTree_Free;
  kdv->p = NewNumericRangeTree();
  spec->stats.invertedSize += ((NumericRangeTree *)kdv->p)->root->range->invertedIndexSize;
  dictAdd(spec->keysDict, keyName, kdv);
  return kdv->p;
}

struct indexIterator *NewNumericFilterIterator(RedisSearchCtx *ctx, const NumericFilter *flt,
                                               ConcurrentSearchCtx *csx, FieldType forType, IteratorsConfig *config,
                                               const FieldIndexFilterContext* filterCtx) {
  RedisModuleString *s = IndexSpec_GetFormattedKeyByName(ctx->spec, flt->fieldName, forType);
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
    t = openNumericKeysDict(ctx->spec, s, 0);
  }

  if (!t) {
    return NULL;
  }

  IndexIterator *it = createNumericIterator(ctx->spec, t, flt, config, filterCtx);
  if (!it) {
    return NULL;
  }

  if (csx) {
    NumericUnionCtx *uc = rm_malloc(sizeof(*uc));
    uc->lastRevId = t->revisionId;
    uc->it = it;
    uc->sp = ctx->spec;
    uc->fieldName = flt->fieldName;
    ConcurrentSearch_AddKey(csx, NumericRangeIterator_OnReopen, uc, rm_free);
  }
  return it;
}

NumericRangeTree *OpenNumericIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey) {
  return openNumericKeysDict(ctx->spec, keyName, 1);
}

void __numericIndex_memUsageCallback(NumericRangeNode *n, void *ctx) {
  unsigned long *sz = ctx;
  *sz += sizeof(NumericRangeNode);

  if (n->range) {
    *sz += sizeof(NumericRange);
    *sz += n->range->card * sizeof(double);
    if (n->range->entries) {
      *sz += InvertedIndex_MemUsage(n->range->entries);
    }
  }
}

unsigned long NumericIndexType_MemUsage(const void *value) {
  const NumericRangeTree *t = value;
  unsigned long ret = sizeof(NumericRangeTree);
  NumericRangeNode_Traverse(t->root, __numericIndex_memUsageCallback, &ret);
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
    IndexReader *ir = NewMinimalNumericReader(rng->entries, false);

    while (INDEXREAD_OK == IR_Read(ir, &res)) {
      RedisModule_SaveUnsigned(rctx->rdb, res->docId);
      RedisModule_SaveDouble(rctx->rdb, res->num.value);
    }
    IR_Free(ir);
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

  RedisModuleString *numField = IndexSpec_GetFormattedKeyByName(sp, nu->fieldName, INDEXFLD_T_NUMERIC);
  NumericRangeTree *rt = openNumericKeysDict(sp, numField, 0);

  if (!rt || rt->revisionId != nu->lastRevId) {
    // The numeric tree was either completely deleted or a node was splitted or removed.
    // The cursor is invalidated.
    it->Abort(it->ctx);
    return;
  }

  if (it->type == READ_ITERATOR) {
    IndexReader_OnReopen(it->ctx);
  } else if (it->type == UNION_ITERATOR) {
    UI_Foreach(it, IndexReader_OnReopen);
  } else {
    RS_LOG_ASSERT_FMT(0,
      "Unexpected iterator type %d. Expected `READ_ITERATOR` (%d) or `UNION_ITERATOR` (%d)",
      it->type, READ_ITERATOR, UNION_ITERATOR);
  }
}
