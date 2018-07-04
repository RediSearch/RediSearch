#include "numeric_index.h"
#include "redis_index.h"
#include "sys/param.h"
#include "rmutil/vector.h"
#include "rmutil/util.h"
#include "index.h"
#include <math.h>
#include <float.h>
#include "redismodule.h"
#include "util/misc.h"
//#include "tests/time_sample.h"
#define NR_EXPONENT 4
#define NR_MAXRANGE_CARD 2500
#define NR_MAXRANGE_SIZE 10000
#define NR_MAX_DEPTH 2

typedef struct {
  IndexIterator *it;
  uint32_t lastRevId;
} NumericUnionCtx;

/* A callback called after a concurrent context regains execution context. When this happen we need
 * to make sure the key hasn't been deleted or its structure changed, which will render the
 * underlying iterators invalid */
void NumericRangeIterator_OnReopen(RedisModuleKey *k, void *privdata) {
  NumericUnionCtx *nu = privdata;
  NumericRangeTree *t = RedisModule_ModuleTypeGetValue(k);

  /* If the key has been deleted we'll get a NULL heere, so we just mark ourselves as EOF
   * We simply abort the root iterator which is either a union of many ranges or a single range
   *
   * If the numeric range tree has chained (split, nodes deleted, etc) since we last closed it,
   * We cannot continue iterating it, since the underlying pointers might be screwed.
   * For now we will just stop processing this query. This causes the query to return bad results,
   * so in the future we can try an reset the state here
   */
  if (k == NULL || t == NULL || t->revisionId != nu->lastRevId) {
    nu->it->Abort(nu->it->ctx);
  }
}

/* Returns 1 if the entire numeric range is contained between min and max */
static inline int NumericRange_Contained(NumericRange *n, double min, double max) {
  if (!n) return 0;
  int rc = (n->minVal >= min && n->maxVal <= max);

  // printf("range %f..%f, min %f max %f, WITHIN? %d\n", n->minVal, n->maxVal, min, max, rc);
  return rc;
}

/* Returns 1 if min and max are both inside the range. this is the opposite of _Within */
static inline int NumericRange_Contains(NumericRange *n, double min, double max) {
  if (!n) return 0;
  int rc = (n->minVal <= min && n->maxVal > max);
  // printf("range %f..%f, min %f max %f, contains? %d\n", n->minVal, n->maxVal, min, max, rc);
  return rc;
}

/* Returns 1 if there is any overlap between the range and min/max */
int NumericRange_Overlaps(NumericRange *n, double min, double max) {
  if (!n) return 0;
  int rc = (min >= n->minVal && min <= n->maxVal) || (max >= n->minVal && max <= n->maxVal);
  // printf("range %f..%f, min %f max %f, overlaps? %d\n", n->minVal, n->maxVal, min, max, rc);
  return rc;
}

static int NumericRange_AddNoCheck(NumericRange *n, t_docId docId, double value) {
  if (n->minVal == NF_NEGATIVE_INFINITY || value < n->minVal) {
    n->minVal = value;
  }
  if (n->maxVal == NF_INFINITY || value > n->maxVal) {
    n->maxVal = value;
  }

  n->numEntries++;
  InvertedIndex_WriteNumericEntry(n->entries, docId, value);
  return NumericRange_GetCardinality(n);
}

typedef union {
  uint64_t u64;
  double d;
} HashKey;

static int NumericRange_AddValue(NumericRange *n, t_docId docId, double value) {
  int status;
  HashKey k = {.d = value};
  khint_t iter = kh_put(rangeVals, n->htvals, k.u64, &status);
  if (status > 0) {
    // Newly inserted
    n->unique_sum += value;
    // printf("%p: Put %lf. Status=%d. card=%u, sum=%lf\n", n, value, status,
    //        NumericRange_GetCardinality(n), n->unique_sum);

    kh_val(n->htvals, iter) = 0;  // Incremented later on
  } else if (status < 0) {
    fprintf(stderr, "Failed to write value!\n");
    abort();
  } else if (status == 0) {
    // printf("Item was already present...\n");
  }

  kh_val(n->htvals, iter)++;

  return NumericRange_AddNoCheck(n, docId, value);
}

int NumericRange_RemoveEntry(NumericRange *n, double value) {
  HashKey k = {.d = value};
  // Get the entry
  khint_t iter = kh_get(rangeVals, n->htvals, k.u64);
  if (iter == kh_end(n->htvals)) {
    assert(0 && "Value is supposed to have been deleted, but is still here???");
    return NR_DELETE_NOTFOUND;
  }

  // printf("Removing %lf\n", value);

  n->numEntries--;
  if (--kh_val(n->htvals, iter)) {
    return NR_DELETE_REMAINING;
  }

  // The value must be removed. If so, we need to check if it was a min/max,
  // and if it was, reset it.
  kh_del(rangeVals, n->htvals, iter);
  n->unique_sum -= value;

  double newMin = DBL_MAX, newMax = DBL_MIN;
  if (value == n->maxVal || value == n->minVal) {
    uint64_t k64;
    uint16_t v;

    kh_foreach(n->htvals, k64, v, {
      k.u64 = k64;
      newMin = MIN(k.d, newMin);
      newMax = MAX(k.d, newMax);
    });
    n->minVal = newMin;
    n->maxVal = newMax;
  }

  return NR_DELETE_REMOVED;
}

void NumericRangeNode_RemoveEntry(NumericRangeNode *n, double value) {
  if (n->range && NumericRangeNode_IsLeaf(n)) {
    NumericRange_RemoveEntry(n->range, value);
  }
  if (value < n->value) {
    if (n->left) {
      NumericRangeNode_RemoveEntry(n->left, value);
    }
  } else {
    if (n->right) {
      NumericRangeNode_RemoveEntry(n->right, value);
    }
  }
}

void NumericRangeTree_RemoveValue(NumericRangeTree *t, double value) {
  NumericRangeNode_RemoveEntry(t->root, value);
}

double NumericRange_Split(NumericRange *n, NumericRangeNode **lp, NumericRangeNode **rp) {

  double split = (n->unique_sum) / (double)NumericRange_GetCardinality(n);

  printf("split point :%f\n", split);
  size_t splitCard = MIN(NR_MAXRANGE_CARD, 1 + n->splitCard * NR_EXPONENT);
  size_t docCount = NumericRange_GetDocCount(n);
  *lp = NewLeafNode(docCount / 2 + 1, n->minVal, split, splitCard);
  *rp = NewLeafNode(docCount / 2 + 1, split, n->maxVal, splitCard);

  RSIndexResult *res = NULL;
  IndexReader *ir = NewNumericReader(n->entries, NULL);
  while (INDEXREAD_OK == IR_Read(ir, &res)) {
    NumericRange_AddValue(res->num.value < split ? (*lp)->range : (*rp)->range, res->docId,
                          res->num.value);
  }
  IR_Free(ir);

  printf("Splitting node %p %f..%f, card %d size %d\n", n, n->minVal, n->maxVal,
         NumericRange_GetCardinality(n), NumericRange_GetDocCount(n));

  printf("left node: %d, right: %d\n", NumericRange_GetDocCount((*lp)->range),
         NumericRange_GetDocCount((*rp)->range));
  return split;
}

NumericRangeNode *NewLeafNode(size_t cap, double min, double max, size_t splitCard) {

  NumericRangeNode *n = RedisModule_Alloc(sizeof(NumericRangeNode));
  n->left = NULL;
  n->right = NULL;
  n->value = 0;

  n->maxDepth = 0;
  n->range = RedisModule_Alloc(sizeof(NumericRange));

  *n->range = (NumericRange){.minVal = min,
                             .maxVal = max,
                             .unique_sum = 0,
                             .splitCard = splitCard,
                             .htvals = kh_init(rangeVals),
                             .entries = NewInvertedIndex(Index_StoreNumeric, 1)};
  return n;
}

static inline int NumericRangeNode_Add(NumericRangeNode *n, NumericRangeNode *parent, t_docId docId,
                                       double value, int level) {

  if (!NumericRangeNode_IsLeaf(n)) {
    // if this node has already split but retains a range, just add to the range without checking
    // anything
    if (n->range) {
      NumericRange_AddNoCheck(n->range, docId, value);
    }

    // recursively add to its left or right child.
    NumericRangeNode **childP = value < n->value ? &n->left : &n->right;
    NumericRangeNode *child = *childP;
    // if the child has split we get 1 in return
    int rc = NumericRangeNode_Add(child, n, docId, value, level + 1);
    if (rc) {
      // if there was a split it means our max depth has increased.
      // we are too deep - we don't retain this node's range anymore.
      // this keeps memory footprint in check
      if (++n->maxDepth > NR_MAX_DEPTH && n->range) {
        InvertedIndex_Free(n->range->entries);
        kh_destroy(rangeVals, n->range->htvals);
        // RedisModule_Free(n->range->values);
        RedisModule_Free(n->range);
        n->range = NULL;
      }

      // check if we need to rebalance the child.
      // To ease the rebalance we don't rebalance the root
      // nor do we rebalance nodes that are with ranges (n->maxDepth > NR_MAX_DEPTH)
      if ((child->right->maxDepth - child->left->maxDepth) > NR_MAX_DEPTH) {  // role to the left
        NumericRangeNode *right = child->right;
        child->right = right->left;
        right->left = child;
        --child->maxDepth;
        *childP = right;  // replace the child with the new child
      } else if ((child->left->maxDepth - child->right->maxDepth) >
                 NR_MAX_DEPTH) {  // role to the right
        NumericRangeNode *left = child->left;
        child->left = left->right;
        left->right = child;
        --child->maxDepth;
        *childP = left;  // replace the child with the new child
      }
    }
    // return 1 or 0 to our called, so this is done recursively
    return rc;
  }

  // if this node is a leaf - we add AND check the cardinlity. We only split leaf nodes
  int card = NumericRange_AddValue(n->range, docId, value);

  // printf("Added %d %f to node %f..%f, card now %zd, size now %zd\n", docId, value,
  // n->range->minVal,
  //        n->range->maxVal, card, n->range->entries->numDocs);
  if (card >= n->range->splitCard || (NumericRange_GetDocCount(n->range) > NR_MAXRANGE_SIZE &&
                                      NumericRange_GetCardinality(n->range) > 1)) {

    if (parent && (parent->left == NULL || parent->right == NULL)) {
      // See if we can merge without creating a new leaf:
      // [0..9]
      //    [10..19]
      //      [20..29]
      /**
       *
       */
    }
    // split this node but don't delete its range
    printf("Splitting at level %d\n", level);
    double split = NumericRange_Split(n->range, &n->left, &n->right);

    n->value = split;

    n->maxDepth = 1;
    return 1;
  }

  return 0;
}

/* Recursively add a node's children to the range. */
void __recursiveAddRange(Vector *v, NumericRangeNode *n, double min, double max) {
  if (!n) return;

  if (n->range) {
    // printf("min %f, max %f, range %f..%f, contained? %d, overlaps? %d, leaf? %d\n", min, max,
    //        n->range->minVal, n->range->maxVal, NumericRange_Contained(n->range, min, max),
    //        NumericRange_Overlaps(n->range, min, max), __isLeaf(n));
    // if the range is completely contained in the search, we can just add it and not inspect any
    // downwards
    if (NumericRange_Contained(n->range, min, max)) {
      Vector_Push(v, n->range);
      return;
    }
    // No overlap at all - no need to do anything
    if (!NumericRange_Overlaps(n->range, min, max)) {
      return;
    }
  }

  // for non leaf nodes - we try to descend into their children
  if (!NumericRangeNode_IsLeaf(n)) {
    __recursiveAddRange(v, n->left, min, max);
    __recursiveAddRange(v, n->right, min, max);
  } else if (NumericRange_Overlaps(n->range, min, max)) {
    Vector_Push(v, n->range);
    return;
  }
}

/* Find the numeric ranges that fit the range we are looking for. We try to minimize the number of
 * nodes we'll later need to union */
Vector *NumericRangeNode_FindRange(NumericRangeNode *n, double min, double max) {

  Vector *leaves = NewVector(NumericRange *, 8);
  __recursiveAddRange(leaves, n, min, max);
  // printf("Found %zd ranges for %f...%f\n", leaves->top, min, max);
  // for (int i = 0; i < leaves->top; i++) {
  //   NumericRange *rng;
  //   Vector_Get(leaves, i, &rng);
  //   printf("%f...%f (%f). %d card, %d splitCard\n", rng->minVal, rng->maxVal,
  //          rng->maxVal - rng->minVal, rng->entries->numDocs, rng->splitCard);
  // }

  return leaves;
}

void NumericRangeNode_Free(NumericRangeNode *n) {
  if (!n) return;
  if (n->range) {
    InvertedIndex_Free(n->range->entries);
    kh_destroy(rangeVals, n->range->htvals);
    RedisModule_Free(n->range);
    n->range = NULL;
  }

  NumericRangeNode_Free(n->left);
  NumericRangeNode_Free(n->right);

  RedisModule_Free(n);
}

/* Create a new numeric range tree */
NumericRangeTree *NewNumericRangeTree() {
  NumericRangeTree *ret = RedisModule_Alloc(sizeof(NumericRangeTree));
  ret->root = NewLeafNode(2, NF_NEGATIVE_INFINITY, NF_INFINITY, 2);
  ret->numEntries = 0;
  ret->numRanges = 1;
  ret->revisionId = 0;
  ret->lastDocId = 0;
  return ret;
}

int NumericRangeTree_Add(NumericRangeTree *t, t_docId docId, double value) {

  // Do not allow duplicate entries. This might happen due to indexer bugs and we need to protect
  // from it
  if (docId <= t->lastDocId) {
    return 0;
  }
  t->lastDocId = docId;

  int rc = NumericRangeNode_Add(t->root, NULL, docId, value, 0);
  // rc != 0 means the tree nodes have changed, and concurrent iteration is not allowed now
  // we increment the revision id of the tree, so currently running query iterators on it
  // will abort the next time they get execution context
  if (rc) {
    t->revisionId++;
  }
  t->numRanges += rc;
  t->numEntries++;

  return rc;
}

Vector *NumericRangeTree_Find(NumericRangeTree *t, double min, double max) {
  return NumericRangeNode_FindRange(t->root, min, max);
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

void NumericRangeTree_Free(NumericRangeTree *t) {
  NumericRangeNode_Free(t->root);
  RedisModule_Free(t);
}

IndexIterator *NewNumericRangeIterator(NumericRange *nr, NumericFilter *f) {

  // if this range is at either end of the filter, we need to check each record
  if (NumericFilter_Match(f, nr->minVal) && NumericFilter_Match(f, nr->maxVal)) {
    // make the filter NULL so the reader will ignore it
    f = NULL;
  }
  IndexReader *ir = NewNumericReader(nr->entries, f);

  return NewReadIterator(ir);
}

/* Create a union iterator from the numeric filter, over all the sub-ranges in the tree that fit
 * the filter */
IndexIterator *createNumericIterator(NumericRangeTree *t, NumericFilter *f) {

  Vector *v = NumericRangeTree_Find(t, f->min, f->max);
  if (!v || Vector_Size(v) == 0) {
    // printf("Got no filter vector\n");
    return NULL;
  }

  int n = Vector_Size(v);
  // if we only selected one range - we can just iterate it without union or anything
  if (n == 1) {
    NumericRange *rng;
    Vector_Get(v, 0, &rng);
    IndexIterator *it = NewNumericRangeIterator(rng, f);
    Vector_Free(v);
    return it;
  }

  // We create a  union iterator, advancing a union on all the selected range,
  // treating them as one consecutive range
  IndexIterator **its = calloc(n, sizeof(IndexIterator *));

  for (size_t i = 0; i < n; i++) {
    NumericRange *rng;
    Vector_Get(v, i, &rng);
    if (!rng) {
      continue;
    }

    its[i] = NewNumericRangeIterator(rng, f);
  }
  Vector_Free(v);

  IndexIterator *it = NewUnionIterator(its, n, NULL, 1, 1);

  return it;
}

RedisModuleType *NumericIndexType = NULL;
#define NUMERICINDEX_KEY_FMT "nm:%s/%s"

RedisModuleString *fmtRedisNumericIndexKey(RedisSearchCtx *ctx, const char *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, NUMERICINDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

struct indexIterator *NewNumericFilterIterator(RedisSearchCtx *ctx, NumericFilter *flt,
                                               ConcurrentSearchCtx *csx) {
  RedisModuleString *s = fmtRedisNumericIndexKey(ctx, flt->fieldName);
  RedisModuleKey *key = RedisModule_OpenKey(ctx->redisCtx, s, REDISMODULE_READ);
  if (!key || RedisModule_ModuleTypeGetType(key) != NumericIndexType) {
    return NULL;
  }
  NumericRangeTree *t = RedisModule_ModuleTypeGetValue(key);

  IndexIterator *it = createNumericIterator(t, flt);
  if (!it) {
    return NULL;
  }

  NumericUnionCtx *uc = malloc(sizeof(*uc));
  uc->lastRevId = t->revisionId;
  uc->it = it;
  if (csx) {
    ConcurrentSearch_AddKey(csx, key, REDISMODULE_READ, s, NumericRangeIterator_OnReopen, uc, free,
                            ConcurrentKey_SharedNothing);
  }
  return it;
}

NumericRangeTree *OpenNumericIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey) {

  RedisModuleKey *key_s = NULL;

  if (!idxKey) {
    idxKey = &key_s;
  }

  *idxKey = RedisModule_OpenKey(ctx->redisCtx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

  int type = RedisModule_KeyType(*idxKey);
  if (type != REDISMODULE_KEYTYPE_EMPTY &&
      RedisModule_ModuleTypeGetType(*idxKey) != NumericIndexType) {
    return NULL;
  }

  /* Create an empty value object if the key is currently empty. */
  NumericRangeTree *t;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    t = NewNumericRangeTree();
    RedisModule_ModuleTypeSetValue((*idxKey), NumericIndexType, t);
  } else {
    t = RedisModule_ModuleTypeGetValue(*idxKey);
  }
  return t;
}

void __numericIndex_memUsageCallback(NumericRangeNode *n, void *ctx) {
  unsigned long *sz = ctx;
  *sz += sizeof(NumericRangeNode);

  if (n->range) {
    *sz += sizeof(NumericRange);
    *sz += NumericRange_GetCardinality(n->range) * sizeof(double);
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

int NumericIndexType_Register(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = NumericIndexType_RdbLoad,
                               .rdb_save = NumericIndexType_RdbSave,
                               .aof_rewrite = GenericAofRewrite_DisabledHandler,
                               .free = NumericIndexType_Free,
                               .mem_usage = NumericIndexType_MemUsage};

  NumericIndexType = RedisModule_CreateDataType(ctx, "numericdx", 0, &tm);
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

static int __cmd_docId(const void *p1, const void *p2) {
  NumericRangeEntry *e1 = (NumericRangeEntry *)p1;
  NumericRangeEntry *e2 = (NumericRangeEntry *)p2;

  return (int)e1->docId - (int)e2->docId;
}
void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver != 0) {
    return 0;
  }

  NumericRangeTree *t = NewNumericRangeTree();
  uint64_t num = RedisModule_LoadUnsigned(rdb);

  // we create an array of all the entries so that we can sort them by docId
  NumericRangeEntry *entries = calloc(num, sizeof(NumericRangeEntry));
  size_t n = 0;
  for (size_t i = 0; i < num; i++) {
    entries[n].docId = RedisModule_LoadUnsigned(rdb);
    entries[n].value = RedisModule_LoadDouble(rdb);
    n++;
  }

  // sort the entries by doc id, as they were not saved in this order
  qsort(entries, num, sizeof(NumericRangeEntry), __cmd_docId);

  // now push them in order into the tree
  for (size_t i = 0; i < num; i++) {
    NumericRangeTree_Add(t, entries[i].docId, entries[i].value);
  }
  free(entries);

  return t;
}

struct __niRdbSaveCtx {
  RedisModuleIO *rdb;
  size_t num;
};

void __numericIndex_rdbSaveCallback(NumericRangeNode *n, void *ctx) {
  struct __niRdbSaveCtx *rctx = ctx;

  if (NumericRangeNode_IsLeaf(n) && n->range) {
    NumericRange *rng = n->range;
    RSIndexResult *res = NULL;
    IndexReader *ir = NewNumericReader(rng->entries, NULL);

    while (INDEXREAD_OK == IR_Read(ir, &res)) {
      RedisModule_SaveUnsigned(rctx->rdb, res->docId);
      RedisModule_SaveDouble(rctx->rdb, res->num.value);
    }
    IR_Free(ir);
  }
}
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value) {

  NumericRangeTree *t = value;

  RedisModule_SaveUnsigned(rdb, t->numEntries);

  struct __niRdbSaveCtx ctx = {rdb, 0};

  NumericRangeNode_Traverse(t->root, __numericIndex_rdbSaveCallback, &ctx);
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
    iter->nodesStack = array_append(iter->nodesStack, ret->left);
    iter->nodesStack = array_append(iter->nodesStack, ret->right);
  }

  return ret;
}

void NumericRangeTreeIterator_Free(NumericRangeTreeIterator *iter) {
  array_free(iter->nodesStack);
  rm_free(iter);
}
