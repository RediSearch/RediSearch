#include "numeric_index.h"
#include "redis_index.h"
#include "sys/param.h"
#include "rmutil/vector.h"
#include "rmutil/util.h"
#include "index.h"
#include <math.h>
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

int NumericRange_Add(NumericRange *n, t_docId docId, double value, int checkCard) {

  int add = 0;
  if (checkCard) {
    add = 1;
    size_t card = n->card;
    for (int i = 0; i < card; i++) {

      if (n->values[i] == value) {
        add = 0;
        break;
      }
    }
  }
  if (n->minVal == NF_NEGATIVE_INFINITY || value < n->minVal) n->minVal = value;
  if (n->maxVal == NF_INFINITY || value > n->maxVal) n->maxVal = value;
  if (add) {
    if (n->card < n->splitCard) {
      n->values[n->card] = value;
    }
    ++n->card;
  }

  InvertedIndex_WriteNumericEntry(n->entries, docId, value);

  return n->card;
}

double NumericRange_Split(NumericRange *n, NumericRangeNode **lp, NumericRangeNode **rp) {

  double split = (n->minVal + n->maxVal) / (double)2;

  // printf("split point :%f\n", split);
  *lp = NewLeafNode(n->entries->numDocs / 2 + 1, n->minVal, split,
                    MIN(NR_MAXRANGE_CARD, 1 + n->splitCard * NR_EXPONENT));
  *rp = NewLeafNode(n->entries->numDocs / 2 + 1, split, n->maxVal,
                    MIN(NR_MAXRANGE_CARD, 1 + n->splitCard * NR_EXPONENT));

  RSIndexResult *res = NULL;
  IndexReader *ir = NewNumericReader(n->entries, NULL);
  while (INDEXREAD_OK == IR_Read(ir, &res)) {
    NumericRange_Add(res->num.value < split ? (*lp)->range : (*rp)->range, res->docId,
                     res->num.value, 1);
  }
  IR_Free(ir);

  // printf("Splitting node %p %f..%f, card %d size %d\n", n, n->minVal, n->maxVal, n->card,
  //        n->entries->numDocs);
  // printf("left node: %d, right: %d\n", (*lp)->range->entries->numDocs,
  //        (*rp)->range->entries->numDocs);
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
                             .card = 0,
                             .splitCard = splitCard,
                             .values = RedisModule_Calloc(splitCard, sizeof(double)),
                             .entries = NewInvertedIndex(Index_StoreNumeric, 1)};
  return n;
}

int NumericRangeNode_Add(NumericRangeNode *n, t_docId docId, double value) {

  if (!__isLeaf(n)) {
    // if this node has already split but retains a range, just add to the range without checking
    // anything
    if (n->range) {
      NumericRange_Add(n->range, docId, value, 0);
    }

    // recursively add to its left or right child. if the child has split we get 1 in return
    int rc = NumericRangeNode_Add((value < n->value ? n->left : n->right), docId, value);
    if (rc) {
      // if there was a split it means our max depth has increased.
      // we we are too deep - we don't retain this node's range anymore.
      // this keeps memory footprint in check
      if (++n->maxDepth > NR_MAX_DEPTH && n->range) {
        InvertedIndex_Free(n->range->entries);
        RedisModule_Free(n->range->values);
        RedisModule_Free(n->range);
        n->range = NULL;
      }
    }
    // return 1 or 0 to our called, so this is done recursively
    return rc;
  }

  // if this node is a leaf - we add AND check the cardinlity. We only split leaf nodes
  int card = NumericRange_Add(n->range, docId, value, 1);

  // printf("Added %d %f to node %f..%f, card now %zd, size now %zd\n", docId, value,
  // n->range->minVal,
  //        n->range->maxVal, card, n->range->entries->numDocs);
  if (card >= n->range->splitCard ||
      (n->range->entries->numDocs > NR_MAXRANGE_SIZE && n->range->card > 1)) {

    // split this node but don't delete its range
    double split = NumericRange_Split(n->range, &n->left, &n->right);

    n->value = split;

    n->maxDepth = 1;
    return 1;
  }

  return 0;
}

/* recrusively add a node's children to the range. */
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
  if (!__isLeaf(n)) {
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
    RedisModule_Free(n->range->values);
    RedisModule_Free(n->range);
    n->range = NULL;
  }

  NumericRangeNode_Free(n->left);
  NumericRangeNode_Free(n->right);

  RedisModule_Free(n);
}

/* Create a new numeric range tree */
NumericRangeTree *NewNumericRangeTree() {
#define GC_NODES_INITIAL_SIZE 10
  NumericRangeTree *ret = RedisModule_Alloc(sizeof(NumericRangeTree));

  ret->root = NewLeafNode(2, NF_NEGATIVE_INFINITY, NF_INFINITY, 2);
  ret->numEntries = 0;
  ret->numRanges = 1;
  ret->revisionId = 0;
  ret->lastDocId = 0;
  ret->gcIterator = NULL;
  return ret;
}

int NumericRangeTree_Add(NumericRangeTree *t, t_docId docId, double value) {

  // Do not allow duplicate entries. This might happen due to indexer bugs and we need to protect
  // from it
  if (docId <= t->lastDocId) {
    return 0;
  }
  t->lastDocId = docId;

  int rc = NumericRangeNode_Add(t->root, docId, value);
  // rc != 0 means the tree nodes have changed, and concurrent iteration is not allowed now
  // we increment the revision id of the tree, so currently running query iterators on it
  // will abort the next time they get execution context
  if (rc) {
    if(t->gcIterator){
      NumericRangeTreeIterator_Free(t->gcIterator);
    }
    t->gcIterator = NULL;
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
  if(t->gcIterator){
    NumericRangeTreeIterator_Free(t->gcIterator);
  }
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

NumericRangeTree *OpenNumericIndex(RedisSearchCtx *ctx, const char *fname,
                                   RedisModuleKey **idxKey) {

  RedisModuleString *s = fmtRedisNumericIndexKey(ctx, fname);
  RedisModuleKey *key_s = NULL;

  if (!idxKey) {
    idxKey = &key_s;
  }

  *idxKey = RedisModule_OpenKey(ctx->redisCtx, s, REDISMODULE_READ | REDISMODULE_WRITE);
  RedisModule_FreeString(ctx->redisCtx, s);

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

  if (__isLeaf(n) && n->range) {
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

NumericRangeTreeIterator* NumericRangeTreeIterator_New(NumericRangeTree *t){
#define NODE_STACK_INITIAL_SIZE 4
  NumericRangeTreeIterator* iter = rm_malloc(sizeof(NumericRangeTreeIterator));
  iter->nodesStack = array_new(NumericRangeNode*, NODE_STACK_INITIAL_SIZE);
  array_append(iter->nodesStack, t->root);
  return iter;
}

NumericRangeNode* NumericRangeTreeIterator_Next(NumericRangeTreeIterator *iter){
  if(array_len(iter->nodesStack) == 0){
    return NULL;
  }
  NumericRangeNode* ret = array_pop(iter->nodesStack);
  if(!__isLeaf(ret)){
    iter->nodesStack = array_append(iter->nodesStack, ret->left);
    iter->nodesStack = array_append(iter->nodesStack, ret->right);
  }

  return ret;
}

void NumericRangeTreeIterator_Free(NumericRangeTreeIterator *iter){
  array_free(iter->nodesStack);
  rm_free(iter);
}
