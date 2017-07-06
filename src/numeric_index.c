#include "numeric_index.h"
#include "sys/param.h"
#include "rmutil/vector.h"
#include "index.h"
#include <math.h>
#include "redismodule.h"
//#include "tests/time_sample.h"
#define NR_EXPONENT 4
#define NR_MAXRANGE_CARD 2500
#define NR_MAXRANGE_SIZE 10000
#define NR_MAX_DEPTH 2

double qselect(double *v, int len, int k) {
#define SWAP(a, b) \
  {                \
    tmp = v[a];    \
    v[a] = v[b];   \
    v[b] = tmp;    \
  }
  int i, st, tmp;

  for (st = i = 0; i < len - 1; i++) {
    if (v[i] > v[len - 1]) continue;
    SWAP(i, st);
    st++;
  }

  SWAP(len - 1, st);

  return k == st ? v[st] : st > k ? qselect(v, st, k) : qselect(v + st, len - st, k - st);
}

/* Returns 1 if the entire numeric range is contained between min and max */
int NumericRange_Contained(NumericRange *n, double min, double max) {
  if (!n) return 0;
  int rc = (n->minVal >= min && n->maxVal <= max);
  // printf("range %f..%f, min %f max %f, WITHIN? %d\n", n->minVal, n->maxVal, min, max, rc);
  return rc;
}

/* Returns 1 if min and max are both inside the range. this is the opposite of _Within */
int NumericRange_Contains(NumericRange *n, double min, double max) {
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
  // printf("Adding %d %f to %f..%f\n", docId, value, n->minVal, n->maxVal);
  if (n->size >= n->cap) {
    n->cap += n->cap ? MIN(n->cap / 2, 1024 * 1024) : 2;
    n->entries = RedisModule_Realloc(n->entries, n->cap * sizeof(NumericRangeEntry));
  }

  int add = 1;
  if (checkCard) {
    for (int i = 0; i < n->size; i++) {
      if (n->entries[i].value == value) {
        add = 0;
        break;
      }
    }
  }

  if (value < n->minVal || n->card == 0) n->minVal = value;
  if (value > n->maxVal || n->card == 0) n->maxVal = value;

  if (add) ++n->card;

  n->entries[n->size++] = (NumericRangeEntry){.docId = docId, .value = value};
  return n->card;
}

double NumericRange_Split(NumericRange *n, NumericRangeNode **lp, NumericRangeNode **rp) {
  // TimeSample ts;
  // TimeSampler_Start(&ts);
  // double scores[n->size];
  // for (size_t i = 0; i < n->size; i++) {
  //   scores[i] = n->entries[i].value;
  // }

  // double split = qselect(scores, n->size, n->size / 2);

  double split = (n->minVal + n->maxVal) / (double)2;
  // printf("split point :%f\n", split);
  *lp = NewLeafNode(n->size / 2 + 1, n->minVal, split,
                    MIN(NR_MAXRANGE_CARD, 1 + n->splitCard * NR_EXPONENT));
  *rp = NewLeafNode(n->size / 2 + 1, split, n->maxVal,
                    MIN(NR_MAXRANGE_CARD, 1 + n->splitCard * NR_EXPONENT));

  for (uint32_t i = 0; i < n->size; i++) {
    NumericRange_Add(n->entries[i].value < split ? (*lp)->range : (*rp)->range, n->entries[i].docId,
                     n->entries[i].value, 1);
  }
  // TimeSampler_End(&ts);

  // printf("Splitting node %p %f..%f, card %d size %d took %.04fus\n", n, n->minVal, n->maxVal,
  //        n->card, n->size, (double)TimeSampler_DurationNS(&ts) / 1000.0F);
  // printf("left node: %d, right: %d\n", (*lp)->range->size, (*rp)->range->size);
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
                             .cap = cap,
                             .size = 0,
                             .card = 0,
                             .splitCard = splitCard,
                             .entries = RedisModule_Calloc(cap, sizeof(NumericRangeEntry))};
  return n;
}

#define __isLeaf(n) (n->left == NULL && n->right == NULL)

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
        RedisModule_Free(n->range->entries);
        RedisModule_Free(n->range);
        n->range = NULL;
      }
    }
    // return 1 or 0 to our called, so this is done recursively
    return rc;
  }

  // if this node is a leaf - we add AND check the cardinlity. We only split leaf nodes
  int card = NumericRange_Add(n->range, docId, value, 1);

  if (card >= n->range->splitCard || (n->range->size > NR_MAXRANGE_SIZE && n->range->card > 1)) {

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
    //
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
  //          rng->maxVal - rng->minVal, rng->size, rng->splitCard);
  // }

  return leaves;
}

void NumericRangeNode_Free(NumericRangeNode *n) {
  if (!n) return;
  if (n->range) {

    RedisModule_Free(n->range->entries);
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

  ret->root = NewLeafNode(2, 0, 0, 2);
  ret->numEntries = 0;
  ret->numRanges = 1;
  return ret;
}

int NumericRangeTree_Add(NumericRangeTree *t, t_docId docId, double value) {

  int rc = NumericRangeNode_Add(t->root, docId, value);
  t->numRanges += rc;
  t->numEntries++;
  //
  // printf("range tree added %d, size now %zd docs %zd ranges\n", docId, t->numEntries,
  // t->numRanges);

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

/* Read the next entry from the iterator, into hit *e.
  *  Returns INDEXREAD_EOF if at the end */
int NR_Read(void *ctx, RSIndexResult **r) {

  NumericRangeIterator *it = ctx;

  if (it->atEOF || it->rng->size == 0) {
    goto eof;
  }

  int match = 0;
  double lastValue = 0;
  do {
    if (it->offset == it->rng->size) {
      goto eof;
    }
    it->lastDocId = it->rng->entries[it->offset].docId;
    // lastValue = it->rng->entries[it->offset].value;
    if (it->nf) {
      match = NumericFilter_Match(it->nf, it->rng->entries[it->offset].value);
    } else {
      match = 1;
    }
    it->offset++;

    // printf("nf %s filter doc %d (%f): %d\n", it->nf->fieldName, it->lastDocId, lastValue, match);
  } while (!match);

  if (match) {
    // match must be true here
    it->rec->docId = it->lastDocId;
    *r = it->rec;

    return INDEXREAD_OK;
  }
eof:
  it->atEOF = 1;
  return INDEXREAD_EOF;
}

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int NR_SkipTo(void *ctx, uint32_t docId, RSIndexResult **r) {

  NumericRangeIterator *it = ctx;

  if (it->atEOF || it->rng->size == 0) {
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  // If we are seeking beyond our last docId - just declare EOF
  if (docId > it->rng->entries[it->rng->size - 1].docId) {
    it->atEOF = 1;
    it->rec->docId = 0;
    return INDEXREAD_EOF;
  }

  // Find the closest entry to the requested docId
  int top = (int)it->rng->size - 1, bottom = (int)it->offset;
  int i = bottom;

  while (bottom <= top) {
    t_docId did = it->rng->entries[i].docId;
    if (did == docId) {
      break;
    }
    if (docId <= did) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
    i = (bottom + top) / 2;
  }

  it->offset = i;
  it->lastDocId = it->rng->entries[i].docId;
  it->rec->docId = it->lastDocId;

  // Now read the current entry
  int rc = NR_Read(it, r);

  // EOF or not found are returned as is
  if (rc != INDEXREAD_OK) return rc;

  // if we got ok - check if the read document was the one we wanted or not.
  // If the requested document doesn't match the filter, we should return NOTFOUND
  return it->lastDocId == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
}
/* the last docId read */
t_docId NR_LastDocId(void *ctx) {
  return ((NumericRangeIterator *)ctx)->lastDocId;
}

/* can we continue iteration? */
int NR_HasNext(void *ctx) {
  return !((NumericRangeIterator *)ctx)->atEOF;
}

/* release the iterator's context and free everything needed */
void NR_Free(IndexIterator *self) {
  NumericRangeIterator *it = self->ctx;
  IndexResult_Free(it->rec);
  free(self->ctx);
  free(self);
}

/* Return the number of results in this iterator. Used by the query execution
 * on the top iterator */
size_t NR_Len(void *ctx) {
  return ((NumericRangeIterator *)ctx)->rng->size;
}

RSIndexResult *NR_Current(void *ctx) {
  return ((NumericRangeIterator *)ctx)->rec;
}

IndexIterator *NewNumericRangeIterator(NumericRange *nr, NumericFilter *f) {
  IndexIterator *ret = malloc(sizeof(IndexIterator));

  NumericRangeIterator *it = malloc(sizeof(NumericRangeIterator));

  it->nf = NULL;
  // if this range is at either end of the filter, we need to check each record
  if (!NumericFilter_Match(f, nr->minVal) || !NumericFilter_Match(f, nr->maxVal)) {
    it->nf = f;
  }

  it->atEOF = 0;
  it->lastDocId = 0;
  it->offset = 0;
  it->rng = nr;
  it->rec = NewVirtualResult();
  it->rec->fieldMask = RS_FIELDMASK_ALL;
  ret->ctx = it;

  ret->Free = NR_Free;
  ret->Len = NR_Len;
  ret->HasNext = NR_HasNext;
  ret->LastDocId = NR_LastDocId;
  ret->Current = NR_Current;
  ret->Read = NR_Read;
  ret->SkipTo = NR_SkipTo;

  return ret;
}

/* Create a union iterator from the numeric filter, over all the sub-ranges in the tree that fit
 * the
 * filter */
IndexIterator *NewNumericFilterIterator(NumericRangeTree *t, NumericFilter *f) {

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
  return NewUnionIterator(its, n, NULL, 1);
}

RedisModuleType *NumericIndexType = NULL;
#define NUMERICINDEX_KEY_FMT "nm:%s/%s"

RedisModuleString *fmtRedisNumericIndexKey(RedisSearchCtx *ctx, const char *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, NUMERICINDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

NumericRangeTree *OpenNumericIndex(RedisSearchCtx *ctx, const char *fname) {

  RedisModuleString *s = fmtRedisNumericIndexKey(ctx, fname);
  RedisModuleKey *key = RedisModule_OpenKey(ctx->redisCtx, s, REDISMODULE_READ | REDISMODULE_WRITE);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != NumericIndexType) {
    return NULL;
  }

  /* Create an empty value object if the key is currently empty. */
  NumericRangeTree *t;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    t = NewNumericRangeTree();
    RedisModule_ModuleTypeSetValue(key, NumericIndexType, t);
  } else {
    t = RedisModule_ModuleTypeGetValue(key);
  }
  return t;
}

void __numericIndex_memUsageCallback(NumericRangeNode *n, void *ctx) {
  unsigned long *sz = ctx;
  *sz += sizeof(NumericRangeNode);
  if (n->range) {
    *sz += sizeof(NumericRange);
    *sz += n->range->cap * sizeof(NumericRangeEntry);
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
                               .aof_rewrite = NumericIndexType_AofRewrite,
                               .free = NumericIndexType_Free,
                               .mem_usage = NumericIndexType_MemUsage};

  NumericIndexType = RedisModule_CreateDataType(ctx, "numericdx", 0, &tm);
  if (NumericIndexType == NULL) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

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

    for (size_t n = 0; n < rng->size; n++) {
      RedisModule_SaveUnsigned(rctx->rdb, rng->entries[n].docId);
      RedisModule_SaveDouble(rctx->rdb, rng->entries[n].value);
      ++rctx->num;
    }
  }
}
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value) {

  NumericRangeTree *t = value;

  RedisModule_SaveUnsigned(rdb, t->numEntries);

  struct __niRdbSaveCtx ctx = {rdb, 0};

  NumericRangeNode_Traverse(t->root, __numericIndex_rdbSaveCallback, &ctx);
}

void NumericIndexType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
}
void NumericIndexType_Digest(RedisModuleDigest *digest, void *value) {
}

void NumericIndexType_Free(void *value) {
  NumericRangeTree *t = value;
  NumericRangeTree_Free(t);
}
