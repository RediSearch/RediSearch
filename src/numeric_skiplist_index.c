#include "numeric_skiplist_index.h"
#include "redis_index.h"
#include "sys/param.h"
#include "rmutil/vector.h"
#include "rmutil/util.h"
#include "index.h"
#include "util/arr.h"
#include <math.h>
#include "redismodule.h"
#include "util/misc.h"
#include "inverted_index.h"

#include "dep/skiplist/skiplist.h"

typedef struct {
  IndexIterator *it;
  uint32_t lastRevId;
} NumericUnionCtx;

uint64_t numericSkiplistUniqueId = 0;

/* A callback called after a concurrent context regains execution context. When this happen we need
 * to make sure the key hasn't been deleted or its structure changed, which will render the
 * underlying iterators invalid */
void NumericSkiplistIterator_OnReopen(void *privdata) {
}

NumericSkiplistNode *NewSkiplistNode(double value) {
  NumericSkiplistNode *n = rm_malloc(sizeof(*n));
  n->value = value;
  // TODO: Index_StoreNumericSkip??
  n->invidx = NewInvertedIndex(Index_StoreNumeric, 1);
  return n;
}

int NumericSkiplistCompare(void *a, void *b) {
  NumericSkiplistNode *nna = (NumericSkiplistNode *)a;
  NumericSkiplistNode *nnb = (NumericSkiplistNode *)b;
  return nna->value > nnb->value ? 1 : nna->value < nnb->value ? -1 : 0;
}

void NumericSkiplistElementDtor(void *a) {
  NumericSkiplistNode *node = a;
  InvertedIndex_Free(node->invidx);
}

/* Create a new numeric range tree */
NumericSkiplist *NewNumericSkiplist() {
  NumericSkiplist *ret = rm_malloc(sizeof(*ret));

  ret->sl = slCreate(&NumericSkiplistCompare, &NumericSkiplistElementDtor);
  ret->numInvIdx = 0;
  ret->numEntries = 0;
  ret->revisionId = 0;
  ret->lastDocId = 0;
  ret->uniqueId = numericSkiplistUniqueId++;
  return ret;
}

NRN_AddRv NumericSkiplist_Add(NumericSkiplist *t, t_docId docId, double value) {
  NRN_AddRv rv = {.sz = 0, .changed = 0, .numRecords = 0};
  // Do not allow duplicate entries. This might happen due to indexer bugs and we need to protect
  // from it
  if (docId <= t->lastDocId) {
    return rv;
  }
  t->lastDocId = docId;
  
  NumericSkiplistNode key = {.value = value};
  NumericSkiplistNode *n = slGet(t->sl, &key);
  if (!n) {
    t->revisionId++;
    n = NewSkiplistNode(value);
    t->numInvIdx++;
    slInsert(t->sl, n);
  }  

  t->numEntries++;
  rv.numRecords = 1;
  rv.sz = InvertedIndex_WriteNumericSkiplistEntry(n->invidx, docId);
  return rv;
}

void NumericSkiplist_Free(NumericSkiplist *ns) {
  slFree(ns->sl);
}

/* Create a union iterator from the numeric filter, over all the sub-ranges in the tree that fit
 * the filter */
IndexIterator *createNumericSkiplistUnionIterator(const IndexSpec *sp, NumericSkiplist *t,
                                                  const NumericFilter *f) {
  skiplistIterator *iter = slIteratorCreate(t->sl, (void *)&f->min, (void *)&f->max);
  if (!iter) return NULL;

  NumericSkiplistNode *n = NULL;
  Vector *v = NewVector(NumericSkiplistNode *, 8);

  while ((n = slIteratorNext(iter)) && n->value <= f->max) {
    if ((f->inclusiveMin == 0 && n->value == f->min) ||
        (f->inclusiveMax == 0 && n->value == f->max)) 
        continue;
    Vector_Push(v, n);
  }

  if (!v || Vector_Size(v) == 0) {
    if (v) {
      Vector_Free(v);
    }
    return NULL;
  }

  int vectorLen = Vector_Size(v);
  // if we only selected one range - we can just iterate it without union or anything
  if (vectorLen == 1) {
    NumericSkiplistNode *nsn;
    Vector_Get(v, 0, &nsn);
    NumericSkiplistReaderCtx *nsrc = rm_malloc(sizeof(*nsrc));
    *nsrc = (NumericSkiplistReaderCtx){.nsn = nsn, .f = f};
    IndexReader *ir = NewNumericSkiplistReader(sp, nsrc);
    IndexIterator *it = NewReadIterator(ir);
    Vector_Free(v);
    return it;
  }

  // We create a  union iterator, advancing a union on all the selected range,
  // treating them as one consecutive range
  IndexIterator **its = rm_calloc(vectorLen, sizeof(IndexIterator *));

  for (size_t i = 0; i < vectorLen; i++) {
    NumericSkiplistNode *nsn;
    Vector_Get(v, i, &nsn);
    if (!nsn) { // TODO: delete?
      continue;
    }
    NumericSkiplistReaderCtx *nsrc = rm_malloc(sizeof(*nsrc));
    *nsrc = (NumericSkiplistReaderCtx){.nsn = nsn, .f = f};
    IndexReader *ir = NewNumericSkiplistReader(sp, nsrc);
    its[i] = NewReadIterator(ir);
  }
  Vector_Free(v);

  IndexIterator *it = NewUnionIterator(its, vectorLen, NULL, 1, 1);
  return it;
}

static NumericSkiplist *openNumericSkiplistKeysDict(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                             int write) {
  KeysDictValue *kdv = dictFetchValue(ctx->spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->dtor = (void (*)(void *))NumericSkiplist_Free;
  kdv->p = NewNumericSkiplist();
  dictAdd(ctx->spec->keysDict, keyName, kdv);
  return kdv->p;
}

struct indexIterator *NewNumericSkiplistIterator(RedisSearchCtx *ctx, const NumericFilter *flt,
                                               ConcurrentSearchCtx *csx, FieldType forType) {
  RedisModuleString *s = IndexSpec_GetFormattedKeyByName(ctx->spec, flt->fieldName, forType);
  if (!s) {
    return NULL;
  }
  
  NumericSkiplist *t = NULL;
  t = openNumericSkiplistKeysDict(ctx, s, 0);
  if (!t) {
    return NULL;
  }

  IndexIterator *it = createNumericSkiplistUnionIterator(ctx->spec, t, flt);
  if (!it) {
    return NULL;
  }

  if (csx) {
    NumericUnionCtx *uc = rm_malloc(sizeof(*uc));
    uc->lastRevId = t->revisionId;
    uc->it = it;
    ConcurrentSearch_AddKey(csx, NumericSkiplistIterator_OnReopen, uc, rm_free);
  }
  return it;
}

NumericSkiplist *OpenNumericSkiplistIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey) {

  NumericSkiplist *t;
  t = openNumericSkiplistKeysDict(ctx, keyName, 1);
  return t;
}
/*
static unsigned long NumericIndexType_MemUsage(const void *value) {
  const NumericSkiplist *t = value;
  unsigned long sz = sizeof(NumericSkiplist);
  NumericSkiplistNode start = {.value = INT64_MIN};
  skiplistIterator *iter = slIteratorCreate(t->sl, NULL);
  NumericSkiplistNode *n;
  while ((n = slIteratorNext(iter))) {
    sz += sizeof(n);
    sz += InvertedIndex_MemUsage(n->invidx);
  }
  return sz;
}*/

/****************************************************
 * Iterator to be used by GC
 ***************************************************/

// TODO: start iterating from different locations so all indexes are being GCed.
NumericSkiplistIterator *NumericSkiplistIterator_New(NumericSkiplist *t) {
  return slIteratorCreate(t->sl, NULL, NULL);
}

NumericSkiplistNode *NumericSkiplistIterator_Next(NumericSkiplistIterator *iter) {
  return slIteratorNext(iter);
}

void NumericSkiplistIterator_Free(NumericSkiplistIterator *iter) {
  slIteratorDestroy(iter);
}
