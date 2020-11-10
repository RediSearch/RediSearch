#include "decimal_index.h"
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

#include "util/skiplist.h"

typedef struct {
  IndexIterator *it;
  uint32_t lastRevId;
} DecimalUnionCtx;

uint64_t decimalSkiplistUniqueId = 0;

/* A callback called after a concurrent context regains execution context. When this happen we need
 * to make sure the key hasn't been deleted or its structure changed, which will render the
 * underlying iterators invalid */
void DecimalSkiplistIterator_OnReopen(void *privdata) {
}

DecimalSkiplistNode *NewSkiplistNode(double value) {
  DecimalSkiplistNode *n = rm_malloc(sizeof(*n));
  n->value = value;
  // TODO: Index_StoreDecimalSkip??
  n->invidx = NewInvertedIndex(Index_StoreDecimal, 1);
  return n;
}

int DecimalSkiplistCompare(void *a, void *b) {
  DecimalSkiplistNode *nna = (DecimalSkiplistNode *)a;
  DecimalSkiplistNode *nnb = (DecimalSkiplistNode *)b;
  return nna->value > nnb->value ? 1 : nna->value < nnb->value ? -1 : 0;
}

void DecimalSkiplistElementDtor(void *a) {
  DecimalSkiplistNode *node = a;
  InvertedIndex_Free(node->invidx);
  rm_free(a);
}

/* Create a new decimal skiplist */
DecimalSkiplist *NewDecimalSkiplist() {
  DecimalSkiplist *ret = rm_malloc(sizeof(*ret));

  ret->sl = slCreate(&DecimalSkiplistCompare, &DecimalSkiplistElementDtor);
  ret->numInvIdx = 0;
  ret->numEntries = 0;
  ret->revisionId = 0;
  ret->lastDocId = 0;
  ret->uniqueId = decimalSkiplistUniqueId++;
  return ret;
}

NRN_AddRv DecimalSkiplist_Add(DecimalSkiplist *ds, t_docId docId, double value) {
  NRN_AddRv rv = {.sz = 0, .changed = 0, .numRecords = 0};
  // Do not allow duplicate entries. This might happen due to indexer bugs and we need to protect
  // from it
  if (docId <= ds->lastDocId) {
    return rv;
  }
  ds->lastDocId = docId;
  
  DecimalSkiplistNode key = {.value = value};
  DecimalSkiplistNode *n = slGet(ds->sl, &key);
  if (!n) {
    ds->revisionId++;
    n = NewSkiplistNode(value);
    ds->numInvIdx++;
    slInsert(ds->sl, n);
  }  

  ds->numEntries++;
  rv.numRecords = 1;
  rv.sz = InvertedIndex_WriteDecimalEntry(n->invidx, docId, value);
  return rv;
}

void DecimalSkiplist_Free(DecimalSkiplist *ns) {
  if (ns->sl) slFree(ns->sl);
  rm_free(ns);
}

/* Create a union iterator from the numeric filter, over all the skiplist nodes which fit
 * the filter */
IndexIterator *createDecimalSkiplistUnionIterator(const IndexSpec *sp, DecimalSkiplist *ds,
                                                  NumericFilter *f) {
  DecimalSkiplistNode start = {.value = f->min};
  DecimalSkiplistIterator *iter = DecimalSkiplistIterator_New(ds, &start);
  if (!iter) return NULL;

  DecimalSkiplistNode *n = NULL;
  Vector *v = NewVector(DecimalSkiplistNode *, 8);

  while ((n = DecimalSkiplistIterator_Next(iter)) && n->value <= f->max) {
    if (f->min > n->value ||
       (f->inclusiveMin == 0 && n->value == f->min) ||
       (f->inclusiveMax == 0 && n->value == f->max)) 
        continue;
    Vector_Push(v, n);
  }
  DecimalSkiplistIterator_Free(iter);

  if (!v || Vector_Size(v) == 0) {
    if (v) {
      Vector_Free(v);
    }
    return NULL;
  }

  int vectorLen = Vector_Size(v);
  // if we only selected one range - we can just iterate it without union or anything
  if (vectorLen == 1) {
    DecimalSkiplistNode *dsn;
    Vector_Get(v, 0, &dsn);
    //DecimalSkiplistReaderCtx *nsrc = f->nsrc = rm_malloc(sizeof(*nsrc));
    //*nsrc = (DecimalSkiplistReaderCtx){.dsn = dsn, .f = f};
    IndexReader *ir = NewDecimalReader(sp, dsn->invidx, dsn->value);
    IndexIterator *it = NewReadIterator(ir);
    Vector_Free(v);
    return it;
  }

  // We create a  union iterator, advancing a union on all the selected range,
  // treating them as one consecutive range
  IndexIterator **its = rm_calloc(vectorLen, sizeof(IndexIterator *));

  for (size_t i = 0; i < vectorLen; i++) {
    DecimalSkiplistNode *dsn;
    Vector_Get(v, i, &dsn);
    if (!dsn) { // TODO: delete?
      continue;
    }
    //DecimalSkiplistReaderCtx *nsrc = f->nsrc = rm_malloc(vectorLen * sizeof(*nsrc));
    //nsrc[i] = (DecimalSkiplistReaderCtx){.dsn = dsn, .f = f};
    IndexReader *ir = NewDecimalReader(sp, dsn->invidx, dsn->value);
    its[i] = NewReadIterator(ir);
  }
  Vector_Free(v);

  IndexIterator *it = NewUnionIterator(its, vectorLen, NULL, 1, 1);
  return it;
}

static DecimalSkiplist *openDecimalSkiplistKeysDict(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                             int write) {
  KeysDictValue *kdv = dictFetchValue(ctx->spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->dtor = (void (*)(void *))DecimalSkiplist_Free;
  kdv->p = NewDecimalSkiplist();
  dictAdd(ctx->spec->keysDict, keyName, kdv);
  return kdv->p;
}

struct indexIterator *NewDecimalSkiplistIterator(RedisSearchCtx *ctx, NumericFilter *flt,
                                               ConcurrentSearchCtx *csx, FieldType forType) {
  RedisModuleString *s = IndexSpec_GetFormattedKeyByName(ctx->spec, flt->fieldName, forType);
  if (!s) {
    return NULL;
  }
  
  DecimalSkiplist *ds = NULL;
  ds = openDecimalSkiplistKeysDict(ctx, s, 0);
  if (!ds) {
    return NULL;
  }

  IndexIterator *it = createDecimalSkiplistUnionIterator(ctx->spec, ds, flt);
  if (!it) {
    return NULL;
  }

  if (csx) {
    DecimalUnionCtx *uc = rm_malloc(sizeof(*uc));
    uc->lastRevId = ds->revisionId;
    uc->it = it;
    ConcurrentSearch_AddKey(csx, DecimalSkiplistIterator_OnReopen, uc, rm_free);
  }
  return it;
}

DecimalSkiplist *OpenDecimalSkiplistIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey) {
  DecimalSkiplist *t;
  t = openDecimalSkiplistKeysDict(ctx, keyName, 1);
  return t;
}
/*
static unsigned long DecimalIndexType_MemUsage(const void *value) {
  const DecimalSkiplist *t = value;
  unsigned long sz = sizeof(DecimalSkiplist);
  DecimalSkiplistNode start = {.value = INT64_MIN};
  skiplistIterator *iter = slIteratorCreate(t->sl, NULL);
  DecimalSkiplistNode *n;
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
DecimalSkiplistIterator *DecimalSkiplistIterator_New(DecimalSkiplist *ds, void *start) {
  return slIteratorCreate(ds->sl, NULL);
}

DecimalSkiplistNode *DecimalSkiplistIterator_Next(DecimalSkiplistIterator *iter) {
  return slIteratorNext(iter);
}

void DecimalSkiplistIterator_Free(DecimalSkiplistIterator *iter) {
  slIteratorDestroy(iter);
}
