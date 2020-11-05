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
#include "util/skiplist.h"
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
void NumericRangeIterator_OnReopen(void *privdata) {
}

size_t NumericRange_Add(NumericRange *range, t_docId docId, double value, int checkCard) {

  int add = 0;
  if (checkCard) {
    add = 1;
    size_t card = range->card;
    for (int i = 0; i < array_len(range->values); i++) {

      if (range->values[i].value == value) {
        add = 0;
        range->values[i].appearances++;
        break;
      }
    }
  }
  RS_LOG_ASSERT(value >= range->minVal, "value cannot be smaller than minVal")
  if (value > range->maxVal)
    range->maxVal = value;
  if (add) {
    if (range->card < range->splitCard) {
      CardinalityValue val = {.value = value, .appearances = 1};
      range->values = array_append(range->values, val);
      range->unique_sum += value;
    }
    ++range->card;
  }

  size_t size = InvertedIndex_WriteNumericEntry(range->entries, docId, value);
  range->invertedIndexSize += size;
  return size;
}

// TODO : remove cardinality functionality
static NumericRange *NewNumericRange(size_t cap, double min, double max, size_t splitCard) {
  NumericRange *range = rm_malloc(sizeof(NumericRange));

  range->minVal = min;
  range->maxVal = max;
  range->unique_sum = 0;
  range->card = 0;
  range->splitCard = splitCard;
  range->values = array_new(CardinalityValue, 1);
  range->entries = NewInvertedIndex(Index_StoreNumeric, 1);
  range->invertedIndexSize = 0;

  return range;
}

double NumericRange_Split(NumericRange *range, NumericRange **lp, NumericRange **rp,
                          NRN_AddRv *rv) {

  double split = (range->unique_sum) / (double)range->card;

  // printf("split point :%f\n", split);
  *lp = NewNumericRange(range->entries->numDocs / 2 + 1, range->minVal, split,
                    MIN(NR_MAXRANGE_CARD, 1 + range->splitCard * NR_EXPONENT));
  *rp = NewNumericRange(range->entries->numDocs / 2 + 1, split, range->maxVal,
                    MIN(NR_MAXRANGE_CARD, 1 + range->splitCard * NR_EXPONENT));

  RSIndexResult *res = NULL;
  IndexReader *ir = NewNumericReader(NULL, range->entries, NULL);
  while (INDEXREAD_OK == IR_Read(ir, &res)) {
    rv->sz += NumericRange_Add((res->num.value < split) ? *lp : *rp, res->docId,
                               res->num.value, 1);
    ++rv->numRecords;
  }
  IR_Free(ir);

  // printf("Splitting node %p %f..%f, card %d size %d\n", n, range->minVal, range->maxVal, range->card,
  //        range->entries->numDocs);
  // printf("left node: %d, right: %d\n", (*lp)->range->entries->numDocs,
  //        (*rp)->range->entries->numDocs);
  return split;
}

void NumericRange_Free(NumericRange *range) {
  if (!range) return;

  InvertedIndex_Free(range->entries);
  array_free(range->values);
  rm_free(range);
}

int NumericSkiplistCompare(NumericRange *a, NumericRange *b) {
  return a->minVal > b->minVal ? 1 : a->minVal < b->minVal ? -1 : 0;
}

void NumericSkiplistElementDtor(NumericRange *range) {
  NumericRange_Free(range);
}

uint64_t numericTreesUniqueId = 0;

/* Create a new numeric range skiplist */
NumericRangeSkiplist *NewNumericRangeSkiplist() {
  NumericRangeSkiplist *ret = rm_malloc(sizeof(NumericRangeSkiplist));

  ret->sl = slCreate((slCmpFunc)&NumericSkiplistCompare,
                     (slDestroyFunc)&NumericSkiplistElementDtor);
  NumericRange *range = NewNumericRange(INIT_INVIDX_CAP, NF_NEGATIVE_INFINITY, NF_INFINITY, INIT_INVIDX_CARD);
  slInsert(ret->sl, range);
  ret->numEntries = 0;
  ret->numRanges = 1;
  ret->revisionId = 0;
  ret->lastDocId = 0;
  ret->uniqueId = numericTreesUniqueId++;
  return ret;
}

NRN_AddRv NumericRangeSkiplist_Add(NumericRangeSkiplist *nrsl, t_docId docId, double value) {

  NRN_AddRv rv = {.sz = 0, .changed = 0, .numRecords = 0};
  // Do not allow duplicate entries. This might happen due to indexer bugs and we need to protect
  // from it
  if (docId <= nrsl->lastDocId) {
    return rv;
  }
  nrsl->lastDocId = docId;

  NumericRange val = {.minVal = value};
  NumericRange *range = slFind(nrsl->sl, &val);


  rv.sz = (uint32_t)NumericRange_Add(range, docId, value, 1);
  ++rv.numRecords;
  int card = range->card;
  // printf("Added %d %f to node %f..%f, card now %zd, size now %zd\n", docId, value,
  // range->minVal,
  //        range->maxVal, card, range->entries->numDocs);
  if (card >= range->splitCard || (range->entries->numDocs > NR_MAXRANGE_SIZE && card > 1)) {
    NumericRange *lower, *higher;
    // split this node but don't delete its range
    NumericRange_Split(range, &lower, &higher, &rv);

    // Delete old range
    rv.sz -= range->invertedIndexSize;
    rv.numRecords -= range->entries->numDocs;
    slDelete(nrsl->sl, range, NULL);

    // Insert new splitted ranges
    slInsert(nrsl->sl, lower);
    slInsert(nrsl->sl, higher);

    rv.changed = 1;
  }

  // rc != 0 means the skiplist has changed, and concurrent iteration is not allowed now
  // we increment the revision id of the skiplist, so currently running query iterators on it
  // will abort the next time they get execution context
  if (rv.changed) {
    nrsl->revisionId++;
  }
  nrsl->numRanges += rv.changed;
  nrsl->numEntries++;

  return rv;
}

Vector *NumericRangeSkiplist_Find(NumericRangeSkiplist *nrsl, double min, double max) {
  Vector *v = NewVector(NumericRange *, 8);

  NumericRange *range;
  NumericRange start = {.minVal = min};
  NumericSkiplistIterator *iter = NumericSkiplistIterator_New(nrsl, &start);
  while ((range = NumericSkiplistIterator_Next(iter))) {
    if (range->minVal <= max)
      Vector_Push(v, range);
    else
      break;
  }
  NumericSkiplistIterator_Free(iter);

  return v;
}

void NumericRangeSkiplist_Free(NumericRangeSkiplist *nrsl) {
  slFree(nrsl->sl);
  rm_free(nrsl);
}

IndexIterator *NewNumericRangeIterator(const IndexSpec *sp, NumericRange *nr,
                                       const NumericFilter *f) {

  // if this range is at either end of the filter, we need to check each record
  if (NumericFilter_Match(f, nr->minVal) && NumericFilter_Match(f, nr->maxVal) &&
      f->geoFilter == NULL) {
    // make the filter NULL so the reader will ignore it
    f = NULL;
  }
  IndexReader *ir = NewNumericReader(sp, nr->entries, f);

  return NewReadIterator(ir);
}

/* Create a union iterator from the numeric filter, over all the sub-ranges in the skiplist that fit
 * the filter */
IndexIterator *createNumericIterator(const IndexSpec *sp, NumericRangeSkiplist *nrsl,
                                     const NumericFilter *f) {

  Vector *v = NumericRangeSkiplist_Find(nrsl, f->min, f->max);
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
    IndexIterator *it = NewNumericRangeIterator(sp, rng, f);
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

    its[i] = NewNumericRangeIterator(sp, rng, f);
  }
  Vector_Free(v);

  IndexIterator *it = NewUnionIterator(its, n, NULL, 1, 1);

  return it;
}

RedisModuleType *NumericIndexType = NULL;
#define NUMERICINDEX_KEY_FMT "nm:%s/%s"
#define DECIMALINDEX_KEY_FMT "dc:%s/%s"

RedisModuleString *fmtRedisNumericIndexKey(RedisSearchCtx *ctx, const char *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, NUMERICINDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

RedisModuleString *fmtRedisDecimalIndexKey(RedisSearchCtx *ctx, const char *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, DECIMALINDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

static NumericRangeSkiplist *openNumericKeysDict(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                             int write) {
  KeysDictValue *kdv = dictFetchValue(ctx->spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->dtor = (void (*)(void *))NumericRangeSkiplist_Free;
  kdv->p = NewNumericRangeSkiplist();
  dictAdd(ctx->spec->keysDict, keyName, kdv);
  return kdv->p;
}

struct indexIterator *NewNumericFilterIterator(RedisSearchCtx *ctx, const NumericFilter *flt,
                                               ConcurrentSearchCtx *csx, FieldType forType) {
  RedisModuleString *s = IndexSpec_GetFormattedKeyByName(ctx->spec, flt->fieldName, forType);
  if (!s) {
    return NULL;
  }
  RedisModuleKey *key = NULL;
  NumericRangeSkiplist *nrsl = openNumericKeysDict(ctx, s, 0);

  if (!nrsl) {
    return NULL;
  }

  IndexIterator *it = createNumericIterator(ctx->spec, nrsl, flt);
  if (!it) {
    return NULL;
  }

  if (csx) {
    NumericUnionCtx *uc = rm_malloc(sizeof(*uc));
    uc->lastRevId = nrsl->revisionId;
    uc->it = it;
    ConcurrentSearch_AddKey(csx, NumericRangeIterator_OnReopen, uc, rm_free);
  }
  return it;
}

NumericRangeSkiplist *OpenNumericIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                   RedisModuleKey **idxKey) {

  NumericRangeSkiplist *nrsl;
  if (!ctx->spec->keysDict) {
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
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
      nrsl = NewNumericRangeSkiplist();
      RedisModule_ModuleTypeSetValue((*idxKey), NumericIndexType, nrsl);
    } else {
      nrsl = RedisModule_ModuleTypeGetValue(*idxKey);
    }
  } else {
    nrsl = openNumericKeysDict(ctx, keyName, 1);
  }
  return nrsl;
}

unsigned long __numericIndex_memUsageCallback(NumericRange *range) {
  unsigned long sz = sizeof(NumericRange);
  sz += array_len(range->values) * sizeof(*range->values);
  sz += InvertedIndex_MemUsage(range->entries);
  return sz;
}

unsigned long NumericIndexType_MemUsage(const void *value) {
  unsigned long ret = sizeof(NumericRangeSkiplist);

  const NumericRangeSkiplist *nrsl = value;
  NumericSkiplistIterator *iter = NumericSkiplistIterator_New(nrsl, NULL);
  NumericRange *range;
  while ((range = NumericSkiplistIterator_Next(iter))) {
    ret += sizeof(NumericRange);
    ret += array_len(range->values) * sizeof(*range->values);
    ret += InvertedIndex_MemUsage(range->entries);
  }
  NumericSkiplistIterator_Free(iter);

  return ret;
}

/*********************************************************************
 *                             Ignore RDB
 *********************************************************************/

#define NUMERIC_INDEX_ENCVER 1

int NumericIndexType_Register(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = NumericIndexType_RdbLoad,
                               .rdb_save = NumericIndexType_RdbSave, // TODO
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
    entries = array_append(entries, cur);
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
  NumericRangeSkiplist *nrsl = NewNumericRangeSkiplist();

  // now push them in order into the skiplist
  for (size_t i = 0; i < numEntries; i++) {
    NumericRangeSkiplist_Add(nrsl, entries[i].docId, entries[i].value);
  }
  array_free(entries);
  return nrsl;
}

void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value) {	
  IndexReader *ir;
  RSIndexResult *res;	
  NumericRange *range;
  NumericRangeSkiplist *nrsl = value;	

  NumericSkiplistIterator *iter = NumericSkiplistIterator_New(nrsl, NULL);
  while ((range = NumericSkiplistIterator_Next(iter))) {
    ir = NewNumericReader(NULL, range->entries, NULL);	

    while (INDEXREAD_OK == IR_Read(ir, &res)) {	
      RedisModule_SaveUnsigned(rdb, res->docId);	
      RedisModule_SaveDouble(rdb, res->num.value);	
    }	
    IR_Free(ir);
  }
  NumericSkiplistIterator_Free(iter);

  // Save the final record	
  RedisModule_SaveUnsigned(rdb, 0);	
}	

void NumericIndexType_Digest(RedisModuleDigest *digest, void *value) {
}

void NumericIndexType_Free(void *value) {
  NumericRangeSkiplist *nrsl = value;
  NumericRangeSkiplist_Free(nrsl);
}

/****************************************************
 * Numeric skiplist iterator
 ***************************************************/

NumericSkiplistIterator *NumericSkiplistIterator_New(const NumericRangeSkiplist *nrsl,
                                                     NumericRange *start) {
  return slIteratorCreate(nrsl->sl, start);
}

NumericRange *NumericSkiplistIterator_Next(NumericSkiplistIterator *iter) {
  return slIteratorNext(iter);
}

void NumericSkiplistIterator_Free(NumericSkiplistIterator *iter) {
  slIteratorDestroy(iter);
}
