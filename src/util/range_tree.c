#include "range_tree.h"
#include "sys/param.h"
#include "../rmutil/vector.h"
#include "../index.h"

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

int NumericRange_Add(NumericRange *n, t_docId docId, double value) {

  if (n->size >= n->cap) {
    n->cap = n->cap ? MAX(n->cap * 2, 1024 * 1024) : 2;
    n->entries = realloc(n->entries, n->cap * sizeof(NumericRangeEntry));
  }

  int add = 1;
  for (int i = 0; i < n->size; i++) {
    if (n->entries[i].value == value) {
      add = 0;
      break;
    }
  }
  if (add) {
    if (value < n->minVal) n->minVal = value;
    if (value > n->maxVal) n->maxVal = value;
    ++n->card;
  }

  n->entries[n->size++] = (NumericRangeEntry){.docId = docId, .value = value};
  return n->card;
}

double NumericRange_Split(NumericRange *n, RangeTreeNode **lp, RangeTreeNode **rp) {

  double scores[n->size];
  for (size_t i = 0; i < n->size; i++) {
    scores[i] = n->entries[i].value;
  }

  double split = qselect(scores, n->size, n->size / 2);
  *lp = NewNumericRangeNode(n->size / 2, n->minVal, split);
  *rp = NewNumericRangeNode(n->size / 2, split, n->maxVal);

  for (u_int32_t i = 0; i < n->size; i++) {
    NumericRange_Add(n->entries[i].value < split ? &(*lp)->range : &(*rp)->range,
                     n->entries[i].docId, n->entries[i].value);
  }

  return split;
}

RangeTreeNode *NewNumericRangeNode(size_t cap, double min, double max) {

  RangeTreeNode *n = malloc(sizeof(RangeTreeNode));
  n->isLeaf = 1;
  n->range = (NumericRange){.minVal = min,
                            .maxVal = max,
                            .cap = cap,
                            .size = 0,
                            .card = 0,
                            .entries = calloc(cap, sizeof(NumericRangeEntry))};
  return n;
}

void RangeTreNode_ToBranch(RangeTreeNode *n, double value, RangeTreeNode *left,
                           RangeTreeNode *right) {
  if (n->isLeaf) {
    free(n->range.entries);
  }
  n->isLeaf = 0;
  n->node.left = left;
  n->node.right = right;
  n->node.value = value;
}

#define __isLeaf(n) (n->isLeaf)

int RangeTreeNode_Add(RangeTreeNode *n, t_docId docId, double value) {

  while (n && !__isLeaf(n)) {
    n = value < n->node.value ? n->node.left : n->node.right;
  }

  int card = NumericRange_Add(&n->range, docId, value);

  if (card >= RT_LEAF_CARDINALITY_MAX) {
    // printf("Splitting node with leaf %f..%f\n", n->range.minVal, n->range.maxVal);

    RangeTreeNode *rl, *ll;
    double split = NumericRange_Split(&n->range, &ll, &rl);
    RangeTreNode_ToBranch(n, split, ll, rl);
    return 1;
  }

  return 0;
}

Vector *RangeTreeNode_FindRange(RangeTreeNode *n, double min, double max) {

  Vector *leaves = NewVector(NumericRange *, 8);

  RangeTreeNode *vmin = n, *vmax = n;

  while (vmin == vmax && !__isLeaf(vmin)) {
    vmin = min < vmin->node.value ? vmin->node.left : vmin->node.right;
    vmax = max < vmax->node.value ? vmax->node.left : vmax->node.right;
  }

  Vector *stack = NewVector(RangeTreeNode *, 8);

  // put on the stack all right trees of our path to the minimum node
  while (!__isLeaf(vmin)) {
    if (min < vmin->node.value) {
      Vector_Push(stack, vmin->node.right);
    }
    vmin = min < vmin->node.value ? vmin->node.left : vmin->node.right;
  }
  // put on the stack all left trees of our path to the maximum node
  while (vmax && !__isLeaf(vmax)) {
    if (max >= vmax->node.value) {
      Vector_Push(stack, vmax->node.left);
    }
    vmax = max < vmax->node.value ? vmax->node.left : vmax->node.right;
  }

  Vector_Push(leaves, &vmin->range);
  if (vmin != vmax) Vector_Push(leaves, &vmax->range);

  while (Vector_Size(stack)) {
    RangeTreeNode *n;
    if (!Vector_Pop(stack, &n)) break;
    if (!n) continue;

    if (__isLeaf(n)) {
      Vector_Push(leaves, &n->range);
    } else {

      Vector_Push(stack, n->node.left);
      Vector_Push(stack, n->node.right);
    }
  }

  Vector_Free(stack);

  // printf("found %d leaves\n", Vector_Size(leaves));
  return leaves;
}

void RangeTreeNode_Free(RangeTreeNode *n) {
  if (__isLeaf(n)) {
    free(n->range.entries);
  } else {
    RangeTreeNode_Free(n->node.left);
    RangeTreeNode_Free(n->node.right);
  }
  free(n);
}

RangeTree *NewRangeTree() {
  RangeTree *ret = malloc(sizeof(RangeTree));

  ret->root = NewNumericRangeNode(RT_LEAF_CARDINALITY_MAX, 0, 0);
  ret->numEntries = 0;
  ret->numRanges = 1;
  return ret;
}

int RangeTree_Add(RangeTree *t, t_docId docId, double value) {
  int rc = RangeTreeNode_Add(t->root, docId, value);
  t->numRanges += rc;
  t->numEntries++;
  // printf("range tree added %d, size now %zd docs %zd ranges\n", docId, t->numEntries,
  // t->numRanges);

  return rc;
}

Vector *RangeTree_Find(RangeTree *t, double min, double max) {
  return RangeTreeNode_FindRange(t->root, min, max);
}

void RangeTree_Free(RangeTree *t) {
  RangeTreeNode_Free(t->root);
  free(t);
}

/* Read the next entry from the iterator, into hit *e.
  *  Returns INDEXREAD_EOF if at the end */
int NR_Read(void *ctx, IndexResult *r) {
  NumericRangeIterator *it = ctx;
  if (it->atEOF) {
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  it->lastDocId = it->rng->entries[it->offset++].docId;
  if (it->offset == it->rng->size) {
    it->atEOF = 1;
  }
  // TODO: Filter here
  IndexRecord rec = {.flags = 0xFF, .docId = it->lastDocId, .tf = 0};
  IndexResult_PutRecord(r, &rec);
  return INDEXREAD_OK;
}

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int NR_SkipTo(void *ctx, u_int32_t docId, IndexResult *r) {
  // printf("nr %p skipto %d\n", ctx, docId);
  NumericRangeIterator *it = ctx;
  if (docId > it->rng->entries[it->rng->size - 1].docId) {
    //   / printf("nr got to eof\n");
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  u_int top = it->rng->size - 1, bottom = it->offset;
  u_int i = bottom;
  int newi;

  while (bottom < top) {
    t_docId did = it->rng->entries[i].docId;
    if (did == docId) {
      break;
    }
    if (docId <= did) {
      top = i;
    } else {
      bottom = i;
    }
    newi = (bottom + top) / 2;
    if (newi == i) {
      break;
    }
    i = newi;
  }
  it->offset = i + 1;
  if (it->offset == it->rng->size) {
    it->atEOF = 1;
  }

  it->lastDocId = it->rng->entries[i].docId;
  IndexRecord rec = {.flags = 0xFF, .docId = it->lastDocId, .tf = 0};
  IndexResult_PutRecord(r, &rec);
  // printf("lastDocId: %d, docId%d\n", it->lastDocId, docId);
  if (it->lastDocId == docId) {
    if (it->nf) {
      int match = it->nf ? NumericFilter_Match(it->nf, it->rng->entries[i].value) : 1;
      // printf("nf %f..%f, score %f. match? %d\n", it->nf->min, it->nf->max,
      //     it->rng->entries[i].value, match);

      if (!match) return INDEXREAD_NOTFOUND;
    }
    return INDEXREAD_OK;
  }
  return INDEXREAD_NOTFOUND;
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
  free(self->ctx);
  free(self);
}

/* Return the number of results in this iterator. Used by the query execution
 * on the top iterator */
size_t NR_Len(void *ctx) {
  return ((NumericRangeIterator *)ctx)->rng->size;
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
  ret->ctx = it;

  ret->Free = NR_Free;
  ret->Len = NR_Len;
  ret->HasNext = NR_HasNext;
  ret->LastDocId = NR_LastDocId;
  ret->Read = NR_Read;
  ret->SkipTo = NR_SkipTo;
  return ret;
}

IndexIterator *NewNumericFilterIterator(RangeTree *t, NumericFilter *f) {

  Vector *v = RangeTree_Find(t, f->min, f->max);
  if (!v || Vector_Size(v) == 0) {
    return NULL;
  }

  size_t n = Vector_Size(v);
  // printf("Loaded %zd ranges for range filter!\n", n);
  // NewUnionIterator(IndexIterator **its, int num, DocTable *dt) {
  IndexIterator **its = calloc(n, sizeof(IndexIterator *));

  for (size_t i = 0; i < n; i++) {
    NumericRange *rng;
    Vector_Get(v, i, &rng);

    its[i] = NewNumericRangeIterator(rng, f);
  }
  Vector_Free(v);
  return NewUnionIterator(its, n, NULL);
}

RedisModuleType *NumericIndexType = NULL;
#define NUMERICINDEX_KEY_FMT "nm:%s/%s"

RedisModuleString *fmtNumericIndexKey(RedisSearchCtx *ctx, const char *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, NUMERICINDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

RangeTree *OpenNumericIndex(RedisSearchCtx *ctx, const char *fname) {

  RedisModuleString *s = fmtNumericIndexKey(ctx, fname);
  RedisModuleKey *key = RedisModule_OpenKey(ctx->redisCtx, s, REDISMODULE_READ | REDISMODULE_WRITE);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != NumericIndexType) {
    return NULL;
  }

  /* Create an empty value object if the key is currently empty. */
  RangeTree *t;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    t = NewRangeTree();
    RedisModule_ModuleTypeSetValue(key, NumericIndexType, t);
  } else {
    t = RedisModule_ModuleTypeGetValue(key);
  }
  return t;
}

int NumericIndexType_Register(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = NumericIndexType_RdbLoad,
                               .rdb_save = NumericIndexType_RdbSave,
                               .aof_rewrite = NumericIndexType_AofRewrite,
                               .free = NumericIndexType_Free};

  NumericIndexType = RedisModule_CreateDataType(ctx, "numericdx", 0, &tm);
  if (NumericIndexType == NULL) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
void *NumericIndexType_RdbLoad(RedisModuleIO *rdb, int encver) {
  return NULL;
}
void NumericIndexType_RdbSave(RedisModuleIO *rdb, void *value) {
}
void NumericIndexType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
}
void NumericIndexType_Digest(RedisModuleDigest *digest, void *value) {
}

void NumericIndexType_Free(void *value) {
  RangeTree *t = value;
  RangeTree_Free(t);
}