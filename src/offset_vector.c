/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <pthread.h>

#include "redisearch.h"
#include "varint.h"
#include "rmalloc.h"
#include "util/mempool.h"
#include <sys/param.h>

/* We have two types of offset vector iterators - for terms and for aggregates. For terms we simply
 * yield the encoded offsets one by one. For aggregates, we merge them on the fly in order.
 * They are both encapsulated in an abstract iterator interface called RSOffsetIterator, with
 * callbacks and context matching the appropriate implementation.
 */

/* A raw offset vector iterator */
typedef struct {
  Buffer buf;
  BufferReader br;
  uint32_t lastValue;
  RSQueryTerm *term;
} _RSOffsetVectorIterator;

typedef struct {
  const RSAggregateResult *res;
  size_t size;
  RSOffsetIterator *iters;
  uint32_t *offsets;
  RSQueryTerm **terms;
  // uint32_t lastOffset; - TODO: Avoid duplicate offsets

} _RSAggregateOffsetIterator;

/* Get the next entry, or return RS_OFFSETVECTOR_EOF */
uint32_t _ovi_Next(void *ctx, RSQueryTerm **t);
/* Rewind the iterator */
void _ovi_Rewind(void *ctx);

/* memory pool for buffer iterators */
static pthread_key_t __offsetIters;
static pthread_key_t __aggregateIters;

static void __attribute__((constructor)) initKeys() {
  pthread_key_create(&__offsetIters, (void(*)(void*))mempool_destroy);
  pthread_key_create(&__aggregateIters, (void(*)(void*))mempool_destroy);
}

/* Free it */
void _ovi_free(void *ctx) {
  mempool_release(pthread_getspecific(__offsetIters), ctx);
}

void *newOffsetIterator() {
  return rm_malloc(sizeof(_RSOffsetVectorIterator));
}
/* Create an offset iterator interface  from a raw offset vector */
RSOffsetIterator RSOffsetVector_Iterate(const RSOffsetVector *v, RSQueryTerm *t) {
  mempool_t *pool = pthread_getspecific(__offsetIters);
  if (!pool) {
    mempool_options options = {
        .initialCap = 8, .alloc = newOffsetIterator, .free = rm_free};
    pool = mempool_new(&options);
    pthread_setspecific(__offsetIters, pool);
  }
  _RSOffsetVectorIterator *it = mempool_get(pool);
  it->buf = (Buffer){.data = v->data, .offset = v->len, .cap = v->len};
  it->br = NewBufferReader(&it->buf);
  it->lastValue = 0;
  it->term = t;

  return (RSOffsetIterator){.Next = _ovi_Next, .Rewind = _ovi_Rewind, .Free = _ovi_free, .ctx = it};
}

/* An aggregate offset iterator yielding offsets one by one */
uint32_t _aoi_Next(void *ctx, RSQueryTerm **term);
void _aoi_Free(void *ctx);
void _aoi_Rewind(void *ctx);

static void *aggiterNew() {
  _RSAggregateOffsetIterator *it = rm_malloc(sizeof(_RSAggregateOffsetIterator));
  it->size = 0;
  it->offsets = NULL;
  it->iters = NULL;
  it->terms = NULL;
  return it;
}

static void aggiterFree(void *p) {
  _RSAggregateOffsetIterator *aggiter = p;
  rm_free(aggiter->offsets);
  rm_free(aggiter->iters);
  rm_free(aggiter->terms);
  rm_free(aggiter);
}

/* Create an iterator from the aggregate offset iterators of the aggregate result */
static RSOffsetIterator _aggregateResult_iterate(const RSAggregateResult *agg) {
  mempool_t *pool = pthread_getspecific(__aggregateIters);
  if (!pool) {
    mempool_options opts = {
        .initialCap = 8, .alloc = aggiterNew, .free = aggiterFree};
    pool = mempool_new(&opts);
    pthread_setspecific(__aggregateIters, pool);
  }
  _RSAggregateOffsetIterator *it = mempool_get(pool);
  it->res = agg;

  if (agg->numChildren > it->size) {
    it->size = agg->numChildren;
    rm_free(it->iters);
    rm_free(it->offsets);
    rm_free(it->terms);
    it->iters = rm_calloc(agg->numChildren, sizeof(RSOffsetIterator));
    it->offsets = rm_calloc(agg->numChildren, sizeof(uint32_t));
    it->terms = rm_calloc(agg->numChildren, sizeof(RSQueryTerm *));
  }

  for (int i = 0; i < agg->numChildren; i++) {
    it->iters[i] = RSIndexResult_IterateOffsets(agg->children[i]);
    it->offsets[i] = it->iters[i].Next(it->iters[i].ctx, &it->terms[i]);
  }

  return (RSOffsetIterator){.Next = _aoi_Next, .Rewind = _aoi_Rewind, .Free = _aoi_Free, .ctx = it};
}
uint32_t _empty_Next(void *ctx, RSQueryTerm **t) {
  return RS_OFFSETVECTOR_EOF;
}
void _empty_Free(void *ctx) {
}
void _empty_Rewind(void *ctx) {
}

RSOffsetIterator _emptyIterator() {
  return (RSOffsetIterator){
      .Next = _empty_Next, .Rewind = _empty_Rewind, .Free = _empty_Free, .ctx = NULL};
}

/* Create the appropriate iterator from a result based on its type */
RSOffsetIterator RSIndexResult_IterateOffsets(const RSIndexResult *res) {

  switch (res->type) {
    case RSResultType_Term:
      return RSOffsetVector_Iterate(&res->term.offsets, res->term.term);

    // virtual and numeric entries have no offsets and cannot participate
    case RSResultType_Virtual:
    case RSResultType_Numeric:
    case RSResultType_Metric:
      return _emptyIterator();

    case RSResultType_Intersection:
    case RSResultType_Union:
    default:
      // if we only have one sub result, just iterate that...
      if (res->agg.numChildren == 1) {
        return RSIndexResult_IterateOffsets(res->agg.children[0]);
      }
      return _aggregateResult_iterate(&res->agg);
      break;
  }
}

/* Rewind an offset vector iterator and start reading it from the beginning. */
void _ovi_Rewind(void *ctx) {
  _RSOffsetVectorIterator *it = ctx;
  it->lastValue = 0;
  it->buf.offset = 0;
  it->br.pos = 0;
}

void _ovi_Free(void *ctx) {
  rm_free(ctx);
}

uint32_t _ovi_Next(void *ctx, RSQueryTerm **t) {
  _RSOffsetVectorIterator *vi = ctx;

  if (!BufferReader_AtEnd(&vi->br)) {
    vi->lastValue = ReadVarint(&vi->br) + vi->lastValue;
    if (t) *t = vi->term;
    return vi->lastValue;
  }

  return RS_OFFSETVECTOR_EOF;
}

uint32_t _aoi_Next(void *ctx, RSQueryTerm **t) {
  _RSAggregateOffsetIterator *it = ctx;

  int minIdx = -1;
  uint32_t minVal = RS_OFFSETVECTOR_EOF;
  uint32_t *offsets = it->offsets;
  register int num = it->res->numChildren;
  // find the minimal value that's not EOF
  for (register int i = 0; i < num; i++) {
    if (offsets[i] < minVal) {
      minIdx = i;
      minVal = offsets[i];
    }
  }

  // if we found a minimal iterator - advance it for the next round
  if (minIdx != -1) {

    // copy the term of that iterator to t if it's not NULL
    if (t) *t = it->terms[minIdx];

    it->offsets[minIdx] = it->iters[minIdx].Next(it->iters[minIdx].ctx, &it->terms[minIdx]);
  }

  return minVal;
}

void _aoi_Free(void *ctx) {
  _RSAggregateOffsetIterator *it = ctx;
  for (int i = 0; i < it->res->numChildren; i++) {
    it->iters[i].Free(it->iters[i].ctx);
  }

  mempool_release(pthread_getspecific(__aggregateIters), ctx);
}

void _aoi_Rewind(void *ctx) {
  _RSAggregateOffsetIterator *it = ctx;

  for (int i = 0; i < it->res->numChildren; i++) {
    it->iters[i].Rewind(it->iters[i].ctx);
    it->offsets[i] = 0;
  }
}
