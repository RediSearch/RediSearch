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
} _RSOffsetVectorIterator;

typedef struct {
  RSAggregateResult *res;
  size_t size;
  RSOffsetIterator *iters;
  uint32_t *offsets;
  // uint32_t lastOffset; - TODO: Avoid duplicate offsets

} _RSAggregateOffsetIterator;

/* Get the next entry, or return RS_OFFSETVECTOR_EOF */
uint32_t _ovi_Next(void *ctx);
/* Rewind the iterator */
void _ovi_Rewind(void *ctx);

/* memor pool for buffer iterators */
mempool_t *__offsetIters = NULL;
mempool_t *__aggregateIters = NULL;

/* Free it */
void _ovi_free(void *ctx) {
  mempool_release(__offsetIters, ctx);
}

void *newOffsetIterator() {
  return malloc(sizeof(_RSOffsetVectorIterator));
}
/* Create an offset iterator interface  from a raw offset vector */
RSOffsetIterator _offsetVector_iterate(RSOffsetVector *v) {
  if (!__offsetIters) {
    __offsetIters = mempool_new(8, newOffsetIterator, free);
  }
  _RSOffsetVectorIterator *it = mempool_get(__offsetIters);
  it->buf = (Buffer){.data = v->data, .offset = v->len, .cap = v->len};
  it->br = NewBufferReader(&it->buf);
  it->lastValue = 0;

  return (RSOffsetIterator){.Next = _ovi_Next, .Rewind = _ovi_Rewind, .Free = _ovi_free, .ctx = it};
}

/* An aggregate offset iterator yielding offsets one by one */

uint32_t _aoi_Next(void *ctx);
void _aoi_Free(void *ctx);
void _aoi_Rewind(void *ctx);

void *_newAggregateIter() {
  _RSAggregateOffsetIterator *it = malloc(sizeof(_RSAggregateOffsetIterator));
  it->size = 0;
  it->offsets = NULL;
  it->iters = NULL;
  return it;
}
/* Create an iterator from the aggregate offset iterators of the aggregate result */
RSOffsetIterator _aggregateResult_iterate(RSAggregateResult *agg) {
  if (!__aggregateIters) {
    __aggregateIters = mempool_new(8, _newAggregateIter, free);
  }
  _RSAggregateOffsetIterator *it = mempool_get(__aggregateIters);
  it->res = agg;

  if (agg->numChildren > it->size) {
    it->size = agg->numChildren;
    free(it->iters);
    free(it->offsets);
    it->iters = calloc(agg->numChildren, sizeof(RSOffsetIterator));
    it->offsets = calloc(agg->numChildren, sizeof(uint32_t));
  }

  for (int i = 0; i < agg->numChildren; i++) {
    it->iters[i] = RSIndexResult_IterateOffsets(agg->children[i]);
    it->offsets[i] = it->iters[i].Next(it->iters[i].ctx);
  }

  return (RSOffsetIterator){.Next = _aoi_Next, .Rewind = _aoi_Rewind, .Free = _aoi_Free, .ctx = it};
}

/* Create the appropriate iterator from a result based on its type */
RSOffsetIterator RSIndexResult_IterateOffsets(RSIndexResult *res) {

  switch (res->type) {
    case RSResultType_Term:
      return _offsetVector_iterate(&res->term.offsets);

    case RSResultType_Intersection:
    case RSResultType_Union:
    default:
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

uint32_t _ovi_Next(void *ctx) {
  _RSOffsetVectorIterator *vi = ctx;

  if (!BufferReader_AtEnd(&vi->br)) {
    vi->lastValue = ReadVarint(&vi->br) + vi->lastValue;
    return vi->lastValue;
  }

  return RS_OFFSETVECTOR_EOF;
}

uint32_t _aoi_Next(void *ctx) {
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
    it->offsets[minIdx] = it->iters[minIdx].Next(it->iters[minIdx].ctx);
  }

  return minVal;
}

void _aoi_Free(void *ctx) {
  _RSAggregateOffsetIterator *it = ctx;
  for (int i = 0; i < it->res->numChildren; i++) {
    it->iters[i].Free(it->iters[i].ctx);
  }

  mempool_release(__aggregateIters, ctx);
}

void _aoi_Rewind(void *ctx) {
  _RSAggregateOffsetIterator *it = ctx;

  for (int i = 0; i < it->res->numChildren; i++) {
    it->iters[i].Rewind(it->iters[i].ctx);
    it->offsets[i] = 0;
  }
}