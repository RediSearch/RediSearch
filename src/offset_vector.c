#include "offset_vector.h"  // must be included before redisearch.h!!!!
#include "redisearch.h"
#include "varint.h"
#include "rmalloc.h"

typedef struct {
  Buffer buf;
  BufferReader br;
  uint32_t lastValue;
} _RSOffsetVectorIterator;

uint32_t _ovi_Next(void *ctx);
void _ovi_Rewind(void *ctx);
void _ovi_free(void *ctx) {
  rm_free(ctx);
}

RSOffsetIterator _offsetVector_iterate(RSOffsetVector *v) {
  _RSOffsetVectorIterator *it = rm_new(_RSOffsetVectorIterator);
  it->buf = (Buffer){.data = v->data, .offset = v->len, .cap = v->len};
  it->br = NewBufferReader(&it->buf);
  it->lastValue = 0;

  return (RSOffsetIterator){.Next = _ovi_Next, .Rewind = _ovi_Rewind, .Free = _ovi_free, .ctx = it};
}

typedef struct {
  RSAggregateResult *res;
  RSOffsetIterator *iters;
  uint32_t *offsets;
} _RSAggregateOffsetIterator;

uint32_t _aoi_Next(void *ctx);
void _aoi_Free(void *ctx);
void _aoi_Rewind(void *ctx);

RSOffsetIterator _aggregateResult_iterate(RSAggregateResult *agg) {
  _RSAggregateOffsetIterator *it = rm_new(_RSAggregateOffsetIterator);
  it->res = agg;
  it->iters = calloc(agg->numChildren, sizeof(RSOffsetIterator));
  it->offsets = calloc(agg->numChildren, sizeof(uint32_t));

  for (int i = 0; i < agg->numChildren; i++) {
    it->iters[i] = RSIndexResult_IterateOffsets(agg->children[i]);
    it->offsets[i] = it->iters[i].Next(it->iters[i].ctx);
  }

  return (RSOffsetIterator){.Next = _aoi_Next, .Rewind = _aoi_Rewind, .Free = _aoi_Free, .ctx = it};
}

RSOffsetIterator RSIndexResult_IterateOffsets(RSIndexResult *res) {

  switch (res->type) {
    case RSResultType_Term:
      return _offsetVector_iterate(&res->term.offsets);

    case RSResultType_Intersection:
    case RSResultType_Union:
      return _aggregateResult_iterate(&res->agg);
      break;
  }
}

/* Rewind an offset vector iterator and start reading it from the beginning. */
void _ovi_Rewind(void *ctx) {
  _RSOffsetVectorIterator *it = ctx;
  it->lastValue = 0;
  it->buf.offset = 0;
  it->br.pos = it->buf.data;
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

  int num = it->res->numChildren;
  // find the minimal value that's not EOF
  for (int i = 0; i < num; i++) {
    if (minIdx == -1 || it->offsets[i] < minVal) {
      minIdx = i;
      minVal = it->offsets[i];
    }
  }

  // if we found a minimal iterator - advance it
  if (minIdx != -1) {
    it->offsets[minIdx] = it->iters[minIdx].Next(it->iters[minIdx].ctx);
  }
  // return the minimal value - if we haven't found anything it should be EOF
  printf("%p %d\n", it, minVal);
  return minVal;
}

void _aoi_Free(void *ctx) {
  _RSAggregateOffsetIterator *it = ctx;
  for (int i = 0; i < it->res->numChildren; i++) {
    it->iters[i].Free(it->iters[i].ctx);
  }
  rm_free(it->iters);
  rm_free(it->offsets);
}

void _aoi_Rewind(void *ctx) {
  _RSAggregateOffsetIterator *it = ctx;

  for (int i = 0; i < it->res->numChildren; i++) {
    it->iters[i].Rewind(it->iters[i].ctx);
    it->offsets[i] = 0;
  }
}