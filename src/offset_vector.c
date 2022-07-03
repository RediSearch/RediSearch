#include "redisearch.h"
#include "varint.h"

#include "rmalloc.h"
#include "util/mempool.h"

#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// We have two types of offset vector iterators - for terms and for aggregates.
// For terms we simply yield the encoded offsets one by one.
// For aggregates, we merge them on the fly in order.
// They are both encapsulated in an abstract iterator interface called RSOffsetIterator, with
// callbacks and context matching the appropriate implementation.

///////////////////////////////////////////////////////////////////////////////////////////////

class RSOffsetVectorIteratorPool : public MemPool {
public:
  RSOffsetVectorIteratorPool() : MemPool(8, 0, true) {}
};

// A raw offset vector iterator
struct RSOffsetVectorIterator : public RSOffsetIterator,
                                public MemPoolObject<RSOffsetVectorIteratorPool> {
  Buffer buf;
  BufferReader br;
  uint32_t lastValue;
  RSQueryTerm *term;

  RSOffsetVectorIterator(const RSOffsetVector *v, RSQueryTerm *t) : buf(v->data, v->len, v->len),
    br(&buf), lastValue(0), term(t) {}

  virtual uint32_t Next(RSQueryTerm **t);
  virtual void Rewind();
};

//---------------------------------------------------------------------------------------------

// Rewind an offset vector iterator and start reading it from the beginning
void RSOffsetVectorIterator::Rewind() {
  lastValue = 0;
  buf.offset = 0;
  br.pos = 0;
}

//---------------------------------------------------------------------------------------------

// Get the next entry, or return RS_OFFSETVECTOR_EOF
uint32_t RSOffsetVectorIterator::Next(RSQueryTerm **t) {
  if (!BufferReader_AtEnd(&br)) {
    lastValue += ReadVarint(&br);
    if (t) *t = term;
    return lastValue;
  }

  return RS_OFFSETVECTOR_EOF;
}

///////////////////////////////////////////////////////////////////////////////////////////////

class AggregateOffsetIteratorPool : public MemPool {
public:
  AggregateOffsetIteratorPool() : MemPool(8, 0, true) {}
};

struct AggregateOffsetIterator : public RSOffsetIterator,
                                   public MemPoolObject<AggregateOffsetIteratorPool> {
  const AggregateResult *res;
  size_t size;
  RSOffsetIterator *iters;
  uint32_t *offsets;
  RSQueryTerm **terms;
  // uint32_t lastOffset; - TODO: Avoid duplicate offsets

  AggregateOffsetIterator() {
    size = 0;
    offsets = NULL;
    iters = NULL;
    terms = NULL;
  }

  AggregateOffsetIterator(const AggregateResult *agg);

  ~AggregateOffsetIterator() {
    rm_free(offsets);
    rm_free(iters);
    rm_free(terms);
  }

  virtual uint32_t Next(RSQueryTerm **t);
  virtual void Rewind();
};

//---------------------------------------------------------------------------------------------

// Create an iterator from the aggregate offset iterators of the aggregate result
AggregateOffsetIterator::AggregateOffsetIterator(const AggregateResult *agg) {
  res = agg; //@@ ownership

  if (agg->numChildren > size) {
    size = agg->numChildren;
    rm_free(iters);
    rm_free(offsets);
    rm_free(terms);
    iters = rm_calloc(agg->numChildren, sizeof(RSOffsetIterator));
    offsets = rm_calloc(agg->numChildren, sizeof(uint32_t));
    terms = rm_calloc(agg->numChildren, sizeof(RSQueryTerm *));
  }

  for (int i = 0; i < agg->numChildren; i++) {
    iters[i] = agg->children[i]->IterateOffsetsInternal();
    offsets[i] = iters[i].Next(&terms[i]);
  }
}

//---------------------------------------------------------------------------------------------

AggregateOffsetIterator AggregateResult::IterateOffsetsInternal() const {
  return new AggregateOffsetIterator(this);
}

//---------------------------------------------------------------------------------------------

uint32_t AggregateOffsetIterator::Next(RSQueryTerm **t) {
  int minIdx = -1;
  uint32_t minVal = RS_OFFSETVECTOR_EOF;
  int num = res->numChildren;
  // find the minimal value that's not EOF
  for (int i = 0; i < num; ++i) {
    if (offsets[i] < minVal) {
      minIdx = i;
      minVal = offsets[i];
    }
  }

  // if we found a minimal iterator - advance it for the next round
  if (minIdx != -1) {
    // copy the term of that iterator to t if it's not NULL
    if (t)
      *t = terms[minIdx];

    offsets[minIdx] = iters[minIdx].Next(&terms[minIdx]);
  }

  return minVal;
}

//---------------------------------------------------------------------------------------------

AggregateOffsetIterator::~AggregateOffsetIterator() {
  for (int i = 0; i < res->numChildren; i++) {
    delete iters[i];
  }
}

//---------------------------------------------------------------------------------------------

void AggregateOffsetIterator::Rewind() {
  for (int i = 0; i < res->numChildren; i++) {
    iters[i].Rewind(iters[i].ctx);
    offsets[i] = 0;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

RSOffsetIterator::Proxy::~Proxy() {
  if (it != &offset_empty_iterator) {
    delete it;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

RSOffsetIterator RSOffsetVector::Iterate(RSQueryTerm *t) const {
  return RSOffsetVectorIterator(this, t);
}

///////////////////////////////////////////////////////////////////////////////////////////////
