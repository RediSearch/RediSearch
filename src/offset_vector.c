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
  RSOffsetVectorIteratorPool() : MemPool(8, 0, true)
};

// A raw offset vector iterator
struct RSOffsetVectorIterator : public RSOffsetIterator, 
                                public PoolObject<RSOffsetVectorIteratorPool> {
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

// Create an offset iterator interface  from a raw offset vector
RSOffsetIterator RSOffsetVector::Iterate(RSQueryTerm *t) const {
  return new RSOffsetVectorIterator(this, t);
}

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

class RSAggregateOffsetIteratorPool : public MemPool {
public:
  RSAggregateOffsetIteratorPool() : MemPool(8, 0, true)
};

struct RSAggregateOffsetIterator : public RSOffsetIterator,
                                   public PoolObject<RSAggregateOffsetIteratorPool> {
  const RSAggregateResult *res;
  size_t size;
  RSOffsetIterator *iters;
  uint32_t *offsets;
  RSQueryTerm **terms;
  // uint32_t lastOffset; - TODO: Avoid duplicate offsets

  RSAggregateOffsetIterator() {
    size = 0;
    offsets = NULL;
    iters = NULL;
    terms = NULL;
  }

  RSAggregateOffsetIterator(const RSAggregateResult *agg);

  ~RSAggregateOffsetIterator() {
    rm_free(offsets);
    rm_free(iters);
    rm_free(terms);
  }

  virtual uint32_t Next(RSQueryTerm **t);
  virtual void Rewind();
};

//---------------------------------------------------------------------------------------------

// Create an iterator from the aggregate offset iterators of the aggregate result
RSAggregateOffsetIterator::RSAggregateOffsetIterator(const RSAggregateResult *agg) {
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
    iters[i] = agg->children[i]->IterateOffsets();
    offsets[i] = iters[i].Next(&terms[i]);
  }
}

//---------------------------------------------------------------------------------------------

RSAggregateOffsetIterator RSAggregateResult::IterateOffsets() const {
  return new RSAggregateOffsetIterator(this);
}

//---------------------------------------------------------------------------------------------

uint32_t RSAggregateOffsetIterator::Next(RSQueryTerm **t) {
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

RSAggregateOffsetIterator::~RSAggregateOffsetIterator() {
  for (int i = 0; i < res->numChildren; i++) {
    delete iters[i];
  }
}

//---------------------------------------------------------------------------------------------

void RSAggregateOffsetIterator::Rewind() {
  for (int i = 0; i < res->numChildren; i++) {
    iters[i].Rewind(iters[i].ctx);
    offsets[i] = 0;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

class RSOffsetEmptyIterator : public RSOffsetIterator {
};

RSOffsetEmptyIterator offset_empty_iterator;

//---------------------------------------------------------------------------------------------

RSOffsetIterator::Proxy::~Proxy() { 
  if (it != &offset_empty_iterator) {
    delete it;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Create the appropriate iterator from a result based on its type
RSOffsetIterator::Proxy RSIndexResult::IterateOffsets() const {
  switch (type) {
  case RSResultType_Term:
    return term.offsets.Iterate(term.term);

  // virtual and numeric entries have no offsets and cannot participate
  case RSResultType_Virtual:
  case RSResultType_Numeric:
    return &offset_empty_iterator;

  case RSResultType_Intersection:
  case RSResultType_Union:
  default:
    // if we only have one sub result, just iterate that...
    if (agg.numChildren == 1) {
      return agg.children[0].IterateOffsets();
    }
    return agg.IterateOffsets();
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
