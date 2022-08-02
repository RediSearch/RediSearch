#include "redisearch.h"
#include "varint.h"
#include "index_result.h"

#include "rmalloc.h"
#include "util/mempool.h"

#include <sys/param.h>

// Rewind an offset vector iterator and start reading it from the beginning
void RSOffsetVectorIterator::Rewind() {
  lastValue = 0;
  buf.offset = 0;
  br.pos = 0;
}

//---------------------------------------------------------------------------------------------

// Get the next entry, or return RS_OFFSETVECTOR_EOF
uint32_t RSOffsetVectorIterator::Next(RSQueryTerm **t) {
  if (!br.AtEnd()) {
    lastValue += ReadVarint(br);
    if (t) *t = term;
    return lastValue;
  }

  return RS_OFFSETVECTOR_EOF;
}

///////////////////////////////////////////////////////////////////////////////////////////////

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
    iters[i] = *new AggregateOffsetIterator(agg->children[i]);
    offsets[i] = iters[i].Next(&terms[i]);
  }
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
  delete res;
  rm_free(offsets);
  rm_free(iters);
  rm_free(terms);
}

//---------------------------------------------------------------------------------------------

void AggregateOffsetIterator::Rewind() {
  for (int i = 0; i < res->numChildren; i++) {
    iters[i].Rewind();
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

std::unique_ptr<RSOffsetIterator> RSOffsetVector::Iterate(RSQueryTerm *t) const {
  return std::make_unique<RSOffsetIterator>(new RSOffsetVectorIterator(this, t));
}

///////////////////////////////////////////////////////////////////////////////////////////////
