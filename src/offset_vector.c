#include "redisearch.h"
#include "varint.h"
#include "index_result.h"

#include "rmalloc.h"
#include "util/mempool.h"

#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

EmptyOffsetIterator offset_empty_iterator;

///////////////////////////////////////////////////////////////////////////////////////////////

// Rewind an offset vector iterator and start reading it from the beginning
void OffsetVectorIterator::Rewind() {
  lastValue = 0;
  buf.offset = 0;
  br.pos = 0;
}

//---------------------------------------------------------------------------------------------

// Get the next entry, or return RS_OFFSETVECTOR_EOF
uint32_t OffsetVectorIterator::Next(RSQueryTerm **t) {
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

  size_t size = agg->NumChildren();
  iters.reserve(size);
  offsets.reserve(size);
  terms.reserve(size);

  for (int i = 0; i < size; i++) {
    iters.push_back(AggregateOffsetIterator(agg->children[i]));
    RSQueryTerm *temp;
    offsets.push_back(iters.back().Next(&temp));
    terms.push_back(temp);
  }
}

//---------------------------------------------------------------------------------------------

uint32_t AggregateOffsetIterator::Next(RSQueryTerm **t) {
  int minIdx = -1;
  uint32_t minVal = RS_OFFSETVECTOR_EOF;
  int num = res->NumChildren();
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
}

//---------------------------------------------------------------------------------------------

void AggregateOffsetIterator::Rewind() {
  for (int i = 0; i < res->NumChildren(); i++) {
    iters[i].Rewind();
    offsets[i] = 0;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

OffsetIterator::Proxy::~Proxy() {
  if (it != &offset_empty_iterator) {
    delete it;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<OffsetIterator> OffsetVector::Iterate(RSQueryTerm *t) const {
  return std::make_unique<OffsetVectorIterator>(this, t);
}

///////////////////////////////////////////////////////////////////////////////////////////////
