#include "offset_vector.h"  // must be included before redisearch.h!!!!
#include "redisearch.h"
#include "varint.h"

void RSOffsetVector_Iterate(RSOffsetIterator *it, RSOffsetVector *v);
uint32_t RSOffsetIterator_Next(RSOffsetIterator *it);

void RSOffsetVector_Iterate(RSOffsetIterator *it, RSOffsetVector *v) {
  it->br = NewBufferReader(v);
  it->lastValue = 0;
  it->index = 0;
}

inline int offsetVector_HasNext(RSOffsetIterator *vi) {
  return !BufferReader_AtEnd(&vi->br);
}

inline uint32_t RSOffsetIterator_Next(RSOffsetIterator *vi) {
  if (offsetVector_HasNext(vi)) {
    vi->lastValue = ReadVarint(&vi->br) + vi->lastValue;
    return vi->lastValue;
  }

  return RS_OFFSETVECTOR_EOF;
}
