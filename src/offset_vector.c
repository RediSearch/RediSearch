#include "offset_vector.h"  // must be included before redisearch.h!!!!
#include "redisearch.h"
#include "varint.h"

struct RSOffsetIterator RSOffsetVector_Iterate(RSOffsetVector *v);
uint32_t RSOffsetIterator_Next(RSOffsetVector *vi);

RSOffsetIterator RSOffsetVector_Iterate(RSOffsetVector *v) {
  RSOffsetVector_Iterate ret ret.br = NewBufferReader(v);
  ret.lastValue = 0;
  return ret;
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
