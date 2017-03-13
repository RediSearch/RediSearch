#include "offset_vector.h"  // must be included before redisearch.h!!!!
#include "redisearch.h"
#include "varint.h"
#include "rmalloc.h"

RSOffsetIterator *RSOffsetVector_Iterate(RSOffsetVector *v);
uint32_t RSOffsetIterator_Next(RSOffsetIterator *it);

RSOffsetIterator *RSOffsetVector_Iterate(RSOffsetVector *v) {
  RSOffsetIterator *it = rm_new(RSOffsetIterator);
  it->buf = (Buffer){.data = v->data, .offset = v->len, .cap = v->len};
  it->br = NewBufferReader(&it->buf);
  it->lastValue = 0;
  return it;
}

void RSOffsetIterator_Free(RSOffsetIterator *it) {
  rm_free(it);
}

/* Rewind an offset vector iterator and start reading it from the beginning. */
void RSOffsetIterator_Rewind(RSOffsetIterator *it) {
  it->lastValue = 0;
  it->buf.offset = 0;
  it->br.pos = it->buf.data;
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
