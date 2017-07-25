#ifndef __VARINT_H__
#define __VARINT_H__

#include <stdlib.h>
#include <sys/types.h>
#include "buffer.h"

size_t varintSize(int value);

/* Read an encoded integer from the buffer. It is assumed that the buffer will not overflow */
static inline int ReadVarint(BufferReader *b) {

  unsigned char c = BUFFER_READ_BYTE(b);

  int val = c & 127;
  while (c >> 7) {
    ++val;
    c = BUFFER_READ_BYTE(b);
    val = (val << 7) | (c & 127);
  }

  return val;
}

int WriteVarint(int value, BufferWriter *w);

typedef struct {
  Buffer buf;
  // how many members we've put in
  size_t nmemb;
  int lastValue;
} VarintVectorWriter;

#define MAX_VARINT_LEN 5

VarintVectorWriter *NewVarintVectorWriter(size_t cap);
size_t VVW_Write(VarintVectorWriter *w, int i);
size_t VVW_Truncate(VarintVectorWriter *w);
void VVW_Free(VarintVectorWriter *w);

#define VVW_GetCount(vvw) (vvw)->nmemb
#define VVW_GetByteLength(vvw) (vvw)->buf.offset
#define VVW_GetByteData(vvw) (vvw)->buf.data
#define VVW_OFFSETVECTOR_INIT(vvw) \
  { .data = VVW_GetByteData(vvw), .len = VVW_GetByteLength(vvw) }
#endif
