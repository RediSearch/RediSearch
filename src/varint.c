#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <sys/param.h>
#include "varint.h"

static int msb = (int)(~0ULL << 25);

inline int ReadVarint(Buffer *b) {
  u_char c = BUFFER_READ_BYTE(b);

  int val = c & 127;

  while (c >> 7) {
    ++val;
    c = BUFFER_READ_BYTE(b);
    val = (val << 7) | (c & 127);
  }
  return val;
}

int WriteVarint(int value, BufferWriter *w) {
  // printf("Writing %d @ %zd\n", value, w->buf->offset);
  unsigned char varint[16];
  unsigned pos = sizeof(varint) - 1;
  varint[pos] = value & 127;
  while (value >>= 7)
    varint[--pos] = 128 | (--value & 127);

  return w->Write(w->buf, varint + pos, 16 - pos);
}

size_t varintSize(int value) {
  size_t outputSize = 1;
  while (value > 127) {
    value >>= 7;
    outputSize++;
  }

  return outputSize;
}

VarintVectorIterator VarIntVector_iter(VarintVector *v) {
  VarintVectorIterator ret;
  ret.buf = v;
  ret.index = 0;
  ret.lastValue = 0;
  return ret;
}

inline int VV_HasNext(VarintVectorIterator *vi) { return !BufferAtEnd(vi->buf); }

inline int VV_Next(VarintVectorIterator *vi) {
  if (VV_HasNext(vi)) {
    int i = ReadVarint(vi->buf) + vi->lastValue;
    vi->lastValue = i;
    vi->index++;
    return i;
  }

  return -1;
}

size_t VV_Size(VarintVector *vv) {
  if (vv->type == BUFFER_WRITE) {
    return BufferLen(vv);
  }
  // for readonly buffers the size is the capacity
  return vv->cap;
}

void VVW_Free(VarintVectorWriter *w) {
  w->bw.Release(w->bw.buf);
  free(w);
}

VarintVectorWriter *NewVarintVectorWriter(size_t cap) {
  VarintVectorWriter *w = malloc(sizeof(VarintVectorWriter));
  w->bw = NewBufferWriter(NewMemoryBuffer(cap, BUFFER_WRITE));
  w->lastValue = 0;
  w->nmemb = 0;

  return w;
}

/**
Write an integer to the vector.
@param w a vector writer
@param i the integer we want to write
@retur 0 if we're out of capacity, the varint's actual size otherwise
*/
size_t VVW_Write(VarintVectorWriter *w, int i) {
  size_t n = WriteVarint(i - w->lastValue, &w->bw);
  if (n != 0) {
    w->nmemb += 1;
    w->lastValue = i;
  }
  return n;
}

// Truncate the vector
size_t VVW_Truncate(VarintVectorWriter *w) {
  return w->bw.Truncate(w->bw.buf, 0);
}
