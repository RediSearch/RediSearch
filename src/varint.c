#include "varint.h"
#include <assert.h>
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

// static int msb = (int)(~0ULL << 25);

inline int ReadVarint(BufferReader *b) {

  register unsigned char c = BUFFER_READ_BYTE(b);

  register int val = c & 127;
  while (c >> 7) {
    ++val;
    c = BUFFER_READ_BYTE(b);
    val = (val << 7) | (c & 127);
  }

  return val;
}

int WriteVarint(int value, BufferWriter *w) {
  unsigned char varint[16];
  unsigned pos = sizeof(varint) - 1;
  varint[pos] = value & 127;
  while (value >>= 7) varint[--pos] = 128 | (--value & 127);
  // printf("writing %d bytes\n", 16 - pos);
  return Buffer_Write(w, varint + pos, 16 - pos);
}

size_t varintSize(int value) {
  assert(value > 0);
  size_t outputSize = 0;
  do {
    ++outputSize;
  } while (value >>= 7);
  return outputSize;
}

void VVW_Free(VarintVectorWriter *w) {
  Buffer_Free(w->bw.buf);
  free(w->bw.buf);
  free(w);
}

VarintVectorWriter *NewVarintVectorWriter(size_t cap) {
  VarintVectorWriter *w = malloc(sizeof(VarintVectorWriter));
  w->bw = NewBufferWriter(NewBuffer(cap));
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
  return Buffer_Truncate(w->bw.buf, 0);
}
