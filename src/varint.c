#include "varint.h"
#include <assert.h>
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

// static int msb = (int)(~0ULL << 25);

int WriteVarintRaw(int value, Buffer *buf) {
  BufferWriter bw = {.buf = buf, .pos = buf->data + buf->offset};
  return WriteVarint(value, &bw);
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
  Buffer_Free(&w->buf);
  free(w);
}

VarintVectorWriter *NewVarintVectorWriter(size_t cap) {
  VarintVectorWriter *w = malloc(sizeof(VarintVectorWriter));
  VVW_Init(w, cap);
  return w;
}

void VVW_Init(VarintVectorWriter *w, size_t cap) {
  w->lastValue = 0;
  w->nmemb = 0;
  Buffer_Init(&w->buf, cap);
}

/**
Write an integer to the vector.
@param w a vector writer
@param i the integer we want to write
@retur 0 if we're out of capacity, the varint's actual size otherwise
*/
size_t VVW_Write(VarintVectorWriter *w, int i) {
  size_t n = WriteVarintRaw(i - w->lastValue, &w->buf);
  if (n != 0) {
    w->nmemb += 1;
    w->lastValue = i;
  }
  return n;
}

// Truncate the vector
size_t VVW_Truncate(VarintVectorWriter *w) {
  return Buffer_Truncate(&w->buf, 0);
}
