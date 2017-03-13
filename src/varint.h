#ifndef __VARINT_H__
#define __VARINT_H__

#include <stdlib.h>
#include <sys/types.h>
#include "buffer.h"

size_t varintSize(int value);

int ReadVarint(BufferReader *b);
int WriteVarint(int value, BufferWriter *w);

typedef struct {
  BufferWriter bw;
  // how many members we've put in
  size_t nmemb;
  int lastValue;
} VarintVectorWriter;

#define MAX_VARINT_LEN 5

VarintVectorWriter *NewVarintVectorWriter(size_t cap);
size_t VVW_Write(VarintVectorWriter *w, int i);
size_t VVW_Truncate(VarintVectorWriter *w);
void VVW_Free(VarintVectorWriter *w);

#endif
