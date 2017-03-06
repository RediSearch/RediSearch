#ifndef __RS_OFFSET_VECTOR_H__
#define __RS_OFFSET_VECTOR_H__
#include <stdint.h>
#include <stdlib.h>
#include "buffer.h"

typedef Buffer RSOffsetVector;

typedef struct {
  BufferReader br;
  uint32_t index;
  uint32_t lastValue;
} RSOffsetIterator;

#endif