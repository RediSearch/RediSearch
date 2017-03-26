#ifndef __QINT_H__
#define __QINT_H__

#include <stdint.h>
#include <stdlib.h>
#include "../buffer.h"

size_t qint_encode(BufferWriter *bw, uint32_t arr[], int len);
size_t qint_encode1(BufferWriter *bw, uint32_t i);
size_t qint_encode2(BufferWriter *bw, uint32_t i1, uint32_t i2);
size_t qint_encode3(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3);
size_t qint_encode4(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4);

 void qint_decode(BufferReader *br, uint32_t *arr, int len);
 void qint_decode1(BufferReader *br, uint32_t *i);
 void qint_decode2(BufferReader *br, uint32_t *i, uint32_t *i2);
 void qint_decode3(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3);
 void qint_decode4(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3, uint32_t *i4);

#endif