/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "buffer.h"
#include "rmalloc.h"
#include "qint.h"

/* internal function to encode just one number out of a larger array at a given offset (<=4) */
static inline size_t __qint_encode(char *leading, BufferWriter *bw, uint32_t i, int offset) {
  size_t ret = 0;
  // byte size counter
  char n = -1;
  do {
    // write one byte into the buffer and advance the byte count
    ret += Buffer_Write(bw, (unsigned char *)&i, 1);
    n++;
    // shift right until we have no more bigger bytes that are non zero
    i >>= 8;
  } while (i);
  // encode the bit length of our integer into the leading byte.
  // 0 means 1 byte, 1 - 2 bytes, 2 - 3 bytes, 3 - 4 bytes.
  // we encode it at the i*2th place in the leading byte
  *leading |= n << (offset * 2);
  return ret;
}

/* Encode two integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode2(BufferWriter *bw, uint32_t i1, uint32_t i2) {
  size_t ret = 0;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  ret += Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i1, 0);
  ret += __qint_encode(&leading, bw, i2, 1);
  ret += Buffer_WriteAt(bw, pos, &leading, 1);
  return ret;
}

/* Encode three integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode3(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3) {
  size_t ret = 0;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  ret += Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i1, 0);
  ret += __qint_encode(&leading, bw, i2, 1);
  ret += __qint_encode(&leading, bw, i3, 2);
  ret += Buffer_WriteAt(bw, pos, &leading, 1);
  return ret;
}

/* Encode 4 integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode4(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4) {
  size_t ret = 0;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  ret += Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i1, 0);
  ret += __qint_encode(&leading, bw, i2, 1);
  ret += __qint_encode(&leading, bw, i3, 2);
  ret += __qint_encode(&leading, bw, i4, 3);
  ret += Buffer_WriteAt(bw, pos, &leading, 1);

  return ret;
}

#define QINT_DECODE_VALUE(lval, bits, ptr, nused) \
  do {                                            \
    switch (bits) {                               \
      case 0:                                     \
        lval = *(uint8_t *)ptr;                   \
        nused += 1;                               \
        break;                                    \
      case 1:                                     \
        lval = *(uint16_t *)ptr;                  \
        nused += 2;                               \
        break;                                    \
      case 2:                                     \
        lval = *(uint32_t *)ptr & 0x00FFFFFF;     \
        nused += 3;                               \
        break;                                    \
      default:                                    \
        lval = *(uint32_t *)ptr;                  \
        nused += 4;                               \
        break;                                    \
    }                                             \
  } while (0)

#define QINT_DECODE_MULTI(lval, pos, p, total) \
  QINT_DECODE_VALUE(lval, ((*p >> (pos * 2)) & 0x03), (p + total + 1), total)

QINT_API size_t qint_decode2(BufferReader *br, uint32_t *i, uint32_t *i2) {
  const uint8_t *p = (uint8_t *)BufferReader_Current(br);
  size_t total = 0;
  QINT_DECODE_MULTI(*i, 0, p, total);
  QINT_DECODE_MULTI(*i2, 1, p, total);
  Buffer_Skip(br, total + 1);
  return total + 1;
}

QINT_API size_t qint_decode3(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3) {
  const uint8_t *p = (uint8_t *)BufferReader_Current(br);
  size_t total = 0;
  QINT_DECODE_MULTI(*i, 0, p, total);
  QINT_DECODE_MULTI(*i2, 1, p, total);
  QINT_DECODE_MULTI(*i3, 2, p, total);
  Buffer_Skip(br, total + 1);
  return total + 1;
}

QINT_API size_t qint_decode4(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3,
                             uint32_t *i4) {
  const uint8_t *p = (uint8_t *)BufferReader_Current(br);
  size_t total = 0;
  QINT_DECODE_MULTI(*i, 0, p, total);
  QINT_DECODE_MULTI(*i2, 1, p, total);
  QINT_DECODE_MULTI(*i3, 2, p, total);
  QINT_DECODE_MULTI(*i4, 3, p, total);
  Buffer_Skip(br, total + 1);
  return total + 1;
}

#undef QINT_DECODE_MULTI
#undef QINT_DECODE_VALUE
