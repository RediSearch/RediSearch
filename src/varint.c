
#include "varint.h"
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <assert.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// static int msb = (int)(~0ULL << 25);

typedef uint8_t varintBuf[24];

//---------------------------------------------------------------------------------------------

static inline size_t varintEncode(uint32_t value, uint8_t *vbuf) {
  unsigned pos = sizeof(varintBuf) - 1;
  vbuf[pos] = value & 127;
  while (value >>= 7) {
    vbuf[--pos] = 128 | (--value & 127);
  }
  return pos;
}

//---------------------------------------------------------------------------------------------

static size_t varintEncodeFieldMask(t_fieldMask value, uint8_t *vbuf) {
  unsigned pos = sizeof(varintBuf) - 1;
  vbuf[pos] = value & 127;
  while (value >>= 7) {
    vbuf[--pos] = 128 | (--value & 127);
  }
  return pos;
}

//---------------------------------------------------------------------------------------------

#define VARINT_BUF(buf, pos) ((buf) + pos)
#define VARINT_LEN(pos) (sizeof(varintBuf) - (pos))

size_t WriteVarintRaw(uint32_t value, char *buf) {
  varintBuf varint;
  size_t pos = varintEncode(value, varint);
  memcpy(buf, VARINT_BUF(varint, pos), VARINT_LEN(pos));
  return VARINT_LEN(pos);
}

//---------------------------------------------------------------------------------------------

size_t Buffer::WriteVarintBuffer(uint32_t value) {
  varintBuf varint;
  size_t pos = varintEncode(value, varint);
  size_t n = VARINT_LEN(pos);
  Reserve(n);
  memcpy(data + offset, VARINT_BUF(varint, pos), n);
  offset += n;
  return n;
}

//---------------------------------------------------------------------------------------------

VarintVectorWriter::VarintVectorWriter(size_t cap)
  : lastValue(0), nmemb(0), buf(cap)
{}

//---------------------------------------------------------------------------------------------

size_t BufferWriter::WriteVarint(uint32_t value) {
  // printf("writing %d bytes\n", 16 - pos);
  varintBuf varint;
  size_t pos = varintEncode(value, varint);
  size_t nw = VARINT_LEN(pos);

  if (buf->Reserve(nw)) {
    this->pos = buf->data + buf->offset;
  }

  memcpy(this->pos, VARINT_BUF(varint, pos), nw);

  buf->offset += nw;
  this->pos += nw;

  return nw;
}

//---------------------------------------------------------------------------------------------

size_t BufferWriter::WriteVarintFieldMask(t_fieldMask value) {
  // printf("writing %d bytes\n", 16 - pos);

  varintBuf varint;
  size_t pos = varintEncodeFieldMask(value, varint);
  size_t nw = VARINT_LEN(pos);
  return Write(VARINT_BUF(varint, pos), nw);
}

//---------------------------------------------------------------------------------------------

/**
Write an integer to the vector.
@param w a vector writer
@param i the integer we want to write
@retur 0 if we're out of capacity, the varint's actual size otherwise
*/
size_t VarintVectorWriter::Write(uint32_t i) {
  buf.Reserve(16);
  size_t n = buf.WriteVarintBuffer(i - lastValue);
  if (n != 0) {
    nmemb += 1;
    lastValue = i;
  }
  return n;
}

//---------------------------------------------------------------------------------------------

// Truncate the vector
size_t VarintVectorWriter::Truncate() {
  return buf.Truncate(0);
}

///////////////////////////////////////////////////////////////////////////////////////////////
