/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "varint.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include "buffer.h"
#include "rmalloc.h"

// static int msb = (int)(~0ULL << 25);

typedef uint8_t varintBuf[24];

static inline size_t varintEncode(uint32_t value, uint8_t *vbuf) {
  unsigned pos = sizeof(varintBuf) - 1;
  vbuf[pos] = value & 127;
  while (value >>= 7) {
    vbuf[--pos] = 128 | (--value & 127);
  }
  return pos;
}

static size_t varintEncodeFieldMask(t_fieldMask value, uint8_t *vbuf) {
  unsigned pos = sizeof(varintBuf) - 1;
  vbuf[pos] = value & 127;
  while (value >>= 7) {
    vbuf[--pos] = 128 | (--value & 127);
  }
  return pos;
}

#define VARINT_BUF(buf, pos) ((buf) + pos)
#define VARINT_LEN(pos) (sizeof(varintBuf) - (pos))

size_t WriteVarintRaw(uint32_t value, char *buf) {
  varintBuf varint;
  size_t pos = varintEncode(value, varint);
  memcpy(buf, VARINT_BUF(varint, pos), VARINT_LEN(pos));
  return VARINT_LEN(pos);
}

size_t WriteVarintBuffer(uint32_t value, Buffer *buf) {
  varintBuf varint;
  size_t pos = varintEncode(value, varint);
  size_t n = VARINT_LEN(pos);
  Buffer_Reserve(buf, n);
  memcpy(buf->data + buf->offset, VARINT_BUF(varint, pos), n);
  buf->offset += n;
  return n;
}

size_t WriteVarint(uint32_t value, BufferWriter *w) {
  // printf("writing %d bytes\n", 16 - pos);
  varintBuf varint;
  size_t pos = varintEncode(value, varint);
  size_t nw = VARINT_LEN(pos);

  // we assume buffer reserve will not fail
  size_t mem_growth = Buffer_Reserve(w->buf, nw);

  memcpy(BufferWriter_Current(w), VARINT_BUF(varint, pos), nw);

  w->buf->offset += nw;
  w->pos += nw;

  return mem_growth;
}

size_t WriteVarintFieldMask(t_fieldMask value, BufferWriter *w) {
  // printf("writing %d bytes\n", 16 - pos);

  varintBuf varint;
  size_t pos = varintEncodeFieldMask(value, varint);
  size_t nw = VARINT_LEN(pos);
  return Buffer_Write(w, VARINT_BUF(varint, pos), nw);
}

void VVW_Free(VarintVectorWriter *w) {
  Buffer_Free(&w->buf);
  rm_free(w);
}

VarintVectorWriter *NewVarintVectorWriter(size_t cap) {
  VarintVectorWriter *w = rm_malloc(sizeof(VarintVectorWriter));
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
@return 0 if we're out of capacity, the varint's actual size otherwise
*/
size_t VVW_Write(VarintVectorWriter *w, uint32_t i) {
  Buffer_Reserve(&w->buf, 16);
  size_t n = WriteVarintBuffer(i - w->lastValue, &w->buf);
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

uint32_t ReadVarint(BufferReader *b) {

  unsigned char c = BUFFER_READ_BYTE(b);

  uint32_t val = c & 127;
  while (c >> 7) {
    ++val;
    c = BUFFER_READ_BYTE(b);
    val = (val << 7) | (c & 127);
  }

  return val;
}

// Non-inline wrapper functions for FFI to ensure these are available as exported symbols.
uint32_t ReadVarintNonInline(BufferReader *b) {
  return ReadVarint(b);
}

t_fieldMask ReadVarintFieldMaskNonInline(BufferReader *b) {
  return ReadVarintFieldMask(b);
}

t_fieldMask ReadVarintFieldMask(BufferReader *b) {

  unsigned char c = BUFFER_READ_BYTE(b);

  t_fieldMask val = c & 127;
  while (c >> 7) {
    ++val;
    c = BUFFER_READ_BYTE(b);
    val = (val << 7) | (c & 127);
  }

  return val;
}

uint32_t ReadVarintRaw(const char **pos, const char *end) {
  if (*pos >= end) return 0;

  unsigned char c = (unsigned char)**pos;
  (*pos)++;

  uint32_t val = c & 127;
  while (c >> 7) {
    if (*pos >= end) return val; // Prevent buffer overflow
    ++val;
    c = (unsigned char)**pos;
    (*pos)++;
    val = (val << 7) | (c & 127);
  }

  return val;
}

t_fieldMask ReadVarintFieldMaskRaw(const char **pos, const char *end) {
  if (*pos >= end) return 0;

  unsigned char c = (unsigned char)**pos;
  (*pos)++;

  t_fieldMask val = c & 127;
  while (c >> 7) {
    if (*pos >= end) return val; // Prevent buffer overflow
    ++val;
    c = (unsigned char)**pos;
    (*pos)++;
    val = (val << 7) | (c & 127);
  }

  return val;
}
