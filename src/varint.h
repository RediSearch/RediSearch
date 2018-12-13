#ifndef __VARINT_H__
#define __VARINT_H__

#include <stdlib.h>
#include <sys/types.h>
#include <stdint.h>
#include "buffer.h"
#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif
/* Read an encoded integer from the buffer. It is assumed that the buffer will not overflow */
static inline uint32_t ReadVarint(BufferReader *b) {

  unsigned char c = BUFFER_READ_BYTE(b);

  uint32_t val = c & 127;
  while (c >> 7) {
    ++val;
    c = BUFFER_READ_BYTE(b);
    val = (val << 7) | (c & 127);
  }

  return val;
}

static inline t_fieldMask ReadVarintFieldMask(BufferReader *b) {

  unsigned char c = BUFFER_READ_BYTE(b);

  t_fieldMask val = c & 127;
  while (c >> 7) {
    ++val;
    c = BUFFER_READ_BYTE(b);
    val = (val << 7) | (c & 127);
  }

  return val;
}

size_t WriteVarint(uint32_t value, BufferWriter *w);

size_t WriteVarintFieldMask(t_fieldMask value, BufferWriter *w);

typedef struct {
  Buffer buf;
  // how many members we've put in
  size_t nmemb;
  uint32_t lastValue;
} VarintVectorWriter;

#define MAX_VARINT_LEN 5

VarintVectorWriter *NewVarintVectorWriter(size_t cap);
size_t VVW_Write(VarintVectorWriter *w, uint32_t i);
size_t VVW_Truncate(VarintVectorWriter *w);
void VVW_Free(VarintVectorWriter *w);
void VVW_Init(VarintVectorWriter *w, size_t cap);

static inline void VVW_Cleanup(VarintVectorWriter *w) {
  Buffer_Free(&w->buf);
  memset(&w->buf, 0, sizeof w->buf);
}

static inline void VVW_Reset(VarintVectorWriter *w) {
  w->lastValue = 0;
  w->nmemb = 0;
  w->buf.offset = 0;
}

#define VVW_GetCount(vvw) ((vvw) ? (vvw)->nmemb : 0)
#define VVW_GetByteLength(vvw) ((vvw) ? (vvw)->buf.offset : 0)
#define VVW_GetByteData(vvw) ((vvw) ? (vvw)->buf.data : NULL)
#define VVW_OFFSETVECTOR_INIT(vvw) \
  { .data = VVW_GetByteData(vvw), .len = VVW_GetByteLength(vvw) }

#ifdef __cplusplus
}
#endif
#endif
