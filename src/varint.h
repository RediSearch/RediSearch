
#pragma once

#include "buffer.h"
#include "redisearch.h"

#include <stdlib.h>
#include <sys/types.h>
#include <stdint.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Read an encoded integer from the buffer. It is assumed that the buffer will not overflow
inline uint32_t ReadVarint(BufferReader &b) {
  unsigned char c = b.ReadByte();

  uint32_t val = c & 127;
  while (c >> 7) {
    ++val;
    c = b.ReadByte();
    val = (val << 7) | (c & 127);
  }

  return val;
}

//---------------------------------------------------------------------------------------------

inline t_fieldMask ReadVarintFieldMask(BufferReader &b) {
  unsigned char c = b.ReadByte();

  t_fieldMask val = c & 127;
  while (c >> 7) {
    ++val;
    c = b.ReadByte();
    val = (val << 7) | (c & 127);
  }

  return val;
}

//---------------------------------------------------------------------------------------------

size_t WriteVarint(uint32_t value, BufferWriter &w);

size_t WriteVarintFieldMask(t_fieldMask value, BufferWriter &w);

//---------------------------------------------------------------------------------------------

struct VarintVectorWriter {
  Buffer buf;
  size_t nmemb;  // how many members we've put in
  uint32_t lastValue;

  VarintVectorWriter(size_t cap);
  ~VarintVectorWriter();

  size_t GetCount() const { return nmemb; }
  size_t GetByteLength() const { return buf.offset; }
  char *GetByteData() { return buf.data; }

  size_t Write(uint32_t i);
  size_t Truncate();

  void Cleanup() {
    buf.Reset();
  }

  void Reset() {
    lastValue = 0;
    nmemb = 0;
    buf.offset = 0;
  }
};

//---------------------------------------------------------------------------------------------

#if 0

#define MAX_VARINT_LEN 5

#define VVW_OFFSETVECTOR_INIT(vvw) \
  { .data = VVW_GetByteData(vvw), .len = VVW_GetByteLength(vvw) }

#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////
