
#pragma once

#include "redismodule.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <arpa/inet.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Simple wrapper over any kind of string
struct RString {
  const char *s;
  size_t n;
};

//---------------------------------------------------------------------------------------------

// This handy macro expands an RSTRING to 2 arguments, the buffer and the length
#define RSTRING_S_N(rs) (rs)->s, (rs)->n

///////////////////////////////////////////////////////////////////////////////////////////////

#define BUFFER_READ 0
#define BUFFER_WRITE 1
#define BUFFER_FREEABLE 2    // if set, we free the buffer on Release
#define BUFFER_LAZY_ALLOC 4  // only allocate memory in a buffer writer on the first write

//---------------------------------------------------------------------------------------------

struct Buffer {
  char *data;
  size_t cap;
  size_t offset;

  Buffer() : data(0), cap(0), offset(0) {}
  Buffer(size_t cap, size_t offset = 0);
  ~Buffer();

  void Reset();

  size_t Offset() const { return offset; }
  size_t Capacity() const { return cap; }
  bool AtEnd() const { return offset >= cap; }

  size_t Truncate(size_t newlen);
  void Grow(size_t extraLen);

  size_t Reserve(size_t n);
};

//---------------------------------------------------------------------------------------------

class RMBuffer : public Buffer {
public:
  RMBuffer(RedisModuleIO *rdb);
  ~RMBuffer();
};

//---------------------------------------------------------------------------------------------

struct BufferReader {
  Buffer *buf;
  size_t pos;

  BufferReader(Buffer *buf, size_t pos = 0) { Set(buf, pos); }

  void Set(Buffer *buf_, size_t pos_ = 0) {
    buf = buf_;
    pos = pos_;
  }

  bool AtEnd() const { return pos >= buf->offset; }
  void ShrinkToSize() { buf->Truncate(0); }
  size_t Offset() const { return pos; }
  size_t Remaining() const { return buf->cap - pos; }
  char *Current() const { return buf->data + pos; }

  // Skip forward N bytes, returning the resulting offset on success or the end position if where is outside bounds
  void Skip(size_t n) { pos += n; }

  size_t Seek(size_t offset);

  char ReadByte() { return buf->data[pos++]; }

  size_t ReadByte(char *c);

  /**
  Read len bytes from the buffer into data. If offset + len are over capacity
  - we do not read and return 0
  @return the number of bytes consumed
  */
  size_t Read(void *data, size_t len) {
    memcpy(data, buf->data + pos, len);
    pos += len;
    return len;
  }

  uint32_t ReadU32() {
    uint32_t u;
    Read(&u, 4);
    return ntohl(u);
  }

  uint16_t ReadU16() {
    uint16_t u;
    Read(&u, 2);
    return ntohs(u);
  }

  uint8_t ReadU8() {
    uint8_t b;
    Read(&b, 1);
    return b;
  }
};

//---------------------------------------------------------------------------------------------

struct BufferWriter {
  Buffer *buf;
  char *pos;

  BufferWriter(Buffer *b) { Set(b); }

  void Set(Buffer *b) {
    buf = b;
    pos = b->data + b->offset;
  }

  size_t Offset() const { return pos - buf->data; }
  char *PtrAt(size_t pos) const { return buf->data + pos; }

  size_t Seek(size_t offset);

  size_t Write(const void *data, size_t len) {
    if (buf->Reserve(len)) {
      pos = buf->data + buf->offset;
    }
    memcpy(pos, data, len);
    pos += len;
    buf->offset += len;
    return len;
  }

  size_t WriteAt(size_t offset, void *data, size_t len);

  // These are convenience functions for writing numbers to/from a network
  size_t WriteU32(uint32_t u) {
    u = htonl(u);
    return Write(&u, sizeof(u));
  }

  size_t WriteU16(uint16_t u) {
    u = htons(u);
    return Write(&u, sizeof(u));
  }

  size_t WriteU8(uint8_t u) { return Write(&u, sizeof(u)); }

};

///////////////////////////////////////////////////////////////////////////////////////////////
