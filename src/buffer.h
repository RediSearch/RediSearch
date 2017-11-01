#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BUFFER_READ 0
#define BUFFER_WRITE 1
#define BUFFER_FREEABLE 2    // if set, we free the buffer on Release
#define BUFFER_LAZY_ALLOC 4  // only allocate memory in a buffer writer on the first write

typedef struct {
  char *data;
  size_t cap;
  size_t offset;
} Buffer;

typedef struct {
  Buffer *buf;
  size_t pos;
} BufferReader;

#define BUFFER_READ_BYTE(br) br->buf->data[br->pos++]
//++b->buf->offset;

void Buffer_Init(Buffer *b, size_t cap);
size_t Buffer_ReadByte(BufferReader *b, char *c);
/**
Read len bytes from the buffer into data. If offset + len are over capacity
- we do not read and return 0
@return the number of bytes consumed
*/
static inline size_t Buffer_Read(BufferReader *br, void *data, size_t len) {
  // // no capacity - return 0
  // Buffer *b = br->buf;
  // if (br->pos + len > b->cap) {
  //   return 0;
  // }

  memcpy(data, br->buf->data + br->pos, len);
  br->pos += len;
  // b->offset += len;

  return len;
}
size_t Buffer_Seek(BufferReader *b, size_t offset);

static inline size_t BufferReader_Offset(const BufferReader *br) {
  return br->pos;
}

static inline size_t Buffer_Offset(const Buffer *ctx) {
  return ctx->offset;
}

static inline int BufferReader_AtEnd(const BufferReader *br) {
  return br->pos >= br->buf->offset;
}

static inline size_t Buffer_Capacity(const Buffer *ctx) {
  return ctx->cap;
}

static inline int Buffer_AtEnd(const Buffer *ctx) {
  return ctx->offset >= ctx->cap;
}

/**
Skip forward N bytes, returning the resulting offset on success or the end
position if where is outside bounds
*/
static inline size_t Buffer_Skip(BufferReader *br, int bytes) {
  br->pos += bytes;
  return br->pos;
}

typedef struct {
  Buffer *buf;
  char *pos;

} BufferWriter;

size_t Buffer_Truncate(Buffer *b, size_t newlen);

// Ensure that at least extraLen new bytes can be added to the buffer.
// Returns 0 if no realloc was performed. 1 if realloc was performed.
void Buffer_Grow(Buffer *b, size_t extraLen);

static inline size_t Buffer_Reserve(Buffer *buf, size_t n) {
  if (buf->offset + n <= buf->cap) {
    return 0;
  }
  Buffer_Grow(buf, n);
  return 1;
}

static inline size_t Buffer_Write(BufferWriter *bw, void *data, size_t len) {

  Buffer *buf = bw->buf;
  if (Buffer_Reserve(buf, len)) {
    bw->pos = buf->data + buf->offset;
  }
  memcpy(bw->pos, data, len);
  bw->pos += len;
  buf->offset += len;
  return len;
}

BufferWriter NewBufferWriter(Buffer *b);
BufferReader NewBufferReader(Buffer *b);

static inline char *BufferReader_Current(BufferReader *b) {
  return b->buf->data + b->pos;
}

static inline size_t BufferWriter_Offset(BufferWriter *b) {
  return b->pos - b->buf->data;
}

static inline char *BufferWriter_PtrAt(BufferWriter *b, size_t pos) {
  return b->buf->data + pos;
}

size_t BufferWriter_Seek(BufferWriter *b, size_t offset);
size_t Buffer_WriteAt(BufferWriter *b, size_t offset, void *data, size_t len);

Buffer *NewBuffer(size_t len);

Buffer *Buffer_Wrap(char *data, size_t len);
void Buffer_Free(Buffer *buf);

#endif
