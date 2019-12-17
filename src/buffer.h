#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <arpa/inet.h>

#define BUFFER_READ 0
#define BUFFER_WRITE 1
#define BUFFER_FREEABLE 2    // if set, we free the buffer on Release
#define BUFFER_LAZY_ALLOC 4  // only allocate memory in a buffer writer on the first write

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Simple wrapper over any kind of string
 */
typedef struct {
  const char *s;
  size_t n;
} RString;

/**
 * This handy macro expands an RSTRING to 2 arguments, the buffer and the length.
 */
#define RSTRING_S_N(rs) (rs)->s, (rs)->n

typedef struct Buffer {
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

#define Buffer_ShrinkToSize(b) Buffer_Truncate(b, 0)

static inline size_t BufferReader_Offset(const BufferReader *br) {
  return br->pos;
}

static inline size_t BufferReader_Remaining(const BufferReader *br) {
  return br->buf->cap - br->pos;
}

static inline size_t Buffer_Offset(const Buffer *ctx) {
  return ctx->offset;
}

#define BufferReader_AtEnd(br) ((br)->pos >= (br)->buf->offset)

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
#define Buffer_Skip(br, n) ((br)->pos += (n))

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

static inline size_t Buffer_Write(BufferWriter *bw, const void *data, size_t len) {

  Buffer *buf = bw->buf;
  if (Buffer_Reserve(buf, len)) {
    bw->pos = buf->data + buf->offset;
  }
  memcpy(bw->pos, data, len);
  bw->pos += len;
  buf->offset += len;
  return len;
}

/**
 * These are convenience functions for writing numbers to/from a network
 */
static inline size_t Buffer_WriteU32(BufferWriter *bw, uint32_t u) {
  u = htonl(u);
  return Buffer_Write(bw, &u, 4);
}
static inline size_t Buffer_WriteU16(BufferWriter *bw, uint16_t u) {
  u = htons(u);
  return Buffer_Write(bw, &u, 2);
}
static inline size_t Buffer_WriteU8(BufferWriter *bw, uint8_t u) {
  return Buffer_Write(bw, &u, 1);
}
static inline uint32_t Buffer_ReadU32(BufferReader *r) {
  uint32_t u;
  Buffer_Read(r, &u, 4);
  return ntohl(u);
}
static inline uint16_t Buffer_ReadU16(BufferReader *r) {
  uint16_t u;
  Buffer_Read(r, &u, 2);
  return ntohs(u);
}
static inline uint8_t Buffer_ReadU8(BufferReader *r) {
  uint8_t b;
  Buffer_Read(r, &b, 1);
  return b;
}

BufferWriter NewBufferWriter(Buffer *b);
BufferReader NewBufferReader(Buffer *b);

#define BufferReader_Current(b) (b)->buf->data + (b)->pos

static inline size_t BufferWriter_Offset(BufferWriter *b) {
  return b->pos - b->buf->data;
}

static inline char *BufferWriter_PtrAt(BufferWriter *b, size_t pos) {
  return b->buf->data + pos;
}

size_t BufferWriter_Seek(BufferWriter *b, size_t offset);
size_t Buffer_WriteAt(BufferWriter *b, size_t offset, void *data, size_t len);

Buffer *Buffer_Wrap(char *data, size_t len);
void Buffer_Free(Buffer *buf);

#ifdef __cplusplus
}
#endif
#endif
