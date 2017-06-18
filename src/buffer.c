#include "buffer.h"
#include "rmalloc.h"
#include <assert.h>
#include <sys/param.h>

size_t Buffer_Write(BufferWriter *bw, void *data, size_t len) {

  Buffer *buf = bw->buf;
  if (buf->offset + len > buf->cap) {
    do {
      buf->cap += MIN(1 + buf->cap / 5, 1024 * 1024);
    } while (buf->offset + len > buf->cap);

    buf->data = rm_realloc(buf->data, buf->cap);
    bw->pos = buf->data + buf->offset;
  }
  memcpy(bw->pos, data, len);
  bw->pos += len;
  buf->offset += len;
  return len;
}

/**
Truncate the buffer to newlen. If newlen is 0 - trunacte capacity
*/
size_t Buffer_Truncate(Buffer *b, size_t newlen) {
  if (newlen == 0) {
    newlen = Buffer_Offset(b);
  }

  b->data = rm_realloc(b->data, newlen);
  b->cap = newlen;
  return newlen;
}

BufferWriter NewBufferWriter(Buffer *b) {
  BufferWriter ret = {.buf = b, .pos = b->data + b->offset};
  return ret;
}

BufferReader NewBufferReader(Buffer *b) {
  BufferReader ret = {.buf = b, .pos = 0};
  return ret;
}

/* Initialize a static buffer and fill its data */
void Buffer_Init(Buffer *b, size_t cap) {
  b->cap = cap;
  b->offset = 0;
  b->data = rm_malloc(cap);
}

/**
Allocate a new buffer around data.
*/
Buffer *NewBuffer(size_t cap) {
  Buffer *buf = malloc(sizeof(Buffer));
  Buffer_Init(buf, cap);
  return buf;
}

Buffer *Buffer_Wrap(char *data, size_t len) {
  Buffer *buf = malloc(sizeof(Buffer));
  buf->cap = len;
  buf->offset = 0;
  buf->data = data;
  return buf;
}

void Buffer_Free(Buffer *buf) {
  rm_free(buf->data);
}

/**
Read len bytes from the buffer into data. If offset + len are over capacity
- we do not read and return 0
@return the number of bytes consumed
*/
inline size_t Buffer_Read(BufferReader *br, void *data, size_t len) {
  // no capacity - return 0
  Buffer *b = br->buf;
  if (br->pos + len > b->cap) {
    return 0;
  }

  memcpy(data, b->data + br->pos, len);
  br->pos += len;
  // b->offset += len;

  return len;
}

/**
Consme one byte from the buffer
@return 0 if at end, 1 if consumed
*/
inline size_t Buffer_ReadByte(BufferReader *br, char *c) {
  // if (BufferAtEnd(b)) {
  //     return 0;
  // }
  *c = br->buf->data[br->pos++];
  //++b->buf->offset;
  return 1;
}

/**
Skip forward N bytes, returning the resulting offset on success or the end
position if where is outside bounds
*/
inline size_t Buffer_Skip(BufferReader *br, int bytes) {
  // if overflow - just skip to the end
  Buffer *b = br->buf;
  if (br->pos + bytes > b->cap) {
    br->pos = b->cap;
    // b->offset = b->cap;
    return b->cap;
  }

  br->pos += bytes;
  // b->offset += bytes;
  return br->pos;
}

size_t BufferWriter_Seek(BufferWriter *b, size_t offset) {
  if (offset > b->buf->cap) {
    return b->buf->offset;
  }
  b->pos = b->buf->data + offset;
  b->buf->offset = offset;

  return offset;
}

size_t Buffer_WriteAt(BufferWriter *b, size_t offset, void *data, size_t len) {
  size_t pos = b->buf->offset;
  BufferWriter_Seek(b, offset);

  size_t sz = Buffer_Write(b, data, len);
  BufferWriter_Seek(b, pos);
  return sz;
}
/**
Seek to a specific offset. If offset is out of bounds we seek to the end.
@return the effective seek position
*/
inline size_t Buffer_Seek(BufferReader *br, size_t where) {
  Buffer *b = br->buf;

  br->pos = MIN(where, b->cap);

  return where;
}

inline size_t Buffer_Offset(Buffer *ctx) {
  return ctx->offset;
}

inline size_t BufferReader_Offset(BufferReader *r) {
  return r->pos;
}

inline char *BufferReader_Current(BufferReader *b) {
  return b->buf->data + b->pos;
}

inline int Buffer_AtEnd(Buffer *ctx) {
  return ctx->offset >= ctx->cap;
}

size_t Buffer_Capacity(Buffer *ctx) {
  return ctx->cap;
}
inline int BufferReader_AtEnd(BufferReader *br) {
  return br->pos >= br->buf->offset;
}
