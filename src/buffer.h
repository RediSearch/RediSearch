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
  char *pos;
} BufferReader;

#define BUFFER_READ_BYTE(b) \
  *b->pos++;                \
  ++b->buf->offset;

void Buffer_Init(Buffer *b, size_t cap);
size_t Buffer_ReadByte(BufferReader *b, char *c);
size_t Buffer_Read(BufferReader *b, void *data, size_t len);
size_t Buffer_Skip(BufferReader *b, int bytes);
size_t Buffer_Seek(BufferReader *b, size_t offset);
size_t Buffer_Offset(Buffer *ctx);
size_t Buffer_Capacity(Buffer *ctx);
int Buffer_AtEnd(Buffer *ctx);

typedef struct {
  Buffer *buf;
  char *pos;

} BufferWriter;

size_t Buffer_Write(BufferWriter *b, void *data, size_t len);
size_t Buffer_Truncate(Buffer *b, size_t newlen);

BufferWriter NewBufferWriter(Buffer *b);
BufferReader NewBufferReader(Buffer *b);

Buffer *NewBuffer(char *data, size_t len);

#endif
