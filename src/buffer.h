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
  char *pos;
  int type;
  size_t offset;

  // opaque context, e.g. redis context and key name for redis buffers
  void *ctx;
} Buffer;

#define BUFFER_READ_BYTE(b) \
  *b->pos++;                \
  ++b->offset;
size_t BufferReadByte(Buffer *b, char *c);
size_t BufferRead(Buffer *b, void *data, size_t len);
size_t BufferSkip(Buffer *b, int bytes);
size_t BufferSeek(Buffer *b, size_t offset);

size_t BufferLen(Buffer *ctx);

size_t BufferOffset(Buffer *ctx);

int BufferAtEnd(Buffer *ctx);

typedef struct {
  Buffer *buf;
  size_t (*Write)(Buffer *ctx, void *data, size_t len);
  size_t (*Truncate)(Buffer *ctx, size_t newlen);
  void (*Release)(Buffer *ctx);
} BufferWriter;

size_t memwriterWrite(Buffer *b, void *data, size_t len);
size_t memwriterTruncate(Buffer *b, size_t newlen);
void membufferRelease(Buffer *b);

Buffer *NewBuffer(char *data, size_t len, int bufferMode);
BufferWriter NewBufferWriter(Buffer *b);

Buffer *NewMemoryBuffer(size_t cap, int type);

#endif