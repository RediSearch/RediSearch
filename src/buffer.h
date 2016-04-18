#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <stdlib.h>
#include <string.h>

#define BUFFER_READ 0
#define BUFFER_WRITE 1

typedef struct {
    char *data;
    size_t cap;
    char *pos;
    int  type;
    size_t offset;
} Buffer;

inline size_t BufferLen(Buffer *ctx) {
    return ctx->offset;
}

inline size_t BufferOffset(Buffer *ctx) {
    return ctx->offset;
}

inline static int BufferAtEnd(Buffer *ctx) {
    return ctx->offset >= ctx->cap;
}
typedef struct {
    Buffer *buf;    
    size_t (*Write)(Buffer *ctx, void *data, size_t len);
    void (*Truncate)(Buffer *ctx, size_t newlen);
    void (*Release)(Buffer *ctx);
} BufferWriter;


typedef struct {    
    Buffer *buf;
    size_t (*Read)(Buffer *ctx, void *data, size_t len);
    size_t (*ReadByte)(Buffer *ctx, char *c);
    void (*Release)(Buffer *ctx);
    size_t (*Skip)(Buffer *ctx, int bytes);
    size_t (*Seek)(Buffer *ctx, size_t offset);
} BufferReader;



size_t memwriterWrite(Buffer *b, void *data, size_t len);
void memwriterTruncate(Buffer *b, size_t newlen);
void membufferRelease(Buffer *b);
size_t membufferSkip(Buffer *b, int bytes);
size_t membufferSeek(Buffer *b, size_t offset);

Buffer *NewBuffer(char *data, size_t len, int bufferMode);
BufferWriter NewBufferWriter(size_t cap);
size_t memreaderReadByte(Buffer *b, char *c);
size_t memreaderRead(Buffer *b, void *data, size_t len) ;
BufferReader NewBufferReader(char *data, size_t len);



#endif