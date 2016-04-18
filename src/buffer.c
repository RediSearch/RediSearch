#include "buffer.h"
#include <sys/param.h>

size_t memwriterWrite(Buffer *b, void *data, size_t len) {
    if (b->pos + len > b->data + b->cap) {
        do {
            b->cap *= 2;
        } while(b->pos + len > b->data + b->cap);
        
        size_t diff = b->pos - b->data;
        b->data = realloc(b->data, b->cap);
        b->pos = b->data + diff;
    }
    memmove(b->pos, data, len);
    b->pos += len;
    return len;
}
   
/**
Truncate the buffer to newlen. If newlen is 0 - trunacte capacity
*/
void memwriterTruncate(Buffer *b, size_t newlen) {
    
    if (newlen == 0) {
        newlen = BufferLen(b);
    }
    
    size_t diff = b->pos - b->data;
    b->data = realloc(b->data, newlen);
    b->cap = newlen;
    b->pos = b->data + diff;
}

void membufferRelease(Buffer *b) {
    // only release the data if we created the buffer
    if (b->type & BUFFER_WRITE) {
        free(b->data);
    }
    b->cap = 0;
    b->data = NULL;
    b->pos = NULL;
    free(b);
}

BufferWriter NewBufferWriter(size_t cap) {
    
    Buffer *b = NewBuffer(malloc(cap), cap, BUFFER_WRITE);
    
    BufferWriter ret = {
        b,
        memwriterWrite,
        memwriterTruncate,
        membufferRelease
    };
    return ret;
}
 size_t BufferLen(Buffer *ctx) {
    return ctx->pos - ctx->data;
}

int BufferAtEnd(Buffer *ctx) {
    return ctx->pos - ctx->data >= ctx->cap;
}

Buffer *NewBuffer(char *data, size_t len, int type) {
    Buffer *buf = malloc(sizeof(Buffer));
    buf->cap = len;
    buf->data = data;
    buf->pos = data;
    buf->type = type;
    return buf;
}

size_t memreaderRead(Buffer *b, void *data, size_t len) {
    // no capacity - return 0
    if (BufferLen(b) + len > b->cap) {
        return 0;
    }
    
    data = memcpy(data, b->pos,len);
    b->pos += len;
    return len;
}

size_t memreaderReadByte(Buffer *b, char *c) {
     if (BufferLen(b) + 1 > b->cap) {
        return 0;
    }
    *c = *b->pos++;
    return 1;
}

/**
Seek forward N bytes, returning the resulting offset on success or the end position if where is outside bounds
*/ 
size_t membufferSkip(Buffer *b, int bytes) {
  
  // if overflow - just skip to the end
  if (b->pos + bytes > b->data + b->cap) {
      b->pos = b->data + b->cap;
      return b->cap;
  }
  
  b->pos += bytes;
  return b->pos - b->data;
}

size_t membufferSeek(Buffer *b, size_t where) {
  
  where = MIN(where, b->cap);
  b->pos = b->data + where;
  return where;
}

BufferReader NewBufferReader(char *data, size_t len) {
    Buffer *buf = NewBuffer(data, len, BUFFER_READ);
    
    BufferReader ret = {
        buf,
        memreaderRead,
        memreaderReadByte,
        membufferRelease,
        membufferSkip,
        membufferSeek
    };
    return ret;
}
