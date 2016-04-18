#include "buffer.h"
#include <sys/param.h>

size_t memwriterWrite(Buffer *b, void *data, size_t len) {
    if (b->offset + len > b->cap) {
        do {
            b->cap *= 2;
        } while(b->pos + len > b->data + b->cap);
        
        
        b->data = realloc(b->data, b->cap);
        b->pos = b->data + b->offset;
    }
    memmove(b->pos, data, len);
    b->pos += len;
    b->offset += len;
    return len;
}
   
   
/**
Truncate the buffer to newlen. If newlen is 0 - trunacte capacity
*/
void memwriterTruncate(Buffer *b, size_t newlen) {
    
    if (newlen == 0) {
        newlen = BufferLen(b);
    }
    
    
    b->data = realloc(b->data, newlen);
    b->cap = newlen;
    b->pos = b->data + b->offset;
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



Buffer *NewBuffer(char *data, size_t len, int type) {
    Buffer *buf = malloc(sizeof(Buffer));
    buf->cap = len;
    buf->data = data;
    buf->pos = data;
    buf->type = type;
    buf->offset = 0;
    return buf;
}

size_t BufferRead(Buffer *b, void *data, size_t len) {
    // no capacity - return 0
    if (BufferLen(b) + len > b->cap) {
        return 0;
    }
    
    data = memcpy(data, b->pos,len);
    b->pos += len;
    b->offset += len;
    return len;
}

size_t BufferReadByte(Buffer *b, char *c) {
    // if (BufferAtEnd(b)) {
    //     return 0;
    // }
    *c = *b->pos++;
    ++b->offset;
    return 1;
}

/**
Seek forward N bytes, returning the resulting offset on success or the end position if where is outside bounds
*/ 
size_t BufferSkip(Buffer *b, int bytes) {
  
  // if overflow - just skip to the end
  if (b->pos + bytes > b->data + b->cap) {
      b->pos = b->data + b->cap;
      b->offset = b->cap; 
      return b->cap;
  }
  
  b->pos += bytes;
  b->offset += bytes;
  return b->offset;
}

size_t BufferSeek(Buffer *b, size_t where) {
  
  where = MIN(where, b->cap);
  b->pos = b->data + where;
  b->offset = where;
  return where;
}
