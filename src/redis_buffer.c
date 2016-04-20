#include "redis_buffer.h"

size_t redisWriterWrite(Buffer *b, void *data, size_t len) {
    if (b->offset + len > b->cap) {
        do {
            b->cap *= 2;
        } while(b->pos + len > b->data + b->cap);
        
        if (redisWriterTruncate(b, b->cap) == 0) {
            return 0;
        }
        
    }
    
    memcpy(b->pos, data, len);
    b->pos += len;
    b->offset += len;
    return len;
}

size_t redisWriterTruncate(Buffer *b, size_t newlen) {
    // len 0 means "truncate to current len"
    if (newlen == 0) {
        newlen = b->offset;
    }
    
    RedisBufferCtx *ctx = b->ctx;
    
    // resize the data of key
    if (RedisModule_StringTruncate(ctx->key, newlen) == REDISMODULE_ERR) {
        return 0;        
    }
    
    // re-DMA the buffer
    b->data = RedisModule_StringDMA(ctx->key, &b->cap, REDISMODULE_WRITE);
    b->pos = b->data + b->offset;
    b->cap = newlen;
    return b->cap;
    
}

void redisWriterRelease(Buffer *b) {
    //RedisModule_CloseKey(((RedisBufferCtx*)b->ctx)->
    if (b->ctx != NULL) {
        free(b->ctx);
    }
    free(b);
}

Buffer *NewRedisBuffer(RedisModuleCtx *ctx, RedisModuleString *keyname,
                       int bufferMode) {
  
  RedisModule_AutoMemory(ctx);
  
  int flags = REDISMODULE_READ | (bufferMode & BUFFER_WRITE ? REDISMODULE_WRITE : 0);
  
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyname, flags);
  
  if (key == NULL) {
    return NULL;
  }
  
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING &&
      RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    return NULL;
  }
  
  
  size_t len;
  char *data = RedisModule_StringDMA(key, &len, flags);
  Buffer *buf = NewBuffer(data, len, bufferMode);
  
  // set the redis buffer context
  RedisBufferCtx *bcx = calloc(1, sizeof(RedisBufferCtx));
  bcx->ctx = ctx;
  bcx->keyName = keyname;
  bcx->key = key;
  buf->ctx = bcx;
  
  return buf;
}

BufferWriter NewRedisWriter(RedisModuleCtx *ctx, RedisModuleString *keyname) {
    Buffer *buf = NewRedisBuffer(ctx, keyname, BUFFER_WRITE);
    BufferWriter ret = {
        buf,
        redisWriterWrite,
        redisWriterTruncate,
        RedisBufferFree,
    };
    return ret;
}