#include "redis_buffer.h"
#include "util/logging.h"
#include <sys/param.h>

#define REDISBUFFER_MAX_REALLOC 1024 * 1024 * 2

size_t redisWriterWrite(Buffer *b, void *data, size_t len) {
  // if needed - resize the capacity using redis truncate
  if (b->offset + len > b->cap) {
    do {
      size_t cap = b->cap ? b->cap * 5 / 4 : REDISBUFFER_DEFAULT_CAPACITY;
      if (cap == b->cap) cap++;
      b->cap = MIN(cap, b->cap + REDISBUFFER_MAX_REALLOC);
    } while (b->pos + len > b->data + b->cap);

    if (redisWriterTruncate(b, b->cap) == 0) {
      return 0;
    }
  }

  memcpy(b->pos, data, len);
  b->pos += len;
  b->offset += len;

  // LG_DEBUG("Written %zd bytes to redis buffer cap %zd\n", len, b->cap);
  return len;
}

size_t redisWriterTruncate(Buffer *b, size_t newlen) {
  // len 0 means "truncate to current len"
  if (newlen == 0) {
    newlen = b->offset;
  }
  // no need to truncate if the buffer is empty
  if (b->offset == 0 && b->cap == 0 && newlen == 0) {
    return 0;
  }

  RedisBufferCtx *bctx = b->ctx;
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

void RedisBufferFree(Buffer *b) {
  if (!b) return;
  RedisBufferCtx *bctx = b->ctx;
  if (bctx->key != NULL) {
    RedisModule_CloseKey(bctx->key);
  }
  if (b->ctx != NULL) {
    free(b->ctx);
    b->ctx = NULL;
  }
  free(b);
}

Buffer *NewRedisBuffer(RedisModuleCtx *ctx, RedisModuleString *keyname, int bufferMode) {
  int flags = REDISMODULE_READ | (bufferMode & BUFFER_WRITE ? REDISMODULE_WRITE : 0);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyname, flags);

  if (key == NULL) {
    return NULL;
  }

  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING &&
      RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    return NULL;
  }

  size_t len = 0;

  char *data = NULL;
  if (!(bufferMode & BUFFER_LAZY_ALLOC)) {

    // if we need to write to an empty buffer, allocate a new string
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
      RedisModule_StringTruncate(key, REDISBUFFER_DEFAULT_CAPACITY);
    }
    data = RedisModule_StringDMA(key, &len, flags);
    // printf("Opened redis buffer for %s, len %zd\n", RedisModule_StringPtrLen(keyname, NULL),
    // len);
  } else {

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
      data = RedisModule_StringDMA(key, &len, flags);
    }
  }
  // printf("Opened redis buffer for %s, len %zd\n", RedisModule_StringPtrLen(keyname, NULL), len);
  Buffer *buf = NewBuffer(data, len, bufferMode);

  // set the redis buffer context
  RedisBufferCtx *bcx = calloc(1, sizeof(RedisBufferCtx));
  bcx->ctx = ctx;
  bcx->keyName = keyname;
  bcx->key = key;
  buf->ctx = bcx;

  return buf;
}

BufferWriter NewRedisWriter(RedisModuleCtx *ctx, RedisModuleString *keyname, int lazy) {
  Buffer *buf = NewRedisBuffer(ctx, keyname, BUFFER_WRITE | (lazy ? BUFFER_LAZY_ALLOC : 0));
  BufferWriter ret = {
      buf, redisWriterWrite, redisWriterTruncate, RedisBufferFree,
  };
  return ret;
}
