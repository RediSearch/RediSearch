#include "redismodule.h"
#include "buffer.h"

typedef struct {
    RedisModuleCtx *ctx;
    RedisModuleString *keyName;
    RedisModuleKey *key;
} RedisBufferCtx;

size_t redisWriterWrite(Buffer *b, void *data, size_t len);
size_t redisWriterTruncate(Buffer *b, size_t newlen);
void RedisBufferFree(Buffer *b);


#define REDISBUFFER_DEFAULT_CAPACITY 16

Buffer *NewRedisBuffer(RedisModuleCtx *ctx, RedisModuleString *keyname, int bufferMode);
BufferWriter NewRedisWriter(RedisModuleCtx *ctx,  RedisModuleString *keyname);
