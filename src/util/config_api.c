#include "config_api.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"

char *getRedisConfigValue(RedisModuleCtx *ctx, const char* confName) {
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "config", "cc", "get", confName);
  RS_ASSERT(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY);
  if (RedisModule_CallReplyLength(rep) == 0){
    RedisModule_FreeCallReply(rep);
    return NULL;
  }
  RS_ASSERT(RedisModule_CallReplyLength(rep) == 2);
  RedisModuleCallReply *valueRep = RedisModule_CallReplyArrayElement(rep, 1);
  RS_ASSERT(RedisModule_CallReplyType(valueRep) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char* valueRepCStr = RedisModule_CallReplyStringPtr(valueRep, &len);

  char* res = rm_calloc(1, len + 1);
  memcpy(res, valueRepCStr, len);

  RedisModule_FreeCallReply(rep);

  return res;
}