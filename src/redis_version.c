#include "redis_version.h"
#include "redismodule.h"
#include <assert.h>
#include <string.h>

int redisMajorVersion = -1;
int redisMinorVersion = -1;
int redisPatchVersion = -1;

int rlecMajorVersion = -1;
int rlecMinorVersion = -1;
int rlecPatchVersion = -1;
int rlecBuild = -1;

void getRedisVersion() {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleCallReply *reply = RedisModule_Call(ctx, "info", "c", "server");
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char *replyStr = RedisModule_CallReplyStringPtr(reply, &len);

  int n = sscanf(replyStr, "# Server\nredis_version:%d.%d.%d", &redisMajorVersion,
                 &redisMinorVersion, &redisPatchVersion);

  assert(n == 3);

  char *enterpriseStr = strstr(replyStr, "rlec_version:");
  if (enterpriseStr) {
    n = sscanf(enterpriseStr, "rlec_version:%d.%d.%d-%d", &rlecMajorVersion, &rlecMinorVersion,
               &rlecPatchVersion, &rlecBuild);
    if (n != 4) {
      RedisModule_Log(NULL, "warning", "Could not extract enterprise version");
    }
  }

  RedisModule_FreeCallReply(reply);
  RedisModule_FreeThreadSafeContext(ctx);
}
