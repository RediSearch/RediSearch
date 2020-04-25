#include "redis_version.h"
#include "redismodule.h"
#include <assert.h>
#include <string.h>

int redisMajorVersion;
int redisMinorVersion;
int redisPatchVersion;

int rlecMajorVersion;
int rlecMinorVersion;
int rlecPatchVersion;
int rlecBuild;

void getRedisVersion() {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleCallReply *reply = RedisModule_Call(ctx, "info", "c", "server");
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char *replyStr = RedisModule_CallReplyStringPtr(reply, &len);

  int n = sscanf(replyStr, "# Server\nredis_version:%d.%d.%d", &redisMajorVersion,
                 &redisMinorVersion, &redisPatchVersion);

  assert(n == 3);

  rlecMajorVersion = -1;
  rlecMinorVersion = -1;
  rlecPatchVersion = -1;
  rlecBuild = -1;
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
