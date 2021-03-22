#pragma once

#include "redismodule.h"
#include "string.h"
#include "util/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

int RS_ReplyWithHash(RedisModuleCtx *ctx, char *keyC, arrayof(RedisModuleString *) replyArr);

#ifdef __cplusplus
}
#endif
