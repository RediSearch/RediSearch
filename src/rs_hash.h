#pragma once

#include "redismodule.h"
#include "string.h"
#include "rules.h"
#include "util/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

int RS_ReplyWithHash(RedisModuleCtx *ctx, char *keyC, arrayof(void *) replyArr, SchemaRule *rule);

#ifdef __cplusplus
}
#endif
