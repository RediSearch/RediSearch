#ifndef INFO_COMMAND_H
#define INFO_COMMAND_H

#include "redismodule.h"
#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif
int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void replyIndexInfo(RedisModuleCtx *ctx, IndexSpec *sp);
#ifdef __cplusplus
}
#endif
#endif