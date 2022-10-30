#ifndef INFO_COMMAND_H
#define INFO_COMMAND_H

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#ifdef __cplusplus
}
#endif

#endif