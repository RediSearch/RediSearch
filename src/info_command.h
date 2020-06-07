#include "redismodule.h"
#ifndef INFO_COMMAND_H
#define INFO_COMMAND_H

#ifdef __cplusplus
extern "C" {
#endif
int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
#ifdef __cplusplus
}
#endif
#endif