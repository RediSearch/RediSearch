#ifndef RS_MODULE_H_
#define RS_MODULE_H_

#include "redismodule.h"
#include <stdbool.h>

int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool stopOnConfigErr);

#endif