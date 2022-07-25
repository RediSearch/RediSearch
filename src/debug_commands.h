#pragma once

#include "redismodule.h"

int DebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
