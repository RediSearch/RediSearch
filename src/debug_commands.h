/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redismodule.h"

#ifdef RS_CLUSTER_ENTERPRISE
#define RS_DEBUG_FLAGS "readonly _proxy-filtered", 0, 0, 0
#else
#define RS_DEBUG_FLAGS "readonly", 0, 0, 0
#endif
#define DEBUG_COMMAND(name) static int name(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)

typedef struct DebugCommandType {
  char *name;
  int (*callback)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
} DebugCommandType;

int RegisterDebugCommands(RedisModuleCommand *debugCommand);
