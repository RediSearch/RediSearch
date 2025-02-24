/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redismodule.h"
#include  <stdbool.h>
#include <stdatomic.h>

#define RS_DEBUG_FLAGS 0, 0, 0
#define DEBUG_COMMAND(name) static int name(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)

typedef struct DebugCommandType {
  char *name;
  int (*callback)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
} DebugCommandType;

int RegisterDebugCommands(RedisModuleCommand *debugCommand);

// Struct used for debugging background indexing
typedef struct BgIndexingDebugCtx {
  int maxDocsTBscanned;
  int maxDocsTBscannedPause;
  bool pauseBeforeScan;
  volatile atomic_bool pause;
} BgIndexingDebugCtx;

// General debug context
typedef struct DebugCTX {
  bool debugMode;
  BgIndexingDebugCtx bgIndexing;
} DebugCTX;

// Should be called after each debug command that changes the debugCtx
void validateDebugMode(DebugCTX *debugCtx);
