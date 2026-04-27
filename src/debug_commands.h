/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redismodule.h"
#include  <stdbool.h>
#include <stdatomic.h>
#include <string.h>
#include "result_processor.h"

#define RS_DEBUG_FLAGS "readonly", 0, 0, 0
#define DEBUG_COMMAND(name) static int name(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)

#define REPLY_WITH_LONG_LONG(name, val, len)                  \
  RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name)); \
  RedisModule_ReplyWithLongLong(ctx, val);                    \
  len += 2;

#define REPLY_WITH_DOUBLE(name, val, len)                     \
  RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name)); \
  RedisModule_ReplyWithDouble(ctx, val);                      \
  len += 2;

#define REPLY_WITH_STR(name, len)                             \
  RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name)); \
  len += 1;

#define START_POSTPONED_LEN_ARRAY(array_name) \
  size_t len_##array_name = 0;                \
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN)

#define ARRAY_LEN_VAR(array_name) len_##array_name

#define END_POSTPONED_LEN_ARRAY(array_name) \
  RedisModule_ReplySetArrayLength(ctx, len_##array_name)

typedef struct DebugCommandType {
  char *name;
  int (*callback)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
} DebugCommandType;

int RegisterDebugCommands(RedisModuleCommand *debugCommand);

// Struct used for debugging background indexing
typedef struct BgIndexingDebugCtx {
  int maxDocsTBscanned; // Max number of documents to be scanned before stopping
  int maxDocsTBscannedPause; // Number of documents to be scanned before pausing
  bool pauseBeforeScan; // Whether to pause before scanning
  volatile atomic_bool pause; // Volatile atomic bool to wait for the resume command
  bool pauseOnOOM; // Whether to pause on OOM
  bool pauseBeforeOOMretry; // Whether to pause before the first OOM retry
} BgIndexingDebugCtx;

// Struct used for debugging queries
// Note: unrelated to timeout debugging
typedef struct QueryDebugCtx {
  volatile atomic_bool pause; // Volatile atomic bool to wait for the resume command
  ResultProcessor *debugRP; // Result processor for debugging, supports debugging one query at a time
} QueryDebugCtx;

// General debug context
typedef struct DebugCTX {
  bool debugMode; // Indicates whether debug mode is enabled
  BgIndexingDebugCtx bgIndexing; // Background indexing debug context
  QueryDebugCtx query; // Query debug context
} DebugCTX;

// Should be called after each debug command that changes the debugCtx
// Exception for QueryDebugCtx
void validateDebugMode(DebugCTX *debugCtx);

// QueryDebugCtx API function declarations
bool QueryDebugCtx_IsPaused(void);
void QueryDebugCtx_SetPause(bool pause);
ResultProcessor* QueryDebugCtx_GetDebugRP(void);
void QueryDebugCtx_SetDebugRP(ResultProcessor* debugRP);
bool QueryDebugCtx_HasDebugRP(void);

// Yield counter functions
void IncrementLoadYieldCounter(void);
void IncrementBgIndexYieldCounter(void);

// Indexer sleep before yield functions
unsigned int GetIndexerSleepBeforeYieldMicros(void);
