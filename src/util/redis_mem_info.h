/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "redismodule.h"
#include <stdbool.h>

/** Unified Memory Consumption Checker
 *
 * This component provides a thin wrapper around the existing Redis Modules API
 * for memory usage introspection. Its purpose is to unify and simplify memory
 * consumption checks within RediSearch by abstracting direct calls to the
 * underlying Redis memory introspection functions.
 *
 * */

// Get the used memory ratio from Redis modules API.
// If the ratio is 1 or more, we are out of memory.
// The memory limit is calculated against the following:
// OSS : maxmemory
// Enterprise : MIN(max_process_mem, maxmemory)
// GIL must be held before calling this function
static inline bool RedisMemory_isOutOfMemory(void) {
  return RedisModule_GetUsedMemoryRatio() >= 1;
}

// Get the used memory ratio from Redis modules API.
// The ratio is calculated by dividing the used memory by the memory limit.
// OSS : maxmemory
// Enterprise : MIN(max_process_mem, maxmemory)
// GIL must be held before calling this function
static inline float RedisMemory_GetUsedMemoryRatio(void) {
  return RedisModule_GetUsedMemoryRatio();
}

// Get the used memory ratio from Redis server info.
// Same function as before
// GIL must be held before calling this function
// Returns 0 if maxmemory is 0
// TODO: remove this function and use RedisMemory_GetUsedMemoryRatio instead after benchmarking
float RedisMemory_GetUsedMemoryRatioUnified(RedisModuleCtx *ctx);
