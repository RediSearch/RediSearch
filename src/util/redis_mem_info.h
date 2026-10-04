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


// Each term is a ratio against its own budget: the RAM + flash quota for the first, max_ram (folded
// with max_process_mem) for the other two. The swapout term is the one the engine itself regulates,
// and usually the higher of the two RAM terms, though not always — see RedisMemory_GetFlexRatios
// for what each of the underlying INFO fields counts.
typedef struct {
  float total_memory_ratio;
  float ram_ratio;
  float ram_for_swapout_ratio;
} RedisMemoryFlexRatios;

// Read the Flex memory state, in one INFO call. A budget of 0 yields a 0 ratio for the terms that
// divide by it, so an absent bigredis section cannot report pressure.
// GIL must be held before calling this function.
RedisMemoryFlexRatios RedisMemory_GetFlexRatios(RedisModuleCtx *ctx);
