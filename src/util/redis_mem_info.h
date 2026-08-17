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

// The two memory-pressure ratios available on Flex (BigRedis), used by the async background
// scan. Both are expressed against their own budget, so 1.0 means "at budget" for either — but
// they are NOT interchangeable, and no single threshold is meaningful for both: `total` is free-
// running while `ram` is actively regulated (see the fields). Callers must therefore test each
// against its own bound rather than collapsing them with max().
//
// `total` alone would not be enough: on Flex it can stay low while RAM — the real bottleneck for
// indexing — is exhausted, which is exactly what `ram` catches.
typedef struct {
  // used_memory / min_not_0(maxmemory, max_process_mem) — the RAM + flash quota. Scarcity:
  // nothing drives this upward on its own, and reaching 1.0 means the quota really is spent.
  float total;
  // used_ram_for_swapout / min_not_0(max_ram, max_process_mem) — RAM only. This one is a
  // controlled variable: the swapout engine evicts values down to the budget and no further, so
  // once the value cache is warm 1.0 is its designed operating point rather than a warning, and
  // it ripples around that setpoint. Only a departure above the setpoint that persists means the
  // engine has run out of evictable values and RAM demand is genuinely unmet.
  float ram;
} RedisMemoryFlexRatios;

// Read both Flex ratios, in one INFO call. A budget of 0 yields a 0 ratio for that term, so an
// absent bigredis section cannot report pressure.
// GIL must be held before calling this function.
RedisMemoryFlexRatios RedisMemory_GetFlexRatios(RedisModuleCtx *ctx);
