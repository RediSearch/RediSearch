/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "redis_mem_info.h"

#include <stddef.h>

#include "minmax.h"

#define MIN_NOT_0(a,b) (((a)&&(b))?MIN((a),(b)):MAX((a),(b)))

// Get the used memory ratio from Redis server info.
// Same function as before
// GIL must be held before calling this function
// Returns 0 if maxmemory is 0
float RedisMemory_GetUsedMemoryRatioUnified(RedisModuleCtx *ctx) {

  RedisModuleServerInfoData *info = RedisModule_GetServerInfo(ctx, "memory");

  size_t maxmemory = RedisModule_ServerInfoGetFieldUnsigned(info, "maxmemory", NULL);
  size_t max_process_mem = RedisModule_ServerInfoGetFieldUnsigned(info, "max_process_mem", NULL); // Enterprise limit
  maxmemory = MIN_NOT_0(maxmemory, max_process_mem);

  float used_memory = (float)RedisModule_ServerInfoGetFieldUnsigned(info, "used_memory", NULL);

  RedisModule_FreeServerInfo(ctx, info);
  return maxmemory ? used_memory / (float)maxmemory : 0;
}

// The two RAM fields read below, both from INFO bigredis:
//
//   used_ram              everything the process allocated (zmalloc_used_memory): resident values,
//                         key names and per-key metadata — kept even for flash-resident keys —
//                         client and replication buffers, module memory, and the disk-related overhead
//   used_ram_for_swapout  the same total, with actual memtable usage replaced by the full memtable
//                         budget.
//
// Values swapped out to flash are in neither; they reach used_memory through bsUsedQuota().
RedisMemoryFlexRatios RedisMemory_GetFlexRatios(RedisModuleCtx *ctx) {
  // "mem" returns both the `memory` and `bigredis` sections on Flex in a single call.
  RedisModuleServerInfoData *info = RedisModule_GetServerInfo(ctx, "mem");

  size_t max_process_mem = RedisModule_ServerInfoGetFieldUnsigned(info, "max_process_mem", NULL);

  size_t maxmemory = RedisModule_ServerInfoGetFieldUnsigned(info, "maxmemory", NULL);
  size_t total_limit = MIN_NOT_0(maxmemory, max_process_mem);
  float used_memory = (float)RedisModule_ServerInfoGetFieldUnsigned(info, "used_memory", NULL);

  // max_ram does not fold in max_process_mem, but getMaxRAM() does, so apply it here too for parity
  // with the server's own RAM accounting.
  size_t max_ram = RedisModule_ServerInfoGetFieldUnsigned(info, "max_ram", NULL);
  size_t ram_limit = MIN_NOT_0(max_ram, max_process_mem);

  size_t used_ram = RedisModule_ServerInfoGetFieldUnsigned(info, "used_ram", NULL);
  size_t used_ram_for_swapout =
      RedisModule_ServerInfoGetFieldUnsigned(info, "used_ram_for_swapout", NULL);

  RedisModule_FreeServerInfo(ctx, info);

  return (RedisMemoryFlexRatios){
      .total_memory_ratio = total_limit ? used_memory / (float)total_limit : 0,
      .ram_ratio = ram_limit ? (float)used_ram / (float)ram_limit : 0,
      .ram_for_swapout_ratio = ram_limit ? (float)used_ram_for_swapout / (float)ram_limit : 0,
  };
}
