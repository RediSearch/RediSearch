/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "redis_mem_info.h"
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
