/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redismodule.h"
#ifndef INFO_COMMAND_H
#define INFO_COMMAND_H
#define CLOCKS_PER_MILLISEC (CLOCKS_PER_SEC / 1000)

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TotalSpecsInfo {
    size_t total_mem;       // Total memory used by the indexes
    size_t min_mem;         // Memory used by the smallest (local) index
    size_t max_mem;         // Memory used by the largest (local) index
    size_t indexing_time;   // Time spent on indexing
    InfoGCStats gc_stats;   // Garbage collection statistics
} TotalSpecsInfo;

int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
#ifdef __cplusplus
}
#endif
#endif