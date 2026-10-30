/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef QUERY_MEMORY_H__
#define QUERY_MEMORY_H__

#include <stdbool.h>
#include <stdint.h>

#include "query_request.h"

#ifdef __cplusplus
extern "C" {
#endif

// Calls of QueryMemory_ShouldStop per memory read at one checkpoint.
#define QUERY_MEMORY_RATIO_READ_INTERVAL 256

/**
 * The query path's single "should I stop?" check for memory pressure, and its
 * only reader of process memory.
 *
 * Returns true when the query should stop: a stop was already requested, the
 * blocked-client timeout fired (see QueryRequest_ShouldStop), or this call
 * found memory at or above REDIS_MEMORY_OOM_RATIO, in which case it requests
 * a stop with reason OOM first. Memory is read on every
 * QUERY_MEMORY_RATIO_READ_INTERVAL-th call counted by `counter`, the first
 * time on that call, not the first; the other calls only read the request's
 * flags. Each checkpoint site owns its counter, zero-initialized, and must not
 * share it between threads.
 *
 * The memory check is off, and only the stop flags are reported, when ON_OOM
 * is IGNORE, when no memory limit is configured, and when the server lacks
 * RedisModule_GetRawUsedMemoryRatio. Callable on any thread, with or without
 * the GIL, concurrently with other checkpoints of the same request; the ON_OOM
 * policy is the global one, read without synchronization. A NULL
 * `timeout` (no owning request) still reports pressure but records no stop.
 */
bool QueryMemory_ShouldStop(QueryRequestTimeout *timeout, uint32_t *counter);

/**
 * Debug and test override of QUERY_MEMORY_RATIO_READ_INTERVAL for every
 * checkpoint, so tests can read memory on every call. 0 restores the default.
 */
void QueryMemory_SetRatioReadInterval(uint32_t interval);
uint32_t QueryMemory_GetRatioReadInterval(void);

#ifdef __cplusplus
}
#endif

#endif  // QUERY_MEMORY_H__
