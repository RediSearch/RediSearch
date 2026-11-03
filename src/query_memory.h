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

// Calls of QueryMemory_Checkpoint per memory read, counted across the whole request.
#define QUERY_MEMORY_RATIO_READ_INTERVAL 256

/**
 * The query path's single "should I stop?" check for memory pressure, and its
 * only reader of process memory.
 *
 * Returns true when the query should stop: a memory stop was already
 * requested, the blocked-client timeout fired (QueryRequestLimits_IsBlockedClientTimedOut), or
 * this call found memory at or above REDIS_MEMORY_OOM_RATIO, in which case it requests
 * a memory stop first. Memory is read when either the caller's
 * `counter` or the request's shared memory.counter reaches
 * QUERY_MEMORY_RATIO_READ_INTERVAL, so calls from all sites of a request add
 * up and a site called fewer times than the interval still contributes to a
 * read. A read resets both counters. Priming `counter` to the interval minus
 * one reads on the next call. The other calls only read the request's flags.
 * Each site owns its counter, zero-initialized, and must not share it between
 * threads. With a NULL `limits` only `counter` counts.
 *
 * The memory check is off, and only the memory stop and the timeout flag are
 * reported, when ON_OOM is IGNORE, when no memory limit is configured, and when the server lacks
 * RedisModule_GetUsedMemoryRatioEstimate. Callable with or without the GIL, but
 * calls for one request must be serialized like QueryRequestLimits_IsTimedOut,
 * since they share memory.counter; use QueryMemory_CheckpointNow where sites
 * may run concurrently. The ON_OOM policy is the global one, read without
 * synchronization. A NULL `limits` (no owning request) still reports pressure
 * but records no memory stop.
 */
bool QueryMemory_Checkpoint(QueryRequestLimits *limits, uint32_t *counter);

/**
 * Like QueryMemory_Checkpoint, but reads memory on every call and touches no
 * counter, so it is safe on several threads at once for one request. Reports
 * an already requested memory stop, and requests one on pressure. For sites
 * that are rare or expensive enough to make every call worth a read.
 */
bool QueryMemory_CheckpointNow(QueryRequestLimits *limits);

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
