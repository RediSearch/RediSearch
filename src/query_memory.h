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

#include "config.h"
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
 * RedisModule_GetRawUsedMemoryRatio; the fake pressure below overrides the
 * last two. Callable on any thread, with or without
 * the GIL, concurrently with other checkpoints of the same request; the ON_OOM
 * policy is the global one, read without synchronization. A NULL
 * `timeout` (no owning request) still reports pressure but records no stop.
 */
bool QueryMemory_ShouldStop(QueryRequestTimeout *timeout, uint32_t *counter);

/**
 * QueryMemory_ShouldStop for a checkpoint that runs once per unit far larger
 * than a row (a shard reply, a build stage): reads memory on every call,
 * whatever the read interval.
 */
bool QueryMemory_ShouldStopNow(QueryRequestTimeout *timeout);

/**
 * The memory checkpoint between the build stages of a query, before it reads
 * its first row, through QueryMemory_ShouldStopNow. Under
 * pressure the OOM stop is requested, and with `oomPolicy` FAIL the query fails
 * before it runs: QUERY_ERROR_CODE_OUT_OF_MEMORY in `err` and REDISMODULE_ERR.
 * Otherwise REDISMODULE_OK; a requested stop then ends the pipeline at its
 * first read, which ApplyOomStop replies as an empty result with the OOM
 * warning. `timeout` must not be NULL.
 */
int QueryMemory_CheckBeforeExecution(QueryRequestTimeout *timeout, RSOomPolicy oomPolicy,
                                     QueryError *err);

/**
 * Debug and test override of QUERY_MEMORY_RATIO_READ_INTERVAL for every
 * checkpoint, so tests can read memory on every call. 0 restores the default.
 */
void QueryMemory_SetRatioReadInterval(uint32_t interval);
uint32_t QueryMemory_GetRatioReadInterval(void);

/*
 * Fake memory pressure, for tests and `_FT.DEBUG MEMORY_PRESSURE`. Process-wide; nothing resets
 * it except QueryMemory_ResetFakePressure. The ON_OOM IGNORE gate still applies.
 */

/**
 * Replaces the memory read of every checkpoint and of the query entry guard
 * (QueryMemoryGuard) with `ratio`, whether or not the server has the memory
 * API. A negative ratio restores the real read.
 */
void QueryMemory_SetForcedRatio(float ratio);
/** Returns the forced ratio, or a negative value when the real read is used. */
float QueryMemory_GetForcedRatio(void);

/**
 * Makes the `n`-th checkpoint call (QueryMemory_ShouldStop or
 * QueryMemory_ShouldStopNow) from now (1-based, counted across all checkpoints
 * and queries, skipping calls that return on an
 * already requested stop) report memory pressure, regardless of the ratio and
 * the read interval. Every other call behaves normally. 0 disarms it. Either
 * way the call count restarts from 0.
 */
void QueryMemory_SetPressureAtCall(uint64_t n);
uint64_t QueryMemory_GetPressureAtCall(void);
/** Checkpoint calls counted since the last QueryMemory_SetPressureAtCall. */
uint64_t QueryMemory_GetCheckpointCalls(void);

/** Clears the forced ratio, the positional pressure and the read-interval override. */
void QueryMemory_ResetFakePressure(void);

#ifdef __cplusplus
}
#endif

#endif  // QUERY_MEMORY_H__
