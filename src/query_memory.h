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

// Calls of QueryMemory_ShouldStop per memory read, counted across the whole request.
#define QUERY_MEMORY_RATIO_READ_INTERVAL 256

/**
 * The query path's single "should I stop?" check for memory pressure, and its
 * only reader of process memory.
 *
 * Returns true when the query should stop: a memory stop was already
 * requested, the blocked-client timeout fired (see QueryRequest_ShouldStop), or this call
 * found memory at or above REDIS_MEMORY_OOM_RATIO, in which case it requests
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
 * RedisModule_GetUsedMemoryRatioEstimate; the fake pressure below overrides
 * the last two. Callable with or without the GIL, but calls for one request
 * must be serialized like QueryRequestLimits_IsTimedOut, since they share
 * memory.counter; use QueryMemory_ShouldStopNow where sites may run
 * concurrently. The ON_OOM policy is the global one, read without
 * synchronization. A NULL `limits` (no owning request) still reports pressure
 * but records no memory stop.
 */
bool QueryMemory_ShouldStop(QueryRequestLimits *limits, uint32_t *counter);

/**
 * Like QueryMemory_ShouldStop, but reads memory on every call and touches no
 * counter, so it is safe on several threads at once for one request. Reports
 * an already requested memory stop, and requests one on pressure. For sites
 * that are rare or expensive enough to make every call worth a read.
 */
bool QueryMemory_ShouldStopNow(QueryRequestLimits *limits);

/**
 * The memory checkpoint between the build stages of a query, before it reads
 * its first row. Unless a stop is already requested, reads memory on this call,
 * whatever the read interval. Under
 * pressure the OOM stop is requested, and with `oomPolicy` FAIL the query fails
 * before it runs: QUERY_ERROR_CODE_OUT_OF_MEMORY in `err` and REDISMODULE_ERR.
 * Otherwise REDISMODULE_OK; the caller then ends the pipeline's row source
 * on a requested stop (RP_EndRowSourceOnStop) once it is built, so the
 * pipeline ends at its first read: an FT.SEARCH or FT.AGGREGATE then replies
 * an empty result with the OOM warning (ApplyOomStop). FT.HYBRID calls it under
 * ON_OOM FAIL only: its reply does not map an OOM stop under RETURN yet, so
 * RETURN is excluded there. `timeout` must not be NULL.
 */
int QueryMemory_CheckBeforeExecution(QueryRequestLimits *timeout, RSOomPolicy oomPolicy,
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
 * Makes the `n`-th QueryMemory_ShouldStop call from now (1-based, counted
 * across all checkpoints and queries, skipping calls that return on an
 * already requested stop, a memory stop or a fired blocked-client timeout)
 * report memory pressure, regardless of the ratio and the read interval.
 * Every other call behaves normally. 0 disarms it. Either way the call count
 * restarts from 0.
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
