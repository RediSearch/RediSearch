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

// Calls of QueryMemory_Checkpoint per memory read, counted across the whole request.
#define QUERY_MEMORY_RATIO_READ_INTERVAL 256

/**
 * The query path's single "should I stop?" check for memory pressure, and its
 * only reader of process memory.
 *
 * Returns true when the query should stop: a memory stop was already
 * requested, or this call found memory at or above REDIS_MEMORY_OOM_RATIO, in which case it
 * requests a memory stop first. Memory is read when the request's memory.counter reaches
 * QUERY_MEMORY_RATIO_READ_INTERVAL, so calls from all sites of a request add up; the read resets
 * it. The other calls only read the request's flags. `limits` must not be NULL.
 *
 * The memory check is off, and only an already requested memory stop is
 * reported, when ON_OOM is IGNORE, when no memory limit is configured, and when the server lacks
 * RedisModule_GetUsedMemoryRatioEstimate; the fake pressure below overrides
 * the last two. Callable with or without the GIL, from any thread. The request counter is a
 * relaxed atomic, so concurrent calls only shift when memory is read. The ON_OOM policy is the
 * global one, read with a relaxed atomic load because CONFIG SET writes it on the main thread.
 */
static inline bool QueryMemory_Checkpoint(QueryRequestLimits *limits);

/* Process-wide state of the inline fast path of QueryMemory_Checkpoint, written only through the
 * debug and test hooks below. */
extern RS_Atomic(uint32_t) QueryMemory_ratioReadInterval;
extern RS_Atomic(uint32_t) QueryMemory_pressureArmed;

/** The full checkpoint, used while a fake pressure is armed. Same semantics as the fast path. */
bool QueryMemory_CheckpointSlow(QueryRequestLimits *limits);

/** The memory read of a due checkpoint, or of a forced pressure: resets the call counter first. */
bool QueryMemory_ReadAndReset(QueryRequestLimits *limits, bool forcedPressure);

/**
 * Counts one checkpoint call in the request's counter and returns whether it reached the read
 * interval. Shared by the fast path and QueryMemory_CheckpointSlow.
 */
static inline bool QueryMemory_CountCall(QueryRequestLimits *limits) {
  // Load and store, not fetch_add: a lost increment only delays a read, and this avoids an
  // atomic RMW on the hot path.
  const uint32_t calls = RS_AtomicUintLoadRelaxed(&limits->memory.counter) + 1;
  RS_AtomicUintStoreRelaxed(&limits->memory.counter, calls);
  return calls >= RS_AtomicUintLoadRelaxed(&QueryMemory_ratioReadInterval);
}

static inline bool QueryMemory_Checkpoint(QueryRequestLimits *limits) {
  RS_ASSERT(limits);
  if (QueryRequestLimits_IsMemoryStopped(limits)) {
    return true;
  }
  if (__builtin_expect(RS_AtomicUintLoadRelaxed(&QueryMemory_pressureArmed) != 0, 0)) {
    return QueryMemory_CheckpointSlow(limits);
  }
  if (__builtin_expect(!QueryMemory_CountCall(limits), 1)) {
    return false;
  }
  return QueryMemory_ReadAndReset(limits, false);
}

/** QueryMemory_Checkpoint as a linked symbol, for callers that cannot inline it (Rust). */
bool QueryMemory_CheckpointExtern(QueryRequestLimits *limits);

/**
 * The non-amortized checkpoint: reads memory on every call and touches no
 * counter, like QueryRequestLimits_IsTimedOutExact. Reports an already
 * requested memory stop, and requests one on pressure. For sites that are rare
 * or expensive enough to make every call worth a read.
 */
bool QueryMemory_CheckpointExact(QueryRequestLimits *limits);

/**
 * The memory checkpoint after a query is built, before it reads
 * its first row. Unless a stop is already requested, reads memory on this call,
 * whatever the read interval. Under
 * pressure the OOM stop is requested, and with `oomPolicy` FAIL the query fails
 * before it runs: QUERY_ERROR_CODE_OUT_OF_MEMORY in `err` and REDISMODULE_ERR.
 * Otherwise REDISMODULE_OK; the caller then ends the pipeline's row source
 * on a requested stop (RP_EndRowSourceOnStop) once it is built, so the
 * pipeline ends at its first read: an FT.SEARCH or FT.AGGREGATE then replies
 * an empty result with the OOM warning (ApplyOomStop), an FT.HYBRID the rows
 * from any subquery not stopped at build, with it. `limits` must not be NULL.
 */
int QueryMemory_CheckBeforeExecution(QueryRequestLimits *limits, RSOomPolicy oomPolicy,
                                     QueryError *err);

/**
 * Debug and test override of QUERY_MEMORY_RATIO_READ_INTERVAL for every
 * checkpoint, so tests can read memory on every call. `interval` must be at least 1;
 * QueryMemory_ResetFakePressure restores the default.
 */
void QueryMemoryDebug_SetRatioReadInterval(uint32_t interval);
uint32_t QueryMemory_GetRatioReadInterval(void);

/*
 * Fake memory pressure, for tests and `_FT.DEBUG MEMORY_PRESSURE`. Process-wide; nothing resets
 * it except QueryMemory_ResetFakePressure. The ON_OOM IGNORE gate still applies.
 */

/**
 * Replaces the memory read of every checkpoint and of the query entry guard
 * (QueryMemoryGuard) with `ratio`, whether or not the server has the memory
 * API. Callers pass a non-negative ratio; QueryMemory_ResetFakePressure restores the real read.
 */
void QueryMemory_SetForcedRatio(float ratio);
/** Returns the forced ratio, or a negative value when the real read is used. */
float QueryMemory_GetForcedRatio(void);

/**
 * Makes the `n`-th QueryMemory_Checkpoint call from now (1-based, counted
 * across all checkpoints and queries, skipping calls that return on an
 * already requested memory stop)
 * report memory pressure, regardless of the ratio and the read interval.
 * Every other call behaves normally. `n` must be at least 1; the call count
 * restarts from 0. QueryMemory_ResetFakePressure disarms it.
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
