/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "util/timeout.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forwards to the test-mockable `vecsimTimeoutCallback`. Called from the Rust
// vector top-k adhoc-BF scan so it polls the query deadline like the C path does.
int RS_VecSimCheckTimeout(QueryRequestLimits *timeout);

// Replaces the timeout check behind RS_VecSimCheckTimeout and RS_VecSimShouldStop, for the
// FT.DEBUG timeout mock; NULL restores the default.
void RS_VecSimSetTimeoutCallback(int (*callback)(QueryRequestLimits *timeout));

// The memory checkpoint of the Rust vector iterators. Returns nonzero when the query should stop
// (see QueryMemory_CheckpointExact), which under memory pressure first requests the memory stop.
// Reads memory on every call; callers own the cadence.
int RS_VecSimCheckMemory(QueryRequestLimits *timeout);

// VecSim's stop callback (VecSim_SetStopCallbackFunction), called with the query's
// QueryRequestLimits at every search step. Returns nonzero when the query timed out or, per the
// amortized QueryMemory_Checkpoint, memory is under pressure; either ends the search as
// VecSim_QueryReply_Stopped, and the request's memory stop tells an OOM stop from a timeout.
// Callable from any thread: queries on different workers share it, and SVS searches in parallel.
int RS_VecSimShouldStop(void *ctx);

// The memory checkpoint of a per-document loop (the adhoc-BF scan), where a read per call would
// cost more than the work between calls. Like RS_VecSimCheckMemory, but reads memory only every
// QUERY_MEMORY_RATIO_READ_INTERVAL-th call counted by `counter`; every call still observes an
// already requested stop. `counter` is owned by the caller's scan, zero-initialized.
int RS_VecSimCheckMemoryEvery(QueryRequestLimits *timeout, uint32_t *counter);

#ifdef __cplusplus
}
#endif
