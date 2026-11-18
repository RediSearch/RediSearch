/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "vector_top_k.h"
#include "query_memory.h"
#include "util/timeout.h"

// Timeout check of VecSim's stop callback (RS_VecSimShouldStop) and of the Rust adhoc scan.
// Need the redirection so tests can pass a mock function to test timeout behavior.
int (*vecsimTimeoutCallback)(QueryRequestLimits *timeout) = VecSim_TimedOut;

// Non-inline wrapper called from Rust's VectorScoreSource::adhoc_strategy so the
// test-mockable vecsimTimeoutCallback indirection is honored on the adhoc-BF path.
int RS_VecSimCheckTimeout(QueryRequestLimits *timeout) {
  return vecsimTimeoutCallback(timeout);
}

// VecSim calls this at every search step, from any worker thread and from SVS search threads, so
// it holds no state of its own: the request-wide counter of the checkpoint sets the read cadence,
// and the call-local counter never reaches the interval.
int RS_VecSimShouldStop(void *ctx) {
  QueryRequestLimits *limits = ctx;
  uint32_t unused = 0;
  return vecsimTimeoutCallback(limits) || QueryMemory_Checkpoint(limits, &unused);
}

int RS_VecSimCheckMemory(QueryRequestLimits *timeout) {
  // Reads memory on every call and shares no request counter, so it is safe on any thread. Callers
  // own the cadence.
  return QueryMemory_CheckpointExact(timeout);
}

int RS_VecSimCheckMemoryEvery(QueryRequestLimits *timeout, uint32_t *counter) {
  return QueryMemory_Checkpoint(timeout, counter);
}
