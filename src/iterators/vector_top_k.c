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

// Global timeout callback for VecSim searches.
// Need the redirection so tests can pass a mock function to test timeout behavior.
int (*vecsimTimeoutCallback)(QueryRequestLimits *timeout) = VecSim_TimedOut;

// Non-inline wrapper called from Rust's VectorScoreSource::adhoc_strategy so the
// test-mockable vecsimTimeoutCallback indirection is honored on the adhoc-BF path.
int RS_VecSimCheckTimeout(QueryRequestLimits *timeout) {
  return vecsimTimeoutCallback(timeout);
}

int RS_VecSimCheckMemory(QueryRequestLimits *timeout) {
  // One call per VecSim search, so it reads every time; and it shares no request counter, so it
  // is safe on any thread.
  return QueryMemory_CheckpointNow(timeout);
}

int RS_VecSimCheckMemoryEvery(QueryRequestLimits *timeout, uint32_t *counter) {
  return QueryMemory_Checkpoint(timeout, counter);
}
