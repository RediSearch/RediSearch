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

int RS_VecSimShouldStop(void *ctx) {
  QueryRequestLimits *limits = ctx;
  // Compared with the default so the common case skips the indirect call.
  int (*timedOut)(QueryRequestLimits *) = vecsimTimeoutCallback;
  const int stopped = timedOut == VecSim_TimedOut ? VecSim_TimedOut(limits) : timedOut(limits);
  return stopped || QueryMemory_Checkpoint(limits);
}

void RS_VecSimSetTimeoutCallback(int (*callback)(QueryRequestLimits *timeout)) {
  vecsimTimeoutCallback = callback ? callback : VecSim_TimedOut;
}

int RS_VecSimCheckMemory(QueryRequestLimits *timeout) {
  return QueryMemory_CheckpointExact(timeout);
}

int RS_VecSimCheckMemoryEvery(QueryRequestLimits *timeout) {
  return QueryMemory_Checkpoint(timeout);
}
