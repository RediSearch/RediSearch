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

// The memory checkpoint of the Rust vector iterators: once per VecSim search, after a range
// query returns its reply and before a filtered top-k fetches a further batch. Returns nonzero
// when the query should stop (see QueryMemory_CheckpointNow), which under memory pressure first
// requests the memory stop. Reads memory on every call: there is one call per VecSim
// search, which costs more than the read.
int RS_VecSimCheckMemory(QueryRequestLimits *timeout);

// The memory checkpoint of a per-document loop (the adhoc-BF scan), where a read per call would
// cost more than the work between calls. Like RS_VecSimCheckMemory, but reads memory only every
// QUERY_MEMORY_RATIO_READ_INTERVAL-th call counted by `counter`; every call still observes an
// already requested stop. `counter` is owned by the caller's scan, zero-initialized.
int RS_VecSimCheckMemoryEvery(QueryRequestLimits *timeout, uint32_t *counter);

#ifdef __cplusplus
}
#endif
