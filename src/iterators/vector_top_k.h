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
int RS_VecSimCheckTimeout(QueryRequestTimeout *timeout);

// The memory checkpoint of the Rust vector iterators: once per VecSim search, after a range
// query returns its reply and before a filtered top-k fetches a further batch. Returns nonzero
// when the query should stop (see QueryMemory_ShouldStop), which under memory pressure first
// requests the stop with reason OOM. Reads memory on every call: there is one call per VecSim
// search, which costs more than the read.
int RS_VecSimCheckMemory(QueryRequestTimeout *timeout);

// VecSim's stop callback (VecSim_SetStopCallbackFunction, which owns the cadence), called with the
// query's QueryRequestTimeout: the memory checkpoint inside a single VecSim search. Same check as
// RS_VecSimCheckMemory, so it also reports any stop already requested for the query. A nonzero
// return ends the search as VecSim_QueryReply_TimedOut; the stop reason tells an OOM stop from a
// timeout. Holds no state, so SVS can call it from several threads.
int RS_VecSimShouldStop(void *timeout);

#ifdef __cplusplus
}
#endif
