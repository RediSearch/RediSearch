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

// The memory checkpoint of the Rust vector iterators, taken where they hold a whole VecSim reply.
// Returns nonzero when the query should stop (see QueryMemory_ShouldStop), which under memory
// pressure first requests the stop with reason OOM. Reads memory on every call: each call follows
// a VecSim search that costs far more than the read.
int RS_VecSimCheckMemory(QueryRequestTimeout *timeout);

#ifdef __cplusplus
}
#endif
