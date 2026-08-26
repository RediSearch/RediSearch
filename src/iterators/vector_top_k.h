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

// #ifdef __cplusplus
// extern "C" {
// #endif

// Forwards to the test-mockable `vecsimTimeoutCallback`. Called from the Rust
// hybrid adhoc-BF scan so it polls the query deadline like the C path does.
int RS_VecSimCheckTimeout(TimeoutCtx *ctx);


// #ifdef __cplusplus
// }
// #endif