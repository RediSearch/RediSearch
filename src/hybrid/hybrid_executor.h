/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*HybridTicket)(void *ticket);
typedef int (*HybridSubmitTicket)(void *pool, HybridTicket run, void *ticket);

// The caller is one of the runners. submit takes ownership of a ticket on success (zero).
// finish runs exactly once after all runChild calls return and can free ctx. Queued tickets
// may run later. No thread, including the caller, waits for a ticket to get a pool worker.
void HybridExecutor_Run(size_t children, size_t runners, HybridSubmitTicket submit, void *pool,
                        void (*runChild)(void *ctx, size_t child), void (*finish)(void *ctx),
                        void *ctx);

#ifdef __cplusplus
}
#endif
