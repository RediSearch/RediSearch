/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef HYBRID_CONFIG_SNAPSHOT_H
#define HYBRID_CONFIG_SNAPSHOT_H

#include "config.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Thread-safe snapshot of RSGlobalConfig values needed for hybrid command parsing.
 *
 * This struct captures configuration values from RSGlobalConfig on the main thread
 * before dispatching to a worker thread, ensuring consistent config access without
 * race conditions.
 */
typedef struct HybridConfigSnapshot {
    // Full RequestConfig (covers queryTimeoutMS, dialectVersion, oomPolicy, etc.)
    RequestConfig requestConfig;

    // Individual fields from RSGlobalConfig
    size_t maxSearchResults;
    long long cursorMaxIdle;
} HybridConfigSnapshot;

/**
 * Create a new HybridConfigSnapshot by capturing current RSGlobalConfig values.
 * This should be called on the main thread before dispatching to a worker.
 *
 * @return Newly allocated snapshot. Caller is responsible for freeing with HybridConfigSnapshot_Free.
 */
HybridConfigSnapshot *HybridConfigSnapshot_Create();

/**
 * Free a HybridConfigSnapshot.
 * Safe to call with NULL pointer.
 *
 * @param snapshot The snapshot to free (can be NULL)
 */
void HybridConfigSnapshot_Free(void *snapshot);

#ifdef __cplusplus
}
#endif

#endif // HYBRID_CONFIG_SNAPSHOT_H

