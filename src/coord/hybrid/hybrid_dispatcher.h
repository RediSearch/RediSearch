/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdatomic.h>
#include <pthread.h>
#include "references.h"
#include "rmr/rmr.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations
struct MRCommand;
struct MRIterator;

/**
 * HybridDispatcher - Coordinates cursor mapping operations between search and vector similarity
 * Uses StrongRef pattern for safe sharing between multiple RPNet instances
 */
typedef struct HybridDispatcher {
    // Core data - direct management of cursor mappings
    arrayof(CursorMapping *) searchMappings;  // Search cursor mappings
    arrayof(CursorMapping *) vsimMappings;    // Vector similarity cursor mappings
    pthread_mutex_t data_mutex;               // Thread safety for data access

    // Command and context
    MRCommand cmd;                            // The command to execute

    // State management - using atomics for efficiency
    atomic_bool started;                      // Whether the dispatcher has started processing
    atomic_bool done;                         // Whether processing is complete

    // Reference counting (via StrongRef)
    StrongRef self_ref;                       // Self-reference for sharing
} HybridDispatcher;

/**
 * Creates a new HybridDispatcher instance
 * @param cmd The MRCommand to execute
 * @return StrongRef to the new HybridDispatcher instance
 */
StrongRef HybridDispatcher_New(const MRCommand *cmd);

/**
 * Complete dispatch workflow: start processing, wait for completion, and finish
 * @param dispatcher The dispatcher instance
 * @return RS_RESULT_OK on success, error code otherwise
 */
int HybridDispatcher_Dispatch(HybridDispatcher *dispatcher);

/**
 * Checks if the dispatcher has started (thread-safe)
 * @param dispatcher The dispatcher instance
 * @return true if started, false otherwise
 */
bool HybridDispatcher_IsStarted(HybridDispatcher *dispatcher);

/**
 * Sets the mapping array for search or vector similarity (thread-safe)
 * @param dispatcher The dispatcher instance
 * @param mappings Array of cursor mappings (not OWNED)
 * @param isSearch Whether this is for search mappings (true) or vsim mappings (false)
 */
void HybridDispatcher_SetMappingArray(HybridDispatcher *dispatcher, arrayof(CursorMapping *) mappings, bool isSearch);

/**
 * Checks if the dispatcher is done (thread-safe)
 * @param dispatcher The dispatcher instance
 * @return true if done, false otherwise
 */
bool HybridDispatcher_IsDone(HybridDispatcher *dispatcher);

#ifdef __cplusplus
}
#endif
