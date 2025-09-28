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
#include "../../util/references.h"
#include "../rmr/rmr.h"

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
    pthread_cond_t mapping_ready_cond;        // Signals when mappings are added

    // Command and context
    MRCommand cmd;                            // The command to execute

    // State management - using atomics for efficiency
    atomic_bool started;                      // Whether the dispatcher has started processing

    size_t numShards;
} HybridDispatcher;

/**
 * Creates a new HybridDispatcher instance
 * @param cmd The MRCommand to execute
 * @return HybridDispatcher pointer
 */
HybridDispatcher *HybridDispatcher_New(const MRCommand *cmd, size_t numShards);

/**
 * Free a HybridDispatcher instance
 * @param dispatcher The dispatcher instance
 */
void HybridDispatcher_Free(void *obj);

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
bool HybridDispatcher_IsStarted(const HybridDispatcher *dispatcher);

/**
 * Takes ownership of mapping arrays (thread-safe)
 * @param dispatcher_ref StrongRef to the dispatcher instance
 * @param isSearch Whether this is for search mappings (true) or vsim mappings (false)
 * @return Array of cursor mappings (caller takes ownership) or NULL if already taken
 */
arrayof(CursorMapping *) HybridDispatcher_TakeMapping(StrongRef dispatcher_ref, bool isSearch);

/**
 * Waits for both search and vsim mappings to be complete (thread-safe)
 * @param dispatcher The dispatcher instance
 */
void HybridDispatcher_WaitForMappingsComplete(HybridDispatcher *dispatcher);


#ifdef __cplusplus
}
#endif
