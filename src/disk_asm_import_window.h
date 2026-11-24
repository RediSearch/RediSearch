/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Slots of an SST (physical) slot-migration import whose keys search must neither
 * index nor de-index on its own.
 *
 * The keys of an SST import arrive with the DocIdMeta of the source: their docIds
 * name the source's documents, and here they may name unrelated local ones. The RAM
 * overlay then replaces the stale SST copy of every key the source changed since its
 * last flush, and search rebuilds the imported slots in a single scan after it. For a
 * key in the window:
 *
 * - keyspace notifications are ignored: the rebuild is the only indexer of the
 *   imported keys and sees their final values;
 * - the DocIdMeta unlink callback deletes no document by docId.
 *
 * The window opens when the overlay starts and must stay open until the rebuild has
 * indexed the slots, since the keys keep the source's docIds until then. It closes
 * right before search releases the import gate, so the source's later writes that the
 * gate held back are indexed normally, and whenever the import ends.
 *
 * The window is driven by the ASM state machine (asm_state_machine.h), which opens it
 * only when search runs on disk.
 *
 * Every function, and every read of DiskASM_ImportWindow_numSlots_g, must run on the main
 * thread or under the GIL: the stage events, keyspace notifications and the DocIdMeta
 * unlink all do, so the state is not atomic. A reader off the GIL would need atomics.
 */

// Number of slots in the window, exposed only for the inline fast path below.
extern uint32_t DiskASM_ImportWindow_numSlots_g;

// Every slot of `slots` enters the window.
void DiskASM_ImportWindow_Open(const RedisModuleSlotRangeArray *slots);

// Every slot of `slots` leaves the window.
void DiskASM_ImportWindow_Close(const RedisModuleSlotRangeArray *slots);

// Every slot leaves the window.
void DiskASM_ImportWindow_Reset(void);

// Whether `slot` is in the window.
bool DiskASM_IsSlotBeingImported(unsigned int slot);

// The out-of-line part of DiskASM_IsKeyBeingImported.
bool DiskASM_IsKeySlotBeingImported(const RedisModuleString *key);

// Whether `key` is in the window: its notifications are ignored and its unlink keeps its
// documents. `key` may be NULL, which is never in the window.
static inline bool DiskASM_IsKeyBeingImported(const RedisModuleString *key) {
  return DiskASM_ImportWindow_numSlots_g != 0 && key && DiskASM_IsKeySlotBeingImported(key);
}

// The out-of-line part of DiskASM_IsAnySlotBeingImported.
bool DiskASM_IsAnyRangeSlotBeingImported(const RedisModuleSlotRangeArray *slots);

// Whether any slot of `slots` is in the window. The window is empty on a node that imports
// nothing, such as a source trimming the slots it migrated away, so that check is inline.
static inline bool DiskASM_IsAnySlotBeingImported(const RedisModuleSlotRangeArray *slots) {
  return DiskASM_ImportWindow_numSlots_g != 0 && slots &&
         DiskASM_IsAnyRangeSlotBeingImported(slots);
}

#ifdef __cplusplus
}
#endif
