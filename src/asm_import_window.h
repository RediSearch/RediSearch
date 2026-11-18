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
 * Slots of an SST (physical) slot-migration import whose keys search must not
 * index or de-index on its own.
 *
 * The keys of an SST import arrive with the DocIdMeta of the source: their
 * docIds name the source's documents, and here they may name unrelated local
 * ones. The RAM overlay then replaces the stale SST copy of every key the source
 * changed since its last flush. Search rebuilds the imported slots in a single
 * scan after the overlay, so in these slots:
 *
 * - keyspace notifications are ignored while the overlay runs: the scan is the
 *   only indexer of the imported keys and sees their final values;
 * - the DocIdMeta unlink callback only invalidates the key's entries and never
 *   deletes a document by docId, from the start of the overlay until the
 *   rebuild is done.
 *
 * Every function runs on the main thread.
 */
typedef enum {
  ASM_IMPORT_SLOT_NONE = 0,
  // The RAM overlay is restoring the slot's keys.
  ASM_IMPORT_SLOT_OVERLAY,
  // The overlay ended, the rebuild of the slot has not finished. Flex deletes no key in the slot
  // meanwhile, so an unlink here is unexpected.
  ASM_IMPORT_SLOT_REBUILD,
} ASMImportSlotPhase;

// Slot counts, exposed only for the inline fast paths below.
extern uint32_t ASM_ImportWindow_overlaySlots_g;  // in ASM_IMPORT_SLOT_OVERLAY
extern uint32_t ASM_ImportWindow_windowSlots_g;   // in any phase but ASM_IMPORT_SLOT_NONE

// Every slot of `slots` enters ASM_IMPORT_SLOT_OVERLAY.
void ASM_ImportWindow_OnOverlayStart(const RedisModuleSlotRangeArray *slots);

// The slots of `slots` in ASM_IMPORT_SLOT_OVERLAY move to ASM_IMPORT_SLOT_REBUILD.
void ASM_ImportWindow_OnOverlayEnd(const RedisModuleSlotRangeArray *slots);

// Every slot of `slots` leaves the window. Called once the rebuild indexed the imported keys,
// before the source's later writes are applied, and when the import attempt ends.
void ASM_ImportWindow_Close(const RedisModuleSlotRangeArray *slots);

// Every slot leaves the window and the counters are zeroed.
void ASM_ImportWindow_Reset(void);

ASMImportSlotPhase ASM_ImportWindow_SlotPhase(unsigned int slot);

bool ASM_ImportWindow_KeyInOverlay(const RedisModuleString *key);

// Whether keyspace notifications for `key` must be ignored.
static inline bool ASM_ImportWindow_IgnoresNotification(const RedisModuleString *key) {
  return ASM_ImportWindow_overlaySlots_g != 0 && ASM_ImportWindow_KeyInOverlay(key);
}

// Whether any slot is in the window.
static inline bool ASM_ImportWindow_IsOpen(void) {
  return ASM_ImportWindow_windowSlots_g != 0;
}

/*
 * Whether the DocIdMeta unlink of `key` must only invalidate its entries, without
 * deleting documents. `key` may be NULL, which is never absorbed.
 *
 * An absorbed unlink in ASM_IMPORT_SLOT_REBUILD is counted
 * (ASM_ImportWindow_UnexpectedUnlinks), logged and asserted in debug builds: if the
 * rebuild had already indexed the key, its document is left behind.
 */
bool ASM_ImportWindow_AbsorbsUnlink(const RedisModuleString *key);

// Unlinks absorbed in ASM_IMPORT_SLOT_REBUILD since the last ASM_ImportWindow_Reset.
size_t ASM_ImportWindow_UnexpectedUnlinks(void);

#ifdef __cplusplus
}
#endif
