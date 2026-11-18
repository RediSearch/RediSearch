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
 * The window is driven by the ASM state machine (asm_state_machine.h), which opens it
 * only when search runs on disk.
 *
 * Every function, and every read of the counters below, must run on the main thread or
 * under the GIL: the stage events, keyspace notifications and the DocIdMeta unlink all
 * do, so the state is not atomic. A reader off the GIL would need atomics.
 */
typedef enum {
  ASM_IMPORT_SLOT_NONE = 0,
  // The RAM overlay is restoring the slot's keys.
  ASM_IMPORT_SLOT_OVERLAY,
  // The overlay ended, the rebuild of the slot has not finished. Nothing should write the slot:
  // clients are redirected, and Flex holds back the source's later writes (the live tail) while
  // search holds the import gate, which it releases only after closing the window. Without the
  // gate, the tail writes here. The keys the rebuild has not reached still carry the source's
  // docIds, so a write or an unlink of one would replace or delete whichever unrelated local
  // document has that docId.
  //
  // Unlinks stay absorbed: at worst one document of a key already rebuilt is left behind.
  // Notifications are processed, since an ignored one would leave a rebuilt key stale. Both break
  // Flex's guarantee, so both are reported (ASM_ImportWindow_AbsorbsUnlink,
  // ASM_ImportWindow_ReportRebuildWrite).
  ASM_IMPORT_SLOT_AWAITING_REBUILD,
} ASMImportSlotPhase;

// Slot counts, exposed only for the inline fast paths below.
// Slots in ASM_IMPORT_SLOT_OVERLAY, whose keyspace notifications are ignored.
extern uint32_t ASM_ImportWindow_overlayWindowSlots_g;
// Slots in any phase but ASM_IMPORT_SLOT_NONE, whose unlinks are absorbed.
extern uint32_t ASM_ImportWindow_fullASMWindowSlots_g;

// Every slot of `slots` enters ASM_IMPORT_SLOT_OVERLAY.
void ASM_ImportWindow_OnOverlayStart(const RedisModuleSlotRangeArray *slots);

// The slots of `slots` in ASM_IMPORT_SLOT_OVERLAY move to ASM_IMPORT_SLOT_AWAITING_REBUILD.
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
  return ASM_ImportWindow_overlayWindowSlots_g != 0 && ASM_ImportWindow_KeyInOverlay(key);
}

// Whether any slot is in the window.
static inline bool ASM_ImportWindow_IsOpen(void) {
  return ASM_ImportWindow_fullASMWindowSlots_g != 0;
}

/*
 * Whether the DocIdMeta unlink of `key` must only invalidate its entries, without
 * deleting documents. `key` may be NULL, which is never absorbed.
 *
 * An absorbed unlink in ASM_IMPORT_SLOT_AWAITING_REBUILD is unexpected, so it is counted
 * (ASM_ImportWindow_UnexpectedUnlinks), logged and asserted in debug builds.
 */
bool ASM_ImportWindow_AbsorbsUnlink(const RedisModuleString *key);

// Unlinks absorbed in ASM_IMPORT_SLOT_AWAITING_REBUILD since the last ASM_ImportWindow_Reset.
size_t ASM_ImportWindow_UnexpectedUnlinks(void);

// Counts, logs and asserts in debug builds a write notification for `key` if its slot is in
// ASM_IMPORT_SLOT_AWAITING_REBUILD. `key` may be NULL.
void ASM_ImportWindow_ReportRebuildWrite(const RedisModuleString *key);

// Reports a write notification that the window lets through but no writer should cause.
static inline void ASM_ImportWindow_CheckWrite(const RedisModuleString *key) {
  // The full window is larger than the overlay window only while some slot is rebuilt.
  if (ASM_ImportWindow_fullASMWindowSlots_g != ASM_ImportWindow_overlayWindowSlots_g) {
    ASM_ImportWindow_ReportRebuildWrite(key);
  }
}

// Writes reported by ASM_ImportWindow_ReportRebuildWrite since the last ASM_ImportWindow_Reset.
size_t ASM_ImportWindow_UnexpectedWrites(void);

#ifdef __cplusplus
}
#endif
