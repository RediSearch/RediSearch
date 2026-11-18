/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "asm_import_window.h"

#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#define ASM_IMPORT_WINDOW_NUM_SLOTS 16384

extern RedisModuleCtx *RSDummyContext;

// Allocated while any slot is in the window, so a shard that never imports pays nothing.
static uint8_t *slotPhase = NULL;
uint32_t ASM_ImportWindow_overlayWindowSlots_g = 0;
uint32_t ASM_ImportWindow_fullASMWindowSlots_g = 0;
static size_t unexpectedUnlinks = 0;

static void setPhase(unsigned int slot, ASMImportSlotPhase phase) {
  ASMImportSlotPhase old = (ASMImportSlotPhase)slotPhase[slot];
  if (old == phase) return;
  ASM_ImportWindow_overlayWindowSlots_g -= (old == ASM_IMPORT_SLOT_OVERLAY);
  ASM_ImportWindow_overlayWindowSlots_g += (phase == ASM_IMPORT_SLOT_OVERLAY);
  ASM_ImportWindow_fullASMWindowSlots_g -= (old != ASM_IMPORT_SLOT_NONE);
  ASM_ImportWindow_fullASMWindowSlots_g += (phase != ASM_IMPORT_SLOT_NONE);
  slotPhase[slot] = (uint8_t)phase;
}

// Moves every slot of `slots` currently in `from` (any phase when `anyFrom`) to `to`.
static void transition(const RedisModuleSlotRangeArray *slots, bool anyFrom,
                       ASMImportSlotPhase from, ASMImportSlotPhase to) {
  if (!slots) return;
  if (!slotPhase) {
    if (to == ASM_IMPORT_SLOT_NONE || !anyFrom) return;  // nothing to leave or move
    slotPhase = rm_calloc(ASM_IMPORT_WINDOW_NUM_SLOTS, sizeof(*slotPhase));
  }
  for (int32_t r = 0; r < slots->num_ranges; r++) {
    const RedisModuleSlotRange *range = &slots->ranges[r];
    RS_LOG_ASSERT(range->start <= range->end && range->end < ASM_IMPORT_WINDOW_NUM_SLOTS,
                  "invalid slot range");
    for (unsigned int slot = range->start; slot <= range->end; slot++) {
      if (anyFrom || slotPhase[slot] == from) {
        setPhase(slot, to);
      }
    }
  }
  if (ASM_ImportWindow_fullASMWindowSlots_g == 0) {
    rm_free(slotPhase);
    slotPhase = NULL;
  }
}

void ASM_ImportWindow_OnOverlayStart(const RedisModuleSlotRangeArray *slots) {
  transition(slots, true, ASM_IMPORT_SLOT_NONE, ASM_IMPORT_SLOT_OVERLAY);
}

void ASM_ImportWindow_OnOverlayEnd(const RedisModuleSlotRangeArray *slots) {
  transition(slots, false, ASM_IMPORT_SLOT_OVERLAY, ASM_IMPORT_SLOT_REBUILD);
}

void ASM_ImportWindow_Close(const RedisModuleSlotRangeArray *slots) {
  transition(slots, true, ASM_IMPORT_SLOT_NONE, ASM_IMPORT_SLOT_NONE);
}

void ASM_ImportWindow_Reset(void) {
  rm_free(slotPhase);
  slotPhase = NULL;
  ASM_ImportWindow_overlayWindowSlots_g = 0;
  ASM_ImportWindow_fullASMWindowSlots_g = 0;
  unexpectedUnlinks = 0;
}

ASMImportSlotPhase ASM_ImportWindow_SlotPhase(unsigned int slot) {
  RS_LOG_ASSERT(slot < ASM_IMPORT_WINDOW_NUM_SLOTS, "invalid slot");
  return slotPhase ? (ASMImportSlotPhase)slotPhase[slot] : ASM_IMPORT_SLOT_NONE;
}

static unsigned int keySlot(const RedisModuleString *key) {
  size_t len;
  const char *name = RedisModule_StringPtrLen(key, &len);
  return RedisModule_ClusterKeySlotC(name, len);
}

bool ASM_ImportWindow_KeyInOverlay(const RedisModuleString *key) {
  return slotPhase && slotPhase[keySlot(key)] == ASM_IMPORT_SLOT_OVERLAY;
}

bool ASM_ImportWindow_AbsorbsUnlink(const RedisModuleString *key) {
  if (!slotPhase || !key) return false;
  unsigned int slot = keySlot(key);
  switch ((ASMImportSlotPhase)slotPhase[slot]) {
    case ASM_IMPORT_SLOT_NONE:
      return false;
    case ASM_IMPORT_SLOT_OVERLAY:
      return true;
    case ASM_IMPORT_SLOT_REBUILD:
      unexpectedUnlinks++;
      RedisModule_Log(RSDummyContext, "warning",
                      "ASM import: unlink in slot %u while it is rebuilt; its documents are kept "
                      "(%zu so far)", slot, unexpectedUnlinks);
      RS_LOG_ASSERT(false, "ASM import: unexpected unlink in a slot being rebuilt");
      return true;
  }
  return false;
}

size_t ASM_ImportWindow_UnexpectedUnlinks(void) {
  return unexpectedUnlinks;
}
