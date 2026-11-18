/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "disk_asm_import_window.h"

#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#define ASM_IMPORT_WINDOW_NUM_SLOTS 16384
#define WORD_BITS 64
#define NUM_WORDS (ASM_IMPORT_WINDOW_NUM_SLOTS / WORD_BITS)

// Allocated while any slot is in the window, so a shard that never imports pays nothing.
static uint64_t *slotBits = NULL;
uint32_t DiskASM_ImportWindow_numSlots_g = 0;

static bool testBit(unsigned int slot) {
  return slotBits[slot / WORD_BITS] & (1ULL << (slot % WORD_BITS));
}

static void setBit(unsigned int slot, bool in) {
  if (testBit(slot) == in) return;
  RS_LOG_ASSERT(in || DiskASM_ImportWindow_numSlots_g > 0,
                "ASM import window: slot count underflow");
  slotBits[slot / WORD_BITS] ^= 1ULL << (slot % WORD_BITS);
  if (in) {
    DiskASM_ImportWindow_numSlots_g++;
  } else {
    DiskASM_ImportWindow_numSlots_g--;
  }
}

static void update(const RedisModuleSlotRangeArray *slots, bool in) {
  if (!slots) return;
  if (!slotBits) {
    if (!in) return;
    slotBits = rm_calloc(NUM_WORDS, sizeof(*slotBits));
  }
  for (int32_t r = 0; r < slots->num_ranges; r++) {
    const RedisModuleSlotRange *range = &slots->ranges[r];
    RS_LOG_ASSERT(range->start <= range->end && range->end < ASM_IMPORT_WINDOW_NUM_SLOTS,
                  "invalid slot range");
    for (unsigned int slot = range->start; slot <= range->end; slot++) {
      setBit(slot, in);
    }
  }
  if (DiskASM_ImportWindow_numSlots_g == 0) {
    DiskASM_ImportWindow_Reset();
  }
}

void DiskASM_ImportWindow_Open(const RedisModuleSlotRangeArray *slots) {
  update(slots, true);
}

void DiskASM_ImportWindow_Close(const RedisModuleSlotRangeArray *slots) {
  update(slots, false);
}

void DiskASM_ImportWindow_Reset(void) {
  rm_free(slotBits);
  slotBits = NULL;
  DiskASM_ImportWindow_numSlots_g = 0;
}

bool DiskASM_ImportWindow_ContainsSlot(unsigned int slot) {
  RS_LOG_ASSERT(slot < ASM_IMPORT_WINDOW_NUM_SLOTS, "invalid slot");
  return slotBits && testBit(slot);
}

bool DiskASM_ImportWindow_ContainsKeySlot(const RedisModuleString *key) {
  size_t len;
  const char *name = RedisModule_StringPtrLen(key, &len);
  return DiskASM_ImportWindow_ContainsSlot(RedisModule_ClusterKeySlotC(name, len));
}
