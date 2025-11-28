/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "slots_tracker.h"
#ifdef __cplusplus
#include <atomic>
extern "C" {
#else
#include <stdatomic.h>
#endif


// Global version counter for the key space state.
// Aligned with the definition in result_processor.c
#ifdef __cplusplus
extern std::atomic<uint32_t> key_space_version;
#else
extern atomic_uint key_space_version;
#endif

/**
 * Initialize the ASM state machine with the local slots.
 */
static inline void ASM_StateMachine_SetLocalSlots(const RedisModuleSlotRangeArray *local_slots) {
#ifdef __cplusplus
  uint32_t version_before = key_space_version.load(std::memory_order_relaxed);
  uint32_t version_after = slots_tracker_set_local_slots(local_slots);
  if (version_after != version_before) {
    key_space_version.store(version_after, std::memory_order_relaxed);
  }
#else
  uint32_t version_before = atomic_load_explicit(&key_space_version, memory_order_relaxed);
  uint32_t version_after = slots_tracker_set_local_slots(local_slots);
  if (version_after != version_before) {
    atomic_store_explicit(&key_space_version, version_after, memory_order_relaxed);
  }
#endif
}

/**
 * When slots are being imported, we need to mark them as partially available.
 * This means that these slots may exist partially in the key space, but we don't own them.
*/
static inline void ASM_StateMachine_StartImport(const RedisModuleSlotRangeArray *slots) {
#ifdef __cplusplus
  uint32_t version = slots_tracker_mark_partially_available_slots(slots);
  key_space_version.store(version, std::memory_order_relaxed);
#else
  uint32_t version = slots_tracker_mark_partially_available_slots(slots);
  atomic_store_explicit(&key_space_version, version, memory_order_relaxed);
#endif
}

/*
* When slots have finished importing, we need to promote the slots to local ownership.
*/
static inline void ASM_StateMachine_CompleteImport(const RedisModuleSlotRangeArray *slots) {
  slots_tracker_promote_to_local_slots(slots);
}

/**
 * When slots have finished migrating, we need to mark them as fully available but not owned.
 * This means that these slots are fully available in the key space, but we don't own them, as they will start trimming.
*/
static inline void ASM_StateMachine_CompleteMigration(const RedisModuleSlotRangeArray *slots) {
  slots_tracker_mark_fully_available_slots(slots);
}

/*
* When slots are being trimmed, we mark them as partially available.
*/
static inline void ASM_StateMachine_StartTrim(const RedisModuleSlotRangeArray *slots) {
  #ifdef __cplusplus
  uint32_t version = slots_tracker_mark_partially_available_slots(slots);
  key_space_version.store(version, std::memory_order_relaxed);
#else
  uint32_t version = slots_tracker_mark_partially_available_slots(slots);
  atomic_store_explicit(&key_space_version, version, memory_order_relaxed);
#endif
}

/**
 * When slots have finished trimming, we need to remove them from the partially available set.
*/
static inline void ASM_StateMachine_CompleteTrim(const RedisModuleSlotRangeArray *slots) {
  slots_tracker_remove_deleted_slots(slots);
}

/**
 * Resets the ASM state machine to its initial state. (Only used for testing)
 */
static inline void ASM_StateMachine_Reset() {
  slots_tracker_reset_for_testing();
}

#ifdef __cplusplus
}
#endif
