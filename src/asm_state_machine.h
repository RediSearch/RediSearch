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
extern "C" {
#endif

/**
 * Initialize the ASM state machine with the local slots.
 */
void ASM_StateMachine_SetLocalSlots(const RedisModuleSlotRangeArray *local_slots) {
  slots_tracker_set_local_slots(local_slots);
}

/**
 * When slots are being imported, we need to mark them as partially available.
 * This means that these slots may exist partially in the key space, but we don't own them.
*/
void ASM_StateMachine_StartImport(const RedisModuleSlotRangeArray *slots) {
  slots_tracker_mark_partially_available_slots(slots);
}

/*
* When slots have finished importing, we need to promote the slots to local ownership.
*/
void ASM_StateMachine_CompleteImport(const RedisModuleSlotRangeArray *slots) {
  slots_tracker_promote_to_local_slots(slots);
}

/**
 * When slots have finished migrating, we need to mark them as fully available but not owned.
 * This means that these slots are fully available in the key space, but we don't own them, as they will start trimming.
*/
void ASM_StateMachine_CompleteMigration(const RedisModuleSlotRangeArray *slots) {
  slots_tracker_mark_fully_available_slots(slots);
}

/**
 * When slots are being trimmed, we need to check if there is a fully available overlap, to detect if they come from a failed import or not.
 * If there is, it means that the trim is consequence of a successful migration, and we need to drain the worker thread pool and bump the key space version.
 * The draining function is passed as a parameter to allow for easier unit testing
*/
void ASM_StateMachine_StartTrim(const RedisModuleSlotRangeArray *slots, void (*draining_bound_fn)(void)) {
  if (slots_tracker_has_fully_available_overlap(slots)) {
    draining_bound_fn();
  }
  slots_tracker_mark_partially_available_slots(slots);
}

/**
 * When slots have finished trimming, we need to remove them from the partially available set.
*/
void ASM_StateMachine_CompleteTrim(const RedisModuleSlotRangeArray *slots) {
  slots_tracker_remove_deleted_slots(slots);
}

#ifdef __cplusplus
}
#endif
