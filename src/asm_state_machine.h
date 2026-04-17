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
#include "rmutil/rm_assert.h"
#include "rmalloc.h"
#include <stdlib.h>
#include <stddef.h>
#include <pthread.h>

// Sanitizer detection for leak tracking
// This is intended to use the ability of Sanitizer to track memory leaks to detect logical leaks.
// Since we need to keep an exhaustive count of the queries using a specific version, we can use the sanitizer
// to track the number of allocations and deallocations. If there is a logical leak, the sanitizer will
// report it.
#define ASM_SANITIZER_ENABLED 0
#if defined(__has_feature)
# if __has_feature(address_sanitizer)
#  define ASM_SANITIZER_ENABLED 1
# endif
#elif defined(__SANITIZE_ADDRESS__)
# define ASM_SANITIZER_ENABLED 1
#endif

#if ASM_SANITIZER_ENABLED
#include "util/arr_rm_alloc.h"

// Dynamic array to track allocated pointers for leak detection
extern arrayof(int*) asm_sanitizer_allocs;

static void ASM_Sanitizer_Alloc_Init () {
  if (asm_sanitizer_allocs) {
    array_free(asm_sanitizer_allocs);
  }
  asm_sanitizer_allocs = array_new(int*, 100);
}

static void ASM_Sanitizer_Alloc_Free () {
  if (asm_sanitizer_allocs) {
    array_free(asm_sanitizer_allocs);
    asm_sanitizer_allocs = NULL;
  }
}

static void ASM_Santizer_Alloc_Allocate(uint32_t query_key_space_version) {
  if (asm_sanitizer_allocs) {
    int *leak_tracker = (int*)rm_malloc(sizeof(int));
    *leak_tracker = (int)query_key_space_version;
    array_append(asm_sanitizer_allocs, leak_tracker);
  }
}

static void ASM_Sanitizer_Alloc_Deallocate() {
  if (asm_sanitizer_allocs && array_len(asm_sanitizer_allocs) > 0) {
    int *leak_tracker = array_pop(asm_sanitizer_allocs);
    rm_free(leak_tracker);
  }
}
#endif

#define INVALID_KEYSPACE_VERSION 0

// Global version counter for the key space state.
extern uint32_t key_space_version;

/**
 * Initialize the ASM state machine with the local slots.
 */
static inline void ASM_StateMachine_SetLocalSlots(const RedisModuleSlotRangeArray *local_slots) {
  uint32_t version_before = __atomic_load_n(&key_space_version, __ATOMIC_RELAXED);
  uint32_t version_after = slots_tracker_set_local_slots(local_slots);
  if (version_after != version_before) {
    __atomic_store_n(&key_space_version, version_after, __ATOMIC_RELAXED);
  }
}

/**
 * When slots are being imported, we need to mark them as partially available.
 * This means that these slots may exist partially in the key space, but we don't own them.
*/
static inline void ASM_StateMachine_StartImport(const RedisModuleSlotRangeArray *slots) {
  uint32_t version = slots_tracker_mark_partially_available_slots(slots);
  __atomic_store_n(&key_space_version, version, __ATOMIC_RELAXED);
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
  uint32_t version = slots_tracker_mark_partially_available_slots(slots);
  __atomic_store_n(&key_space_version, version, __ATOMIC_RELAXED);
}

/**
 * When slots have finished trimming, we need to remove them from the partially available set.
*/
static inline void ASM_StateMachine_CompleteTrim(const RedisModuleSlotRangeArray *slots) {
  slots_tracker_remove_deleted_slots(slots);
}

// START KEY SPACE VERSION QUERY TRACKER IMPLEMENTATION
//
// Lock-free fixed-size ring of `{version, count}` slots. Replaces the previous
// mutex-protected hash map. A slot with `version == INVALID_KEYSPACE_VERSION`
// (0) is free.
//
// Concurrency contract (must be preserved; relaxing any of these would require
// reintroducing synchronization):
//
//   * `Increase` is main-thread only and called with
//     `query_key_space_version == key_space_version` (enforced by assertion).
//   * `Decrease` may be called from any thread. Each `Increase(v)` is paired
//     with exactly one `Decrease(v)`.
//   * `GetQueryCount` and `CanStartTrimming` are main-thread only.
//   * `key_space_version` is advanced only by the main thread and is
//     monotonically non-decreasing.
//
// Field-access rules:
//
//   * `count` is manipulated only via atomic RMW (`__atomic_fetch_add` /
//     `__atomic_fetch_sub`), so concurrent Decreases serialize at the hardware
//     level.
//   * `version` transitions through three disjoint writes:
//       - `0 -> v` happens in `Increase` on the main thread when allocating a
//         free slot.
//       - `v_stale -> v_current` happens in `Increase` on the main thread
//         when recycling a slot whose count has already drained to 0 but
//         whose version field was not reclaimed by `Decrease` (this occurs
//         when the last `Decrease(v_stale)` ran while `v_stale` was still
//         the current version, so the reclaim branch did not fire).
//       - `v -> 0` happens in `Decrease` on any thread when `fetch_sub`
//         returns `prev == 1` AND `v != key_space_version`.
//     These never target the same slot concurrently: the caller invariant
//     `query_key_space_version == key_space_version` on `Increase` means the
//     slot `Increase` matches or allocates has `version == current`, while
//     the reclaim branch only fires when `version != current`. A stale slot
//     that `Increase` recycles has `count == 0`, so no in-flight
//     `Decrease(v_stale)` can exist (each `Decrease` pairs with an
//     outstanding `Increase`, which would have kept `count > 0`); and
//     `Decrease(v_current)` cannot touch a slot whose version is `v_stale`.
//   * Once a slot is reclaimed, monotonicity of `key_space_version` guarantees
//     no future `Increase(v)` or `Decrease(v)` can be issued for that old
//     value, so no surviving operation observes the slot in its old role.

// Capacity invariant: at most `ASM_MAX_LIVE_VERSIONS` distinct key-space
// versions may have in-flight queries (count > 0) simultaneously. The ring has
// exactly one slot per tracked version, and a slot is reclaimed only when its
// count drops to 0 AND its version is no longer the current one. Consequently,
// every ASM state-machine command that advances `key_space_version` (e.g.
// `SetLocalSlots` when local ownership changes, `StartImport`, `StartTrim`)
// consumes a slot for any previous version that still has in-flight queries.
//
// This puts a hard limit on how many consecutive ASM commands can be issued
// before the oldest in-flight queries drain: if more than
// `ASM_MAX_LIVE_VERSIONS` version advancements occur while any query from each
// of the preceding versions is still running, the next `Increase` cannot find
// a free slot and triggers an assertion. In steady state only one version is
// live; during migrations one or two older versions may still have in-flight
// queries, so the limit is not expected to be reached in normal operation.
#define ASM_MAX_LIVE_VERSIONS 16

typedef struct {
  uint32_t version; // INVALID_KEYSPACE_VERSION == free slot
  uint32_t count;
} ASM_VersionSlot;

extern ASM_VersionSlot asm_version_slots[ASM_MAX_LIVE_VERSIONS];

#if ASM_SANITIZER_ENABLED
// Guards the sanitizer leak-tracking array only. The tracker itself is
// lock-free; this mutex exists purely because `asm_sanitizer_allocs` is a
// dynamic array that is not safe against concurrent append/pop.
extern pthread_mutex_t asm_sanitizer_mutex;
#endif

static inline void ASM_KeySpaceVersionTracker_Init() {
  for (size_t i = 0; i < ASM_MAX_LIVE_VERSIONS; i++) {
    __atomic_store_n(&asm_version_slots[i].version, INVALID_KEYSPACE_VERSION, __ATOMIC_RELAXED);
    __atomic_store_n(&asm_version_slots[i].count, 0, __ATOMIC_RELAXED);
  }

#if ASM_SANITIZER_ENABLED
  ASM_Sanitizer_Alloc_Init();
  pthread_mutex_init(&asm_sanitizer_mutex, NULL);
#endif
}

static inline void ASM_KeySpaceVersionTracker_Destroy() {
  for (size_t i = 0; i < ASM_MAX_LIVE_VERSIONS; i++) {
    __atomic_store_n(&asm_version_slots[i].version, INVALID_KEYSPACE_VERSION, __ATOMIC_RELAXED);
    __atomic_store_n(&asm_version_slots[i].count, 0, __ATOMIC_RELAXED);
  }

#if ASM_SANITIZER_ENABLED
  ASM_Sanitizer_Alloc_Free();
  pthread_mutex_destroy(&asm_sanitizer_mutex);
#endif
}

/* Returns true if the slot at index `i`, whose version field was just observed
 * to hold `v`, can be repurposed by `Increase` for `current_version`.
 *
 * A slot is reusable if it is truly free (`v == INVALID_KEYSPACE_VERSION`) or
 * if it holds an old version whose count has drained to 0. The latter case
 * arises when the last `Decrease(v)` ran while `v` was still the current
 * version, so `Decrease` could not reclaim the slot. Since `Increase` is
 * main-thread only and `current_version == key_space_version`, a stale slot
 * has `v != current_version` and `count == 0`, which means no concurrent
 * `Decrease` can target it; the main thread is the sole writer and can safely
 * repurpose it. */
static inline bool ASM_SlotIsReusableForIncrease(size_t i, uint32_t v, uint32_t current_version) {
  if (v == INVALID_KEYSPACE_VERSION) {
    return true;
  }
  if (v == current_version) {
    return false;
  }
  return __atomic_load_n(&asm_version_slots[i].count, __ATOMIC_RELAXED) == 0;
}

/* Increase the query count for `query_key_space_version`. Main-thread only. */
static void ASM_KeySpaceVersionTracker_IncreaseQueryCount(uint32_t query_key_space_version) {
  RS_ASSERT(query_key_space_version == key_space_version);
  int free_slot = -1;
  bool found = false;
  for (size_t i = 0; i < ASM_MAX_LIVE_VERSIONS; i++) {
    uint32_t v = __atomic_load_n(&asm_version_slots[i].version, __ATOMIC_RELAXED);
    if (v == query_key_space_version) {
      __atomic_fetch_add(&asm_version_slots[i].count, 1, __ATOMIC_RELAXED);
      found = true;
      break;
    }
    if (free_slot < 0 && ASM_SlotIsReusableForIncrease(i, v, query_key_space_version)) {
      free_slot = (int)i;
    }
  }
  if (!found) {
    RS_ASSERT(0 <= free_slot && free_slot < ASM_MAX_LIVE_VERSIONS);
    __atomic_store_n(&asm_version_slots[free_slot].count, 1, __ATOMIC_RELAXED);
    __atomic_store_n(&asm_version_slots[free_slot].version, query_key_space_version, __ATOMIC_RELEASE);
  }

#if ASM_SANITIZER_ENABLED
  pthread_mutex_lock(&asm_sanitizer_mutex);
  ASM_Santizer_Alloc_Allocate(query_key_space_version);
  pthread_mutex_unlock(&asm_sanitizer_mutex);
#endif
}

/* Decrease the query count for `query_key_space_version`. Safe to call from
 * any thread. */
static void ASM_KeySpaceVersionTracker_DecreaseQueryCount(uint32_t query_key_space_version) {
  for (size_t i = 0; i < ASM_MAX_LIVE_VERSIONS; i++) {
    uint32_t v = __atomic_load_n(&asm_version_slots[i].version, __ATOMIC_ACQUIRE);
    if (v == query_key_space_version) {
      uint32_t prev = __atomic_fetch_sub(&asm_version_slots[i].count, 1, __ATOMIC_RELAXED);
      RS_LOG_ASSERT(prev > 0, "Query version count underflow in ASM tracker");
      if (prev == 1 && v != __atomic_load_n(&key_space_version, __ATOMIC_RELAXED)) {
        // We know that this was the last query for this version, and that the version is not the current one, so no query
        // will ever use this version again. We can safely reclaim the slot.
        __atomic_store_n(&asm_version_slots[i].version, INVALID_KEYSPACE_VERSION, __ATOMIC_RELAXED);
        __atomic_store_n(&asm_version_slots[i].count, 0, __ATOMIC_RELAXED);
      }
#if ASM_SANITIZER_ENABLED
      pthread_mutex_lock(&asm_sanitizer_mutex);
      ASM_Sanitizer_Alloc_Deallocate();
      pthread_mutex_unlock(&asm_sanitizer_mutex);
#endif
      return;
    }
  }
  RS_LOG_ASSERT(false, "Query version not found in tracker");
}

/* Get the number of queries that are using a specific version. For testing purposes. */
static inline uint32_t ASM_KeySpaceVersionTracker_GetQueryCount(uint32_t query_version) {
  for (size_t i = 0; i < ASM_MAX_LIVE_VERSIONS; i++) {
    uint32_t v = __atomic_load_n(&asm_version_slots[i].version, __ATOMIC_RELAXED);
    if (v == query_version) {
      return __atomic_load_n(&asm_version_slots[i].count, __ATOMIC_RELAXED);
    }
  }
  return 0;
}

static int ASM_AccountRequestFinished(uint32_t keySpaceVersion, size_t innerQueriesCount) {
  if (keySpaceVersion != INVALID_KEYSPACE_VERSION) {
    for (size_t i = 0; i < innerQueriesCount; i++) {
      ASM_KeySpaceVersionTracker_DecreaseQueryCount(keySpaceVersion);
    }
  }
  return REDISMODULE_OK;
}

// END KEY SPACE VERSION QUERY TRACKER IMPLEMENTATION

/**
 * Resets the ASM state machine to its initial state.
 */
static inline void ASM_StateMachine_Init() {
  ASM_KeySpaceVersionTracker_Init();
  slots_tracker_reset();
}

/*
 * Frees all resources used by the ASM state machine.
*/
static inline void ASM_StateMachine_End() {
  ASM_KeySpaceVersionTracker_Destroy();
}

/*
* This function aims to validate if the system is in a state where we can start trimming.
* The logic here is as follows:
* - If the KeySpaceVersionTracker for the current version is 0, it means there are no queries using the current version, and we can start trimming.
* - Otherwise, we can't start trimming.
*
* @warning This has to be called from the main thread only. It assumes is called when the system understands
* that all the shards have updated their topology, and therefore no more queries would arrive with the old slot ranges that
* are about to be trimmed.
*
* @return true if we can start trimming, false otherwise.
*/
static bool ASM_CanStartTrimming(void) {
  uint32_t current_version = __atomic_load_n(&key_space_version, __ATOMIC_RELAXED);
  return ASM_KeySpaceVersionTracker_GetQueryCount(current_version) == 0;
}
