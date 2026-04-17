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
// mutex-protected hash map. The design relies on the following concurrency
// contract:
//
//   * The `version` field of every slot is written only by the main thread
//     (during `ASM_KeySpaceVersionTracker_IncreaseQueryCount`, which allocates
//     a slot, and during cleanup, which frees it). Workers only read it.
//   * The `count` field is manipulated with atomic RMW operations and may be
//     modified from any thread.
//   * Increase, GetQueryCount, GetTrackedVersionsCount, CanStartTrimming, and
//     cleanup run on the main thread only. Decrease may run from any thread.
//   * `key_space_version` is advanced only by the main thread.
//
// A slot with `version == INVALID_KEYSPACE_VERSION` (0) is free.
//
// Safety of freeing a slot (main-thread cleanup writes `version = 0` when it
// observes `count == 0` for a non-current version): if `count == 0`, all
// matching Decreases issued so far have executed (Decrease is the only
// fetch_sub on `count`, and counts never go negative). No new Decrease(v) can
// appear without a matching Increase(v); a new Increase(v) would allocate a
// fresh slot (possibly reusing the one just freed), not touch a stale one.

// Maximum number of concurrently tracked key space versions. In steady state
// only one version (the current one) is live; during migrations one or two
// older versions may still have in-flight queries. 8 is a generous upper
// bound; exceeding it triggers an assertion.
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
    if (v == INVALID_KEYSPACE_VERSION && free_slot < 0) {
      free_slot = (int)i;
    }
  }
  if (!found) {
    RS_ASSERT(0 <= free_slot < ASM_MAX_LIVE_VERSIONS);
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
        __atomic_store_n(&asm_version_slots[i].version, INVALID_KEYSPACE_VERSION, __ATOMIC_RELAXED);
        //__atomic_store_n(&asm_version_slots[i].count, 0, __ATOMIC_RELAXED);
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
