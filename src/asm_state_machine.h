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
#include "util/khash.h"
#include "deps/rmutil/rm_assert.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <pthread.h>

// Sanitizer detection for leak tracking
#define ASM_SANITIZER_ENABLED 0
#if defined(__has_feature)
# if __has_feature(address_sanitizer)
#  define ASM_SANITIZER_ENABLED 1
# endif
#elif defined(__SANITIZE_ADDRESS__)
# define ASM_SANITIZER_ENABLED 1
#endif

#if ASM_SANITIZER_ENABLED
// Dynamic array to track allocated pointers for leak detection
static int** asm_sanitizer_allocs = NULL;
static int asm_sanitizer_alloc_count = 0;
static int asm_sanitizer_alloc_capacity = 0;
#define ASM_SANITIZER_INITIAL_CAPACITY 100
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

// Define hash map type for tracking query versions -> query counts
KHASH_MAP_INIT_INT(query_key_space_version_tracker, uint32_t);

// Static hash map instance for tracking query versions
extern khash_t(query_key_space_version_tracker) *query_key_space_version_map;

// Mutex for thread-safe hash map operations
static pthread_mutex_t query_version_tracker_mutex = PTHREAD_MUTEX_INITIALIZER;

static inline void ASM_KeySpaceVersionTracker_Init() {
  if (query_key_space_version_map != NULL) {
    kh_destroy(query_key_space_version_tracker, query_key_space_version_map);
  }
  query_key_space_version_map = kh_init(query_key_space_version_tracker);

#if ASM_SANITIZER_ENABLED
  // Initialize sanitizer allocation tracking
  if (asm_sanitizer_allocs) {
    free(asm_sanitizer_allocs);
  }
  asm_sanitizer_allocs = (int**)malloc(ASM_SANITIZER_INITIAL_CAPACITY * sizeof(int*));
  asm_sanitizer_alloc_count = 0;
  asm_sanitizer_alloc_capacity = ASM_SANITIZER_INITIAL_CAPACITY;
#endif
}

static inline void ASM_KeySpaceVersionTracker_Destroy() {
  if (query_key_space_version_map != NULL) {
    kh_destroy(query_key_space_version_tracker, query_key_space_version_map);
    query_key_space_version_map = NULL;
  }

#if ASM_SANITIZER_ENABLED
  // Clean up any remaining sanitizer allocations
  if (asm_sanitizer_allocs) {
    // Do not free the integers, so that sanitizer tests would show leaks
    free(asm_sanitizer_allocs);
    asm_sanitizer_allocs = NULL;
    asm_sanitizer_alloc_count = 0;
    asm_sanitizer_alloc_capacity = 0;
  }
#endif
}

static inline void ASM_KeySpaceVersionTracker_IncreaseQueryCount(uint32_t query_key_space_version) {
  pthread_mutex_lock(&query_version_tracker_mutex);

  int ret;
  khiter_t k = kh_put(query_key_space_version_tracker, query_key_space_version_map, query_key_space_version, &ret);

  if (ret == 0) {
    kh_value(query_key_space_version_map, k)++;
  } else {
    kh_value(query_key_space_version_map, k) = 1;
  }

  pthread_mutex_unlock(&query_version_tracker_mutex);

#if ASM_SANITIZER_ENABLED
  // Allocate a dummy integer for sanitizer leak detection
  // We allocate one for each query count increase
  if (asm_sanitizer_allocs) {
    // Check if we need to reallocate the array
    if (asm_sanitizer_alloc_count >= asm_sanitizer_alloc_capacity) {
      int new_capacity = asm_sanitizer_alloc_capacity * 2;
      int** new_allocs = (int**)realloc(asm_sanitizer_allocs, new_capacity * sizeof(int*));
      if (new_allocs) {
        asm_sanitizer_allocs = new_allocs;
        asm_sanitizer_alloc_capacity = new_capacity;
      }
    }

    // Allocate the tracking integer if we have space
    if (asm_sanitizer_alloc_count < asm_sanitizer_alloc_capacity) {
      int *leak_tracker = (int*)malloc(sizeof(int));
      if (leak_tracker) {
        *leak_tracker = (int)query_key_space_version; // Store version for debugging
        asm_sanitizer_allocs[asm_sanitizer_alloc_count++] = leak_tracker;
      }
    }
  }
#endif
}

static inline void ASM_KeySpaceVersionTracker_DecreaseQueryCount(uint32_t query_key_space_version) {
  pthread_mutex_lock(&query_version_tracker_mutex);

  khiter_t k = kh_get(query_key_space_version_tracker, query_key_space_version_map, query_key_space_version);
  RS_LOG_ASSERT(k != kh_end(query_key_space_version_map), "Query version not found in tracker");

  uint32_t *count = &kh_value(query_key_space_version_map, k);
  if (*count > 0) {
    (*count)--;
  }

  if (*count == 0) {
    uint32_t current_version = __atomic_load_n(&key_space_version, __ATOMIC_RELAXED);
    if (query_key_space_version < current_version) {
      kh_del(query_key_space_version_tracker, query_key_space_version_map, k);
    }
  }

  pthread_mutex_unlock(&query_version_tracker_mutex);

#if ASM_SANITIZER_ENABLED
  // Deallocate a dummy integer for sanitizer leak detection
  // We deallocate one for each query count decrease (LIFO order)
  if (asm_sanitizer_alloc_count > 0) {
    int *leak_tracker = asm_sanitizer_allocs[--asm_sanitizer_alloc_count];
    if (leak_tracker) {
      free(leak_tracker);
    }
  }
#endif
}

/* Get the number of queries that are using a specific version, this is intended to be used in tests only. */
static inline uint32_t ASM_KeySpaceVersionTracker_GetQueryCount(uint32_t query_version) {
  pthread_mutex_lock(&query_version_tracker_mutex);
  khiter_t k = kh_get(query_key_space_version_tracker, query_key_space_version_map, query_version);
  uint32_t result = (k == kh_end(query_key_space_version_map)) ? 0 : kh_value(query_key_space_version_map, k);
  pthread_mutex_unlock(&query_version_tracker_mutex);
  return result;
}

static inline uint32_t ASM_KeySpaceVersionTracker_GetTrackedVersionsCount() {
  pthread_mutex_lock(&query_version_tracker_mutex);
  uint32_t result = kh_size(query_key_space_version_map);
  pthread_mutex_unlock(&query_version_tracker_mutex);
  return result;
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
