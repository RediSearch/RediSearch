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

#ifdef __cplusplus
#include <atomic>
extern "C" {
#else
#include <stdatomic.h>
#endif

#define INVALID_KEYSPACE_VERSION 0

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

// START KEY SPACE VERSION QUERY TRACKER IMPLEMENTATION

// Define hash map type for tracking query versions -> query counts
KHASH_MAP_INIT_INT(query_version_tracker, uint32_t);

// Static hash map instance for tracking query versions
extern khash_t(query_version_tracker) *query_version_map;

static inline void ASM_KeySpaceVersionTracker_Init() {
  if (query_version_map != NULL) {
    kh_destroy(query_version_tracker, query_version_map);
  }
  query_version_map = kh_init(query_version_tracker);
}

static inline void ASM_KeySpaceVersionTracker_Destroy() {
  if (query_version_map != NULL) {
    kh_destroy(query_version_tracker, query_version_map);
    query_version_map = NULL;
  }
}

static inline void ASM_KeySpaceVersionTracker_IncreaseQueryCount(uint32_t query_version) {
  // If map doesn't exist, nothing to increase  // Find or create entry for this query version
  int ret;
  khiter_t k = kh_put(query_version_tracker, query_version_map, query_version, &ret);

  if (ret == 0) {
    // Key already exists, increment the count
    kh_value(query_version_map, k)++;
  } else {
    // New key, set count to 1
    kh_value(query_version_map, k) = 1;
  }
}

static inline void ASM_KeySpaceVersionTracker_DecreaseQueryCount(uint32_t query_version) {
  // Find the entry for this query version
  khiter_t k = kh_get(query_version_tracker, query_version_map, query_version);
  RS_LOG_ASSERT(k != kh_end(query_version_map), "Query version not found in tracker");

  // Decrease the count
  uint32_t *count = &kh_value(query_version_map, k);
  if (*count > 0) {
    (*count)--;
  }

  // If count reaches 0 and query version is smaller than current version, remove the entry
  if (*count == 0) {
#ifdef __cplusplus
    uint32_t current_version = key_space_version.load(std::memory_order_relaxed);
#else
    uint32_t current_version = atomic_load_explicit(&key_space_version, memory_order_relaxed);
#endif
    if (query_version < current_version) {
      kh_del(query_version_tracker, query_version_map, k);
    }
  }
}

/* Get the number of queries that are using a specific version, this is intended to be used in tests only. */
static inline uint32_t ASM_KeySpaceVersionTracker_GetQueryCount(uint32_t query_version) {
  khiter_t k = kh_get(query_version_tracker, query_version_map, query_version);
  if (k == kh_end(query_version_map)) {
    return 0;
  }
  return kh_value(query_version_map, k);
}

static inline uint32_t ASM_KeySpaceVersionTracker_GetTrackedVersionsCount() {
  return kh_size(query_version_map);
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
#ifdef __cplusplus
  uint32_t current_version = key_space_version.load(std::memory_order_relaxed);
#else
  uint32_t current_version = atomic_load_explicit(&key_space_version, memory_order_relaxed);
#endif
  return ASM_KeySpaceVersionTracker_GetQueryCount(current_version) == 0;
}


#ifdef __cplusplus
}
#endif
