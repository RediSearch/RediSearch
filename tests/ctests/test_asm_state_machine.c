/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "redismodule.h"
#include "slot_ranges.h"
#include "rmalloc.h"
#include "asm_state_machine.h"
#include "src/redisearch_rs/headers/slots_tracker.h"
#include "test_util.h"
#include "rmutil/alloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

// Helper function to create slot range arrays
static RedisModuleSlotRangeArray* createSlotRangeArray(uint16_t start, uint16_t end) {
  size_t array_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange);
  RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)rm_malloc(array_size);
  array->num_ranges = 1;
  array->ranges[0].start = start;
  array->ranges[0].end = end;
  return array;
}

// Helper function to create slot range arrays with multiple ranges
static RedisModuleSlotRangeArray* createMultiSlotRangeArray(uint16_t ranges[][2], int num_ranges) {
  size_t array_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * num_ranges;
  RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)rm_malloc(array_size);
  array->num_ranges = num_ranges;

  for (int i = 0; i < num_ranges; i++) {
    array->ranges[i].start = ranges[i][0];
    array->ranges[i].end = ranges[i][1];
  }

  return array;
}

// Helper function to free slot range arrays
static void freeSlotRangeArray(RedisModuleSlotRangeArray* array) {
  if (array) {
    rm_free(array);
  }
}

int testInitialization() {
  ASM_StateMachine_Init();
  atomic_store_explicit(&key_space_version, 0, memory_order_relaxed);
  uint32_t initial_version = atomic_load_explicit(&key_space_version, memory_order_relaxed);
  RedisModuleSlotRangeArray* init_slots = createSlotRangeArray(100, 199);
  ASM_StateMachine_SetLocalSlots(init_slots);
  OptionSlotTrackerVersion version = slots_tracker_check_availability(init_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, atomic_load_explicit(&key_space_version, memory_order_relaxed));
  // The slots tracker starts at version 1, and set local slots increments it by 1
  ASSERT_EQUAL(version.version, 2);
  freeSlotRangeArray(init_slots);
  ASM_StateMachine_End();
  return 0;
}

int testImportWorkflow() {
  ASM_StateMachine_Init();
  atomic_store_explicit(&key_space_version, 0, memory_order_relaxed);

  RedisModuleSlotRangeArray* init_slots = createSlotRangeArray(5, 20);
  RedisModuleSlotRangeArray* import_slots = createSlotRangeArray(100, 199);
  uint16_t complete_ranges[][2] = {{5, 20}, {100, 199}};
  RedisModuleSlotRangeArray* complete_slots = createMultiSlotRangeArray(complete_ranges, 2);

  ASM_StateMachine_SetLocalSlots(init_slots);
  OptionSlotTrackerVersion version = slots_tracker_check_availability(init_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, atomic_load_explicit(&key_space_version, memory_order_relaxed));
  ASSERT_EQUAL(version.version, 2);
  version = slots_tracker_check_availability(import_slots);
  ASSERT_FALSE(version.is_some);

  ASM_StateMachine_StartImport(import_slots);
  version = slots_tracker_check_availability(init_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_FALSE(version.version == atomic_load_explicit(&key_space_version, memory_order_relaxed));
  ASSERT_EQUAL(version.version, 0); // Unstable THERE ARE PARTIALLY AVAILABLE SLOTS that u need to filter
  ASSERT_EQUAL(atomic_load_explicit(&key_space_version, memory_order_relaxed), 3);
  version = slots_tracker_check_availability(complete_slots);
  ASSERT_FALSE(version.is_some);
  version = slots_tracker_check_availability(import_slots);
  ASSERT_FALSE(version.is_some);

  ASM_StateMachine_CompleteImport(import_slots);
  version = slots_tracker_check_availability(complete_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, atomic_load_explicit(&key_space_version, memory_order_relaxed)); // Stable, Local Equals while no partially available slots
  ASSERT_EQUAL(version.version, 3);

  version = slots_tracker_check_availability(import_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, 0); // Unstable, Local Covers but not equals (can query, but must filter (there are more slots available than the query requires))
  version = slots_tracker_check_availability(init_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, 0); // Unstable, Local Covers but not equals (can query, but must filter (there are more slots available than the query requires))

  freeSlotRangeArray(import_slots);
  freeSlotRangeArray(init_slots);
  freeSlotRangeArray(complete_slots);
  ASM_StateMachine_End();
  return 0;
}

int testImportContinuousWorkflow() {
  ASM_StateMachine_Init();
  atomic_store_explicit(&key_space_version, 0, memory_order_relaxed);

  RedisModuleSlotRangeArray* init_slots = createSlotRangeArray(5, 99);
  RedisModuleSlotRangeArray* import_slots = createSlotRangeArray(100, 199);
  RedisModuleSlotRangeArray* complete_slots = createSlotRangeArray(5, 199);
  ASM_StateMachine_SetLocalSlots(init_slots);
  OptionSlotTrackerVersion version = slots_tracker_check_availability(init_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, atomic_load_explicit(&key_space_version, memory_order_relaxed));
  ASSERT_EQUAL(version.version, 2);
  version = slots_tracker_check_availability(import_slots);
  ASSERT_FALSE(version.is_some);

  ASM_StateMachine_StartImport(import_slots);
  version = slots_tracker_check_availability(init_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_FALSE(version.version == atomic_load_explicit(&key_space_version, memory_order_relaxed));
  ASSERT_EQUAL(version.version, 0); // Unstable THERE ARE PARTIALLY AVAILABLE SLOTS that u need to filter
  ASSERT_EQUAL(atomic_load_explicit(&key_space_version, memory_order_relaxed), 3);
  version = slots_tracker_check_availability(complete_slots);
  ASSERT_FALSE(version.is_some);
  version = slots_tracker_check_availability(import_slots);
  ASSERT_FALSE(version.is_some);

  ASM_StateMachine_CompleteImport(import_slots);
  version = slots_tracker_check_availability(complete_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, atomic_load_explicit(&key_space_version, memory_order_relaxed)); // Stable, Local Equals while no partially available slots
  ASSERT_EQUAL(version.version, 3);

  version = slots_tracker_check_availability(import_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, 0); // Unstable, Local Covers but not equals (can query, but must filter (there are more slots available than the query requires))
  version = slots_tracker_check_availability(init_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, 0); // Unstable, Local Covers but not equals (can query, but must filter (there are more slots available than the query requires))

  freeSlotRangeArray(import_slots);
  freeSlotRangeArray(init_slots);
  freeSlotRangeArray(complete_slots);
  ASM_StateMachine_End();
  return 0;
}

int testMigrationTrimmingWorkflow() {
  ASM_StateMachine_Init();
  RedisModuleSlotRangeArray* init_slots = createSlotRangeArray(5, 199);
  RedisModuleSlotRangeArray* migration_slots = createSlotRangeArray(100, 199);
  RedisModuleSlotRangeArray* disjoint_slots = createSlotRangeArray(5, 99);

  ASM_StateMachine_SetLocalSlots(init_slots);
  OptionSlotTrackerVersion version = slots_tracker_check_availability(init_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, atomic_load_explicit(&key_space_version, memory_order_relaxed)); // Stable, Local Equals while no partially available slots
  ASSERT_EQUAL(version.version, 2);
  version = slots_tracker_check_availability(migration_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, 0); // Unstable, Local Covers but not equals (can query, but must filter (there are more slots available than the query requires))
  version = slots_tracker_check_availability(disjoint_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, 0); // Unstable, Local Covers but not equals (can query, but must filter (there are more slots available than the query requires))

  // Start migration does nothing
  ASM_StateMachine_CompleteMigration(migration_slots);
  version = slots_tracker_check_availability(init_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, atomic_load_explicit(&key_space_version, memory_order_relaxed)); // Stable, Local Equals while no partially available slots
  ASSERT_EQUAL(version.version, 2);
  version = slots_tracker_check_availability(migration_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, 0); // Unstable, Local Covers but not equals (can query, but must filter (there are more slots available than the query requires))
  version = slots_tracker_check_availability(disjoint_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, 0); // Unstable, Local Covers but not equals (can query, but must filter (there are more slots available than the query requires))

  ASM_StateMachine_StartTrim(migration_slots);
  version = slots_tracker_check_availability(init_slots);
  ASSERT_FALSE(version.is_some);
  version = slots_tracker_check_availability(migration_slots);
  ASSERT_FALSE(version.is_some);
  version = slots_tracker_check_availability(disjoint_slots);
  ASSERT_TRUE(version.is_some);
  ASSERT_EQUAL(version.version, 0); // Unstable, Local Covers but not equals (can query, but must filter (there are more slots available than the query requires))

  ASM_StateMachine_CompleteTrim(migration_slots);
  version = slots_tracker_check_availability(init_slots);
  ASSERT_FALSE(version.is_some);
  version = slots_tracker_check_availability(migration_slots);
  ASSERT_FALSE(version.is_some);
  version = slots_tracker_check_availability(disjoint_slots);
  ASSERT_EQUAL(version.version, atomic_load_explicit(&key_space_version, memory_order_relaxed)); // Stable, Local Equals while no partially available slots
  ASSERT_EQUAL(version.version, 3);

  freeSlotRangeArray(migration_slots);
  freeSlotRangeArray(init_slots);
  freeSlotRangeArray(disjoint_slots);
  ASM_StateMachine_End();
  return 0;
}

int testKeySpaceVersionTracker() {
  ASM_StateMachine_Init();
  atomic_store_explicit(&key_space_version, 1, memory_order_relaxed);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 0);
  // One query is using version 1
  ASM_KeySpaceVersionTracker_IncreaseQueryCount(1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(1), 1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);
  // Another query starts using version 1
  ASM_KeySpaceVersionTracker_IncreaseQueryCount(1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(1), 2);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);

  // One query finishes using version 1
  ASM_KeySpaceVersionTracker_DecreaseQueryCount(1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(1), 1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);

  // Another query finishes using version 1
  ASM_KeySpaceVersionTracker_DecreaseQueryCount(1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(1), 0);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);

  // Another query starts using version 1 and finish
  ASM_KeySpaceVersionTracker_IncreaseQueryCount(1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(1), 1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);
  ASM_KeySpaceVersionTracker_DecreaseQueryCount(1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(1), 0);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);

  // Another two queries start using version 1
  ASM_KeySpaceVersionTracker_IncreaseQueryCount(1);
  ASM_KeySpaceVersionTracker_IncreaseQueryCount(1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(1), 2);
  atomic_store_explicit(&key_space_version, 2, memory_order_relaxed);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);
  ASM_KeySpaceVersionTracker_DecreaseQueryCount(1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(1), 1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);
  // The last one using version 1 finishes (Now version 1 is not tracked anymore)
  ASM_KeySpaceVersionTracker_DecreaseQueryCount(1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(1), 0);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 0);

  // Version 2 is now being used
  ASM_KeySpaceVersionTracker_IncreaseQueryCount(2);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(2), 1);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);
  ASM_KeySpaceVersionTracker_DecreaseQueryCount(2);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetQueryCount(2), 0);
  ASSERT_EQUAL(ASM_KeySpaceVersionTracker_GetTrackedVersionsCount(), 1);

  ASM_StateMachine_End();

  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testInitialization);
  TESTFUNC(testImportWorkflow);
  TESTFUNC(testImportContinuousWorkflow);
  TESTFUNC(testMigrationTrimmingWorkflow);
  TESTFUNC(testKeySpaceVersionTracker);
})
