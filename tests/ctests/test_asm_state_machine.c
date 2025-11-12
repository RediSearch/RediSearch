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

// Global flag to track if draining was called
static int draining_called = 0;

// Mock draining function for testing
static void mock_draining_function() {
  draining_called = 1;
}

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
  draining_called = 0;
  RedisModuleSlotRangeArray* local_slots = createSlotRangeArray(100, 199);
  ASM_StateMachine_Init(local_slots);
  ASSERT_TRUE(slots_tracker_check_availability(local_slots).is_some);
  freeSlotRangeArray(local_slots);
  return 0;
}

int testImportWorkflow() {
  draining_called = 0;

  RedisModuleSlotRangeArray* local_slots = createSlotRangeArray(5, 20);
  RedisModuleSlotRangeArray* import_slots = createSlotRangeArray(100, 199);

  ASM_StateMachine_Init(local_slots);
  ASSERT_TRUE(slots_tracker_check_availability(local_slots).is_some);
  ASSERT_FALSE(slots_tracker_check_availability(import_slots).is_some);

  ASM_StateMachine_StartImport(import_slots);
  ASSERT_FALSE(slots_tracker_check_availability(import_slots).is_some);
  ASSERT_TRUE(slots_tracker_check_availability(local_slots).is_some);

  ASM_StateMachine_CompleteImport(import_slots);
  ASSERT_TRUE(slots_tracker_check_availability(import_slots).is_some);
  ASSERT_TRUE(slots_tracker_check_availability(local_slots).is_some);

  freeSlotRangeArray(import_slots);
  freeSlotRangeArray(local_slots);
  return 0;
}

int testImportContinuousWorkflow() {
  draining_called = 0;

  RedisModuleSlotRangeArray* local_slots = createSlotRangeArray(5, 99);
  RedisModuleSlotRangeArray* import_slots = createSlotRangeArray(100, 199);
  RedisModuleSlotRangeArray* complete_slots = createSlotRangeArray(5, 199);
  ASM_StateMachine_Init(local_slots);
  ASSERT_TRUE(slots_tracker_check_availability(local_slots).is_some);
  ASSERT_FALSE(slots_tracker_check_availability(import_slots).is_some);
  ASSERT_FALSE(slots_tracker_check_availability(complete_slots).is_some);

  ASM_StateMachine_StartImport(import_slots);
  ASSERT_FALSE(slots_tracker_check_availability(import_slots).is_some);
  ASSERT_FALSE(slots_tracker_check_availability(complete_slots).is_some);
  ASSERT_TRUE(slots_tracker_check_availability(local_slots).is_some);

  ASM_StateMachine_CompleteImport(import_slots);
  ASSERT_TRUE(slots_tracker_check_availability(import_slots).is_some);
  ASSERT_TRUE(slots_tracker_check_availability(local_slots).is_some);
  ASSERT_TRUE(slots_tracker_check_availability(complete_slots).is_some);

  freeSlotRangeArray(import_slots);
  freeSlotRangeArray(local_slots);
  freeSlotRangeArray(complete_slots);
  return 0;
}

int testMigrationTrimmingWorkflow() {
  draining_called = 0;
  RedisModuleSlotRangeArray* local_slots = createSlotRangeArray(5, 199);
  RedisModuleSlotRangeArray* migration_slots = createSlotRangeArray(100, 199);
  RedisModuleSlotRangeArray* disjoint_slots = createSlotRangeArray(5, 99);

  ASM_StateMachine_Init(local_slots);
  ASSERT_TRUE(slots_tracker_check_availability(local_slots).is_some);
  ASSERT_TRUE(slots_tracker_check_availability(migration_slots).is_some);
  ASSERT_TRUE(slots_tracker_check_availability(disjoint_slots).is_some);
  // Start migration does nothing

  ASM_StateMachine_CompleteMigration(migration_slots);
  ASSERT_FALSE(slots_tracker_check_availability(migration_slots).is_some);
  ASSERT_FALSE(slots_tracker_check_availability(local_slots).is_some);
  ASSERT_TRUE(slots_tracker_check_availability(disjoint_slots).is_some);

  ASM_StateMachine_StartTrim(migration_slots, mock_draining_function);
  ASSERT_TRUE(draining_called);
  ASSERT_FALSE(slots_tracker_check_availability(migration_slots).is_some);
  ASSERT_FALSE(slots_tracker_check_availability(local_slots).is_some);
  ASSERT_TRUE(slots_tracker_check_availability(disjoint_slots).is_some);
  ASM_StateMachine_CompleteTrim(migration_slots);


  freeSlotRangeArray(migration_slots);
  freeSlotRangeArray(local_slots);
  freeSlotRangeArray(disjoint_slots);
  return 0;
}


TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testInitialization);
  TESTFUNC(testImportWorkflow);
  TESTFUNC(testImportContinuousWorkflow);
  TESTFUNC(testMigrationTrimmingWorkflow);
})
