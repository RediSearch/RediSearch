/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include <gtest/gtest.h>
#include <vector>
#include <algorithm>
#include "redismodule.h"
#include "slot_ranges.h"
#include "rmalloc.h"
#include "asm_state_machine.h"
#include "slots_tracker.h"

// Global flag to track if draining was called
static bool draining_called = false;

// Mock draining function for testing
static void mock_draining_function() {
    draining_called = true;
}

class ASMStateMachineTest : public ::testing::Test {
protected:

    // Helper function to create slot range arrays
    RedisModuleSlotRangeArray* createSlotRangeArray(const std::vector<std::pair<uint16_t, uint16_t>>& ranges) {
        size_t array_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * ranges.size();
        RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)rm_malloc(array_size);
        array->num_ranges = ranges.size();

        for (size_t i = 0; i < ranges.size(); i++) {
            array->ranges[i].start = ranges[i].first;
            array->ranges[i].end = ranges[i].second;
        }

        return array;
    }

    // Helper function to free slot range arrays
    void freeSlotRangeArray(RedisModuleSlotRangeArray* array) {
        if (array) {
            rm_free(array);
        }
    }
};

// Test basic import workflow
TEST_F(ASMStateMachineTest, TestImportWorkflow) {
    // Start import for slots 100-199
    auto *initial_slots = createSlotRangeArray({{0, 99}});
    slots_tracker_set_local_slots(initial_slots);
    auto* import_slots = createSlotRangeArray({{100, 199}});

    // Start import
    ASM_StateMachine_StartImport(import_slots);

    // Complete import
    ASM_StateMachine_CompleteImport(import_slots);

    freeSlotRangeArray(import_slots);
    freeSlotRangeArray(initial_slots);
}
/*
// Test basic migration workflow
TEST_F(ASMStateMachineTest, TestMigrationWorkflow) {
    // Complete migration for slots 200-299 (making them fully available)
    auto* migration_slots = createSlotRangeArray({{200, 299}});

    ASM_StateMachine_CompleteMigration(migration_slots);

    // Slots should be available after migration
    auto availability = checkAvailability({{200, 299}});
    EXPECT_TRUE(availability.is_some);

    freeSlotRangeArray(migration_slots);
}

// Test trim workflow with overlap
TEST_F(ASMStateMachineTest, TestTrimWorkflowWithOverlap) {
    // First, make some slots fully available
    auto* migration_slots = createSlotRangeArray({{300, 399}});
    ASM_StateMachine_CompleteMigration(migration_slots);

    // Check that overlap is detected
    EXPECT_TRUE(hasFullyAvailableOverlap({{350, 450}}));

    // Start trim with overlap - should trigger draining
    auto* trim_slots = createSlotRangeArray({{350, 450}});
    ASM_StateMachine_StartTrim(trim_slots, mock_draining_function);

    // Draining should have been called
    EXPECT_TRUE(draining_called);

    // Complete trim
    ASM_StateMachine_CompleteTrim(trim_slots);

    freeSlotRangeArray(migration_slots);
    freeSlotRangeArray(trim_slots);
}

// Test trim workflow without overlap
TEST_F(ASMStateMachineTest, TestTrimWorkflowWithoutOverlap) {
    // Start trim without any fully available slots
    auto* trim_slots = createSlotRangeArray({{500, 599}});

    // Check no overlap
    EXPECT_FALSE(hasFullyAvailableOverlap({{500, 599}}));

    ASM_StateMachine_StartTrim(trim_slots, mock_draining_function);

    // Draining should not have been called
    EXPECT_FALSE(draining_called);

    // Complete trim
    ASM_StateMachine_CompleteTrim(trim_slots);

    freeSlotRangeArray(trim_slots);
}

// Test complex import -> migration -> trim cycle
TEST_F(ASMStateMachineTest, TestComplexImportMigrationTrimCycle) {
    auto* slots = createSlotRangeArray({{600, 699}});

    // Import cycle
    ASM_StateMachine_StartImport(slots);
    ASM_StateMachine_CompleteImport(slots);

    // Slots should be local now
    auto after_import = checkAvailability({{600, 699}});
    EXPECT_TRUE(after_import.is_some);

    // Migration cycle (move to fully available)
    ASM_StateMachine_CompleteMigration(slots);

    // Should still be available
    auto after_migration = checkAvailability({{600, 699}});
    EXPECT_TRUE(after_migration.is_some);

    // Trim cycle
    ASM_StateMachine_StartTrim(slots, mock_draining_function);
    EXPECT_TRUE(draining_called); // Should trigger draining due to overlap

    ASM_StateMachine_CompleteTrim(slots);

    freeSlotRangeArray(slots);
}

// Test failed import scenario
TEST_F(ASMStateMachineTest, TestFailedImportScenario) {
    auto* import_slots = createSlotRangeArray({{700, 799}});

    // Start import
    ASM_StateMachine_StartImport(import_slots);

    // Simulate failed import by doing trim instead of complete import
    ASM_StateMachine_StartTrim(import_slots, mock_draining_function);
    ASM_StateMachine_CompleteTrim(import_slots);

    // Slots should not be available after failed import cleanup
    auto failed_availability = checkAvailability({{700, 799}});
    EXPECT_FALSE(failed_availability.is_some);

    freeSlotRangeArray(import_slots);
}

// Test overlapping slot ranges
TEST_F(ASMStateMachineTest, TestOverlappingSlotRanges) {
    // Set up fully available slots
    auto* migration_slots = createSlotRangeArray({{100, 300}});
    ASM_StateMachine_CompleteMigration(migration_slots);

    // Check that overlap is detected
    EXPECT_TRUE(hasFullyAvailableOverlap({{150, 250}}));
    EXPECT_TRUE(hasFullyAvailableOverlap({{100, 120}}));
    EXPECT_TRUE(hasFullyAvailableOverlap({{280, 300}}));

    // Start import for partially overlapping range
    auto* import_slots = createSlotRangeArray({{250, 400}});
    ASM_StateMachine_StartImport(import_slots);

    // The overlapping part (250-300) should no longer be fully available
    // but the non-overlapping part (100-249) should still be
    EXPECT_TRUE(hasFullyAvailableOverlap({{100, 249}}));
    EXPECT_FALSE(hasFullyAvailableOverlap({{250, 300}})); // Now partially available

    // Complete import
    ASM_StateMachine_CompleteImport(import_slots);

    // Now all ranges should be available
    auto all_availability = checkAvailability({{0, 99}, {250, 400}});
    EXPECT_TRUE(all_availability.is_some);

    freeSlotRangeArray(migration_slots);
    freeSlotRangeArray(import_slots);
}

// Test version evolution through operations
TEST_F(ASMStateMachineTest, TestVersionEvolution) {
    // Get initial version
    auto initial = checkAvailability({{0, 99}});
    uint32_t initial_version = initial.version;

    // Start import should increment version
    auto* import_slots = createSlotRangeArray({{800, 899}});
    ASM_StateMachine_StartImport(import_slots);

    auto after_start_import = checkAvailability({{0, 99}});
    EXPECT_GT(after_start_import.version, initial_version);

    // Complete import should not increment version
    ASM_StateMachine_CompleteImport(import_slots);
    auto after_complete_import = checkAvailability({{0, 99}});
    EXPECT_EQ(after_complete_import.version, after_start_import.version);

    freeSlotRangeArray(import_slots);
}

// Test empty slot ranges
TEST_F(ASMStateMachineTest, TestEmptySlotRanges) {
    auto* empty_slots = createSlotRangeArray({});

    // Operations with empty ranges should not crash
    ASM_StateMachine_StartImport(empty_slots);
    ASM_StateMachine_CompleteImport(empty_slots);
    ASM_StateMachine_CompleteMigration(empty_slots);
    ASM_StateMachine_StartTrim(empty_slots, mock_draining_function);
    ASM_StateMachine_CompleteTrim(empty_slots);

    // Draining should not be called for empty ranges
    EXPECT_FALSE(draining_called);

    freeSlotRangeArray(empty_slots);
}

// Test single slot ranges
TEST_F(ASMStateMachineTest, TestSingleSlotRanges) {
    auto* single_slot = createSlotRangeArray({{1000, 1000}});

    ASM_StateMachine_StartImport(single_slot);
    ASM_StateMachine_CompleteImport(single_slot);

    auto availability = checkAvailability({{1000, 1000}});
    EXPECT_TRUE(availability.is_some);

    freeSlotRangeArray(single_slot);
}

// Test maximum slot range
TEST_F(ASMStateMachineTest, TestMaximumSlotRange) {
    auto* max_range = createSlotRangeArray({{0, 16383}});

    ASM_StateMachine_StartImport(max_range);
    ASM_StateMachine_CompleteImport(max_range);

    auto availability = checkAvailability({{0, 16383}});
    EXPECT_TRUE(availability.is_some);

    freeSlotRangeArray(max_range);
}

// Test state consistency during concurrent-like operations
TEST_F(ASMStateMachineTest, TestStateConsistency) {
    auto* slots1 = createSlotRangeArray({{1100, 1199}});
    auto* slots2 = createSlotRangeArray({{1200, 1299}});

    // Start multiple imports
    ASM_StateMachine_StartImport(slots1);
    ASM_StateMachine_StartImport(slots2);

    // Complete them in different order
    ASM_StateMachine_CompleteImport(slots2);
    ASM_StateMachine_CompleteImport(slots1);

    // Both should be available
    auto avail1 = checkAvailability({{1100, 1199}});
    auto avail2 = checkAvailability({{1200, 1299}});
    EXPECT_TRUE(avail1.is_some);
    EXPECT_TRUE(avail2.is_some);

    freeSlotRangeArray(slots1);
    freeSlotRangeArray(slots2);
}*/
