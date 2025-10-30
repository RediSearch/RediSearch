/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Slots tracking module for managing Redis cluster slot ranges.
//!
//! This module provides a C FFI interface for tracking slot ranges using pairs of u16 values
//! and performing set operations on them. It maintains three global instances that can
//! be modified through the C API.
//!
//! **Thread Safety**: The global instances are designed to be accessed from a single thread only.
//! They do not use synchronization primitives like `Mutex`. If you need to access them from
//! multiple threads, you must provide your own synchronization at the C level.
//!
//! The version counter is atomic and can be safely read from any thread to check if the
//! slots configuration has changed.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, Ordering};

mod slots_set;
use slots_set::{CoverageRelation, SlotsSet};

#[cfg(test)]
mod slots_set_tests;

// ============================================================================
// Global State Structure
// ============================================================================

/// Encapsulates all slot tracking state in a single structure.
///
/// This groups the three slot sets and version counter together for better
/// organization while maintaining the same zero-cost abstraction as individual
/// statics.
///
/// # Safety
///
/// This type is NOT thread-safe. Access to the UnsafeCell fields must be
/// single-threaded. The caller must ensure that:
/// - Only one thread accesses the slot sets at a time
/// - No other references exist while mutable references are alive
///
/// The version field is atomic and can be safely read from any thread.
struct SlotsTrackerState {
    /// Local responsibility slots - owned by this Redis instance in the cluster topology.
    local: UnsafeCell<SlotsSet>,
    /// Fully available non-owned slots - locally available but not owned by this instance.
    fully_available: UnsafeCell<SlotsSet>,
    /// Partially available non-owned slots - partially available and not owned by this instance.
    partially_available: UnsafeCell<SlotsSet>,
    /// Version counter for tracking changes to the slots configuration.
    /// Incremented whenever the slot configuration changes. Wraps around safely.
    /// Atomic so it can be safely read from any thread.
    version: AtomicU32,
}

// SAFETY: This is marked as Sync to allow use in static variables, but the caller
// MUST ensure single-threaded access to the UnsafeCell fields. This is enforced
// by the C API contract. The AtomicU32 is inherently Sync.
unsafe impl Sync for SlotsTrackerState {}

impl SlotsTrackerState {
    const fn new() -> Self {
        Self {
            local: UnsafeCell::new(SlotsSet::new()),
            fully_available: UnsafeCell::new(SlotsSet::new()),
            partially_available: UnsafeCell::new(SlotsSet::new()),
            version: AtomicU32::new(0),
        }
    }
}

// ============================================================================
// Global Static Instance (Private)
// ============================================================================

/// Global slots tracker state.
///
/// Contains all three slot sets and the version counter in a single structure
/// for better organization and encapsulation.
///
/// # Safety
///
/// All mutation functions accessing this state are `unsafe extern "C"` and
/// documented to require single-threaded access. Violating this contract
/// (e.g., calling from multiple threads) is undefined behavior.
static STATE: SlotsTrackerState = SlotsTrackerState::new();

/// Reserved version value indicating unstable/partial availability.
///
/// This value will never equal a real version counter, so comparisons will always fail.
/// Used when slots are available but partially or the coverage doesn't exactly match.
pub const SLOTS_TRACKER_UNSTABLE_VERSION: u32 = u32::MAX;

/// Reserved version value indicating slots are not available.
///
/// This value indicates that the query cannot proceed because required slots are not available.
pub const SLOTS_TRACKER_UNAVAILABLE: u32 = u32::MAX - 1;

/// Maximum valid version value before wrapping to 0.
const MAX_VALID_VERSION: u32 = u32::MAX - 2;

/// Increments the version counter, skipping reserved values.
///
/// If the current version is `MAX_VALID_VERSION` (u32::MAX - 2), wraps to 0
/// instead of incrementing to reserved values (u32::MAX - 1 or u32::MAX).
///
/// # Safety
///
/// This function assumes single-threaded access (main thread only).
fn increment_version() {
    let current = STATE.version.load(Ordering::Relaxed);
    debug_assert!(
        current <= MAX_VALID_VERSION,
        "Version counter out of valid range"
    );
    let next = if current < MAX_VALID_VERSION {
        current + 1
    } else {
        0 // Wrap around to 0
    };
    STATE.version.store(next, Ordering::Relaxed);
}

// ============================================================================
// C FFI Interface - Public API
// ============================================================================

/// C-compatible slot range structure matching RedisModuleSlotRange.
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
}

/// C-compatible slot range array structure matching RedisModuleSlotRangeArray.
///
/// This is a variable-length structure with a flexible array member.
#[repr(C)]
pub struct SlotRangeArray {
    pub num_ranges: i32,
    pub ranges: [SlotRange; 0], // Flexible array member
}

// ============================================================================
// Private Helper Functions
// ============================================================================

/// Converts a C SlotRangeArray pointer to a Rust slice.
///
/// # Panics
///
/// Panics if the pointer is null or num_ranges is less than 1.
///
/// # Safety
///
/// The caller must ensure the pointer is valid and points to a properly initialized
/// SlotRangeArray with at least `num_ranges` elements in the flexible array.
unsafe fn parse_slot_ranges(ranges: *const SlotRangeArray) -> &'static [SlotRange] {
    assert!(!ranges.is_null(), "SlotRangeArray pointer is null");

    // SAFETY: Caller guarantees valid pointer
    let ranges_ref = unsafe { &*ranges };

    assert!(
        ranges_ref.num_ranges >= 1,
        "num_ranges must be at least 1, got {}",
        ranges_ref.num_ranges
    );

    // SAFETY: Caller guarantees the flexible array has num_ranges elements
    unsafe {
        std::slice::from_raw_parts(ranges_ref.ranges.as_ptr(), ranges_ref.num_ranges as usize)
    }
}

/// Gets mutable reference to the local slots set.
///
/// # Safety
///
/// The caller must ensure single-threaded access to the static instance.
unsafe fn get_local_slots() -> &'static mut SlotsSet {
    // SAFETY: Caller guarantees single-threaded access
    unsafe { &mut *STATE.local.get() }
}

/// Gets mutable reference to the fully available slots set.
///
/// # Safety
///
/// The caller must ensure single-threaded access to the static instance.
unsafe fn get_fully_available_slots() -> &'static mut SlotsSet {
    // SAFETY: Caller guarantees single-threaded access
    unsafe { &mut *STATE.fully_available.get() }
}

/// Gets mutable reference to the partially available slots set.
///
/// # Safety
///
/// The caller must ensure single-threaded access to the static instance.
unsafe fn get_partially_available_slots() -> &'static mut SlotsSet {
    // SAFETY: Caller guarantees single-threaded access
    unsafe { &mut *STATE.partially_available.get() }
}

/// Gets mutable references to all three slot sets.
///
/// # Safety
///
/// The caller must ensure single-threaded access to the static instances.
unsafe fn get_all_sets() -> (
    &'static mut SlotsSet,
    &'static mut SlotsSet,
    &'static mut SlotsSet,
) {
    // SAFETY: Caller guarantees single-threaded access
    let local = unsafe { get_local_slots() };
    // SAFETY: Caller guarantees single-threaded access
    let fully = unsafe { get_fully_available_slots() };
    // SAFETY: Caller guarantees single-threaded access
    let partial = unsafe { get_partially_available_slots() };
    (local, fully, partial)
}

// ============================================================================
// Main API Functions
// ============================================================================

/// Sets the local responsibility slot ranges.
///
/// This function updates the LOCAL_SLOTS set to match the provided ranges.
/// If the ranges differ from the current configuration:
/// - Updates LOCAL_SLOTS to the new ranges
/// - Removes any overlapping slots from FULLY_AVAILABLE_SLOTS and PARTIALLY_AVAILABLE_SLOTS
/// - Increments the VERSION counter
///
/// If the ranges are identical to the current configuration, no changes are made.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)] // Function is marked unsafe via #[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_set_local_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges_slice = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let (local_slots, fully_available, partially_available) = unsafe { get_all_sets() };

    // Check if the ranges are already equal (no change needed)
    if local_slots == ranges_slice {
        return;
    }

    // Update local slots and remove from other sets
    local_slots.set_from_ranges(ranges_slice);
    fully_available.remove_ranges(ranges_slice);
    partially_available.remove_ranges(ranges_slice);
    increment_version();
}

/// Sets the partially available slot ranges.
///
/// This function updates the PARTIALLY_AVAILABLE_SLOTS set to match the provided ranges.
/// It also removes the given slots from LOCAL_SLOTS and FULLY_AVAILABLE_SLOTS, and
/// increments the VERSION counter.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)] // Function is marked unsafe via #[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_set_partially_available_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges_slice = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let (local_slots, fully_available, partially_available) = unsafe { get_all_sets() };

    // Update partially available slots and remove from other sets
    partially_available.add_ranges(ranges_slice);
    local_slots.remove_ranges(ranges_slice);
    fully_available.remove_ranges(ranges_slice);
    increment_version();
}

/// Sets the fully available non-owned slot ranges.
///
/// This function updates the FULLY_AVAILABLE_SLOTS set to match the provided ranges.
/// It also removes the given slots from LOCAL_SLOTS.
///
/// Note: This does NOT increment the VERSION counter, as slots are moving from owned
/// to not-owned with no data changes. It also does NOT remove from PARTIALLY_AVAILABLE_SLOTS.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)] // Function is marked unsafe via #[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_set_fully_available_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges_slice = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let local_slots = unsafe { get_local_slots() };
    // SAFETY: Caller guarantees single-threaded access
    let fully_available = unsafe { get_fully_available_slots() };

    // Update fully available slots and remove from local slots
    // Note: Do NOT increment VERSION, do NOT remove from partially_available
    fully_available.add_ranges(ranges_slice);
    local_slots.remove_ranges(ranges_slice);
}

/// Removes deleted slot ranges from the partially available slots.
///
/// This function removes the given slot ranges from PARTIALLY_AVAILABLE_SLOTS (set 3).
/// These slots are expected to be found in set 3 only, as they represent slots that
/// were just deleted.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)] // Function is marked unsafe via #[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_remove_deleted_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges_slice = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let partially_available = unsafe { get_partially_available_slots() };

    // Remove deleted slots from partially available set only
    // Note: Do NOT increment VERSION, only remove from set 3
    partially_available.remove_ranges(ranges_slice);
}

/// Checks if there is any overlap between the given slot ranges and the fully available slots.
///
/// This function checks if any of the provided slot ranges overlap with FULLY_AVAILABLE_SLOTS (set 2).
/// Returns true if there is at least one overlapping slot, false otherwise.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)] // Function is marked unsafe via #[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_has_fully_available_overlap(ranges: *const SlotRangeArray) -> bool {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges_slice = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let fully_available = unsafe { get_fully_available_slots() };

    // Check if any of the ranges overlap with fully available slots
    fully_available.has_overlap(ranges_slice)
}

/// Checks if all requested slots are available and returns version information.
///
/// This function performs an optimized availability check:
///
/// **Fast path (common case)**: If sets 2 & 3 are empty and input exactly matches set 1,
/// returns the current version immediately.
///
/// **Full check**: If sets 2 or 3 are not empty:
/// - Checks if sets 1+2 cover all input slots
/// - If not covered: returns `SLOTS_TRACKER_UNAVAILABLE`
/// - If covered exactly AND set 3 is empty: returns current version
/// - If covered but not exactly OR set 3 is not empty: returns `SLOTS_TRACKER_UNSTABLE_VERSION`
///
/// Return values:
/// - Current version (0..u32::MAX-2): Slots match exactly and are stable
/// - `SLOTS_TRACKER_UNSTABLE_VERSION` (u32::MAX): Slots available but partial/inexact match
/// - `SLOTS_TRACKER_UNAVAILABLE` (u32::MAX-1): Required slots are not available
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)] // Function is marked unsafe via #[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_check_availability(ranges: *const SlotRangeArray) -> u32 {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges_slice = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let (local_slots, fully_available, partially_available) = unsafe { get_all_sets() };

    // Fast path: If sets 2 & 3 are empty and input exactly matches set 1
    if fully_available.is_empty() && partially_available.is_empty() && local_slots == ranges_slice {
        return STATE.version.load(Ordering::Relaxed);
    }

    // Full check: Use union_relation to check coverage
    match local_slots.union_relation(fully_available, ranges_slice) {
        CoverageRelation::Equals if partially_available.is_empty() => {
            // Exact match and no partial slots
            STATE.version.load(Ordering::Relaxed)
        }
        CoverageRelation::Equals | CoverageRelation::Covers => {
            // Covered but not exact, or has partial slots
            SLOTS_TRACKER_UNSTABLE_VERSION
        }
        CoverageRelation::NoMatch => {
            // Not all slots are available
            SLOTS_TRACKER_UNAVAILABLE
        }
    }
}

/// Returns the current version of the slots configuration.
///
/// This function can be called from any thread to check if the configuration has changed.
/// The version counter wraps around, so only equality checks are meaningful.
///
/// # Safety
///
/// This function is safe to call from any thread.
#[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_get_version() -> u32 {
    STATE.version.load(Ordering::Relaxed)
}
