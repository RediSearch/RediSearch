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

mod slot_set;

mod slots_tracker;
pub use slots_tracker::{SLOTS_TRACKER_UNAVAILABLE, SLOTS_TRACKER_UNSTABLE_VERSION, SlotsTracker};

// ============================================================================
// Global State Structure
// ============================================================================

/// Encapsulates all slot tracking state in a single structure.
///
/// Wraps the safe `SlotsTracker` implementation with unsafe access for the C FFI.
/// The atomic version counter mirrors the internal tracker's version for thread-safe reads.
///
/// # Safety
///
/// This type is NOT thread-safe for mutations. Access to the UnsafeCell must be
/// single-threaded. The caller must ensure that:
/// - Only one thread (the main thread) accesses the tracker for mutations
/// - No other references exist while mutable references are alive
///
/// The version field is atomic and can be safely read from any thread.
struct SlotsTrackerState {
    /// The safe slots tracker implementation wrapped in UnsafeCell for static access.
    tracker: UnsafeCell<SlotsTracker>,
    /// Atomic version counter that mirrors the tracker's internal version.
    /// This allows thread-safe reads of the version without accessing the tracker.
    version: AtomicU32,
}

// SAFETY: This is marked as Sync to allow use in static variables, but the caller
// MUST ensure single-threaded access to the UnsafeCell field. This is enforced
// by the C API contract. The AtomicU32 is inherently Sync.
unsafe impl Sync for SlotsTrackerState {}

impl SlotsTrackerState {
    const fn new() -> Self {
        Self {
            tracker: UnsafeCell::new(SlotsTracker::new()),
            version: AtomicU32::new(0),
        }
    }
}

// ============================================================================
// Global Static State Instance (Private)
// ============================================================================

/// Global slots tracker state.
///
/// Contains the safe SlotsTracker wrapped in UnsafeCell for static access.
///
/// # Safety
///
/// All mutation functions accessing this state are `unsafe extern "C"` and
/// documented to require single-threaded access. Violating this contract
/// (e.g., calling from multiple threads) is undefined behavior.
static STATE: SlotsTrackerState = SlotsTrackerState::new();

// ============================================================================
// Private Helper Functions
// ============================================================================

/// Gets mutable reference to the tracker.
///
/// # Safety
///
/// The caller must ensure single-threaded access to the static instance.
unsafe fn get_tracker() -> &'static mut SlotsTracker {
    // SAFETY: Caller guarantees single-threaded access
    unsafe { &mut *STATE.tracker.get() }
}

/// Syncs the atomic version counter with the tracker's internal version.
///
/// This should be called after any operation that modifies the tracker's version.
fn sync_version(tracker: &SlotsTracker) {
    STATE
        .version
        .store(tracker.get_version(), Ordering::Relaxed);
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
unsafe fn parse_slot_ranges<'a>(ranges: *const SlotRangeArray) -> &'a [SlotRange] {
    assert!(!ranges.is_null(), "SlotRangeArray pointer is null");

    // SAFETY: Caller guarantees valid pointer
    let ranges = unsafe { &*ranges };

    assert!(
        ranges.num_ranges >= 1,
        "num_ranges must be at least 1, got {}",
        ranges.num_ranges
    );

    // SAFETY: Caller guarantees the flexible array has num_ranges elements
    unsafe { std::slice::from_raw_parts(ranges.ranges.as_ptr(), ranges.num_ranges as usize) }
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
pub unsafe extern "C" fn slots_tracker_set_local_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let tracker = unsafe { get_tracker() };

    // Delegate to the safe implementation
    tracker.set_local_slots(ranges);

    sync_version(tracker);
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
pub unsafe extern "C" fn slots_tracker_set_partially_available_slots(
    ranges: *const SlotRangeArray,
) {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let tracker = unsafe { get_tracker() };

    // Delegate to the safe implementation
    tracker.set_partially_available_slots(ranges);

    // Sync the atomic version counter
    sync_version(tracker);
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
pub unsafe extern "C" fn slots_tracker_set_fully_available_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let tracker = unsafe { get_tracker() };

    // Delegate to the safe implementation
    // Note: This does NOT increment the version
    tracker.set_fully_available_slots(ranges);
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
pub unsafe extern "C" fn slots_tracker_remove_deleted_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let tracker = unsafe { get_tracker() };

    // Delegate to the safe implementation
    // Note: This does NOT increment the version
    tracker.remove_deleted_slots(ranges);
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
pub unsafe extern "C" fn slots_tracker_has_fully_available_overlap(
    ranges: *const SlotRangeArray,
) -> bool {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let tracker = unsafe { get_tracker() };

    // Delegate to the safe implementation
    tracker.has_fully_available_overlap(ranges)
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
pub unsafe extern "C" fn slots_tracker_check_availability(ranges: *const SlotRangeArray) -> u32 {
    // SAFETY: Caller guarantees valid pointer and main thread access
    let ranges = unsafe { parse_slot_ranges(ranges) };

    // SAFETY: Caller guarantees single-threaded access
    let tracker = unsafe { get_tracker() };

    // Delegate to the safe implementation
    tracker.check_availability(ranges)
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
