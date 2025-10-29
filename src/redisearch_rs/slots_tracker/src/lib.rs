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
use slots_set::SlotsSet;

// ============================================================================
// Thread-unsafe wrapper for single-threaded access
// ============================================================================

/// A wrapper around `UnsafeCell` that implements `Sync` to allow static storage.
///
/// # Safety
///
/// This type is NOT thread-safe. The caller must ensure that access is single-threaded.
/// Using this from multiple threads will cause undefined behavior.
struct UnsafeSyncCell<T>(UnsafeCell<T>);

impl<T> UnsafeSyncCell<T> {
    const fn new(value: T) -> Self {
        UnsafeSyncCell(UnsafeCell::new(value))
    }

    /// Get a mutable pointer to the inner value.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - Only one thread accesses this at a time
    /// - No other references (mutable or immutable) exist while this reference is alive
    const fn get(&self) -> *mut T {
        self.0.get()
    }
}

// SAFETY: This is marked as Sync to allow use in static variables, but the caller
// MUST ensure single-threaded access. This is enforced by the C API contract.
unsafe impl<T> Sync for UnsafeSyncCell<T> {}

// ============================================================================
// Global Static Instances (Private)
// ============================================================================

/// Global instance #1: Local responsibility slots
///
/// Contains slot ranges that are owned by this Redis instance in the cluster topology.
static LOCAL_SLOTS: UnsafeSyncCell<SlotsSet> = UnsafeSyncCell::new(SlotsSet::new());

/// Global instance #2: Fully available non-owned slots
///
/// Contains slot ranges that are locally fully available but not owned by this instance.
static FULLY_AVAILABLE_SLOTS: UnsafeSyncCell<SlotsSet> = UnsafeSyncCell::new(SlotsSet::new());

/// Global instance #3: Partially available non-owned slots
///
/// Contains slot ranges that are partially available and not owned by this instance.
static PARTIALLY_AVAILABLE_SLOTS: UnsafeSyncCell<SlotsSet> = UnsafeSyncCell::new(SlotsSet::new());

/// Version counter for tracking changes to the slots configuration.
///
/// This counter is incremented whenever the slot configuration changes.
/// It can wrap around safely as we only care about equality checks, not ordering.
/// This is atomic so it can be safely read from any thread.
static VERSION: AtomicU32 = AtomicU32::new(0);

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
    let current = VERSION.load(Ordering::Relaxed);
    assert!(current <= MAX_VALID_VERSION, "Version counter out of valid range");
    let next = if current < MAX_VALID_VERSION {
        current + 1
    } else {
        0 // Wrap around to 0
    };
    VERSION.store(next, Ordering::Relaxed);
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

/// C-compatible error codes for FFI operations.
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SlotTrackerError {
    /// Operation completed successfully.
    Success = 0,
    /// Invalid slot range provided.
    InvalidRange = 1,
    /// Failed to acquire lock on the slots set.
    LockFailed = 2,
    /// Invalid instance ID provided.
    InvalidInstance = 3,
}

// ============================================================================
// Private Helper Functions
// ============================================================================

/// Converts a C SlotRangeArray pointer to a Rust slice.
///
/// Returns None if the pointer is null or num_ranges is negative.
///
/// # Safety
///
/// The caller must ensure the pointer is valid and points to a properly initialized
/// SlotRangeArray with at least `num_ranges` elements in the flexible array.
#[allow(clippy::missing_const_for_fn)] // Can't be const due to slice::from_raw_parts
unsafe fn parse_slot_ranges(ranges: *const SlotRangeArray) -> Option<&'static [SlotRange]> {
    if ranges.is_null() {
        return None;
    }

    // SAFETY: Caller guarantees valid pointer
    let ranges_ref = unsafe { &*ranges };
    
    if ranges_ref.num_ranges < 0 {
        return None;
    }

    if ranges_ref.num_ranges == 0 {
        return Some(&[]);
    }

    // SAFETY: Caller guarantees the flexible array has num_ranges elements
    let slice = unsafe {
        std::slice::from_raw_parts(
            ranges_ref.ranges.as_ptr(),
            ranges_ref.num_ranges as usize,
        )
    };
    
    Some(slice)
}

/// Gets mutable references to all three slot sets.
///
/// # Safety
///
/// The caller must ensure single-threaded access to the static instances.
unsafe fn get_all_sets() -> (&'static mut SlotsSet, &'static mut SlotsSet, &'static mut SlotsSet) {
    // SAFETY: Caller guarantees single-threaded access
    let local = unsafe { &mut *LOCAL_SLOTS.get() };
    // SAFETY: Caller guarantees single-threaded access
    let fully = unsafe { &mut *FULLY_AVAILABLE_SLOTS.get() };
    // SAFETY: Caller guarantees single-threaded access
    let partial = unsafe { &mut *PARTIALLY_AVAILABLE_SLOTS.get() };
    (local, fully, partial)
}

/// Gets mutable reference to the local slots set.
///
/// # Safety
///
/// The caller must ensure single-threaded access to the static instance.
unsafe fn get_local_slots() -> &'static mut SlotsSet {
    // SAFETY: Caller guarantees single-threaded access
    unsafe { &mut *LOCAL_SLOTS.get() }
}

/// Gets mutable reference to the fully available slots set.
///
/// # Safety
///
/// The caller must ensure single-threaded access to the static instance.
unsafe fn get_fully_available_slots() -> &'static mut SlotsSet {
    // SAFETY: Caller guarantees single-threaded access
    unsafe { &mut *FULLY_AVAILABLE_SLOTS.get() }
}

/// Gets mutable reference to the partially available slots set.
///
/// # Safety
///
/// The caller must ensure single-threaded access to the static instance.
unsafe fn get_partially_available_slots() -> &'static mut SlotsSet {
    // SAFETY: Caller guarantees single-threaded access
    unsafe { &mut *PARTIALLY_AVAILABLE_SLOTS.get() }
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
    let Some(ranges_slice) = (unsafe { parse_slot_ranges(ranges) }) else {
        return;
    };

    // SAFETY: Caller guarantees single-threaded access
    let (local_slots, fully_available, partially_available) = unsafe { get_all_sets() };

    // TODO: Implement in SlotsSet:
    // 1. Check if local_slots.equals(ranges_slice)
    // 2. If different:
    //    a. local_slots.set_from_ranges(ranges_slice)
    //    b. fully_available.remove_ranges(ranges_slice)
    //    c. partially_available.remove_ranges(ranges_slice)
    //    d. increment_version();
    
    // Placeholder: Always increment version for now
    let _ = (local_slots, fully_available, partially_available, ranges_slice);
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
    let Some(ranges_slice) = (unsafe { parse_slot_ranges(ranges) }) else {
        return;
    };

    // SAFETY: Caller guarantees single-threaded access
    let (local_slots, fully_available, partially_available) = unsafe { get_all_sets() };

    // TODO: Implement in SlotsSet:
    // 1. partially_available.set_from_ranges(ranges_slice)
    // 2. local_slots.remove_ranges(ranges_slice)
    // 3. fully_available.remove_ranges(ranges_slice)
    // 4. increment_version();
    
    // Placeholder: Always increment version for now
    let _ = (local_slots, fully_available, partially_available, ranges_slice);
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
    let Some(ranges_slice) = (unsafe { parse_slot_ranges(ranges) }) else {
        return;
    };

    // SAFETY: Caller guarantees single-threaded access
    let local_slots = unsafe { get_local_slots() };
    // SAFETY: Caller guarantees single-threaded access
    let fully_available = unsafe { get_fully_available_slots() };

    // TODO: Implement in SlotsSet:
    // 1. fully_available.set_from_ranges(ranges_slice)
    // 2. local_slots.remove_ranges(ranges_slice)
    // Note: Do NOT increment VERSION, do NOT remove from partially_available
    
    // Placeholder
    let _ = (local_slots, fully_available, ranges_slice);
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
    let Some(ranges_slice) = (unsafe { parse_slot_ranges(ranges) }) else {
        return;
    };

    // SAFETY: Caller guarantees single-threaded access
    let partially_available = unsafe { get_partially_available_slots() };

    // TODO: Implement in SlotsSet:
    // 1. partially_available.remove_ranges(ranges_slice)
    // Note: Do NOT increment VERSION, only remove from set 3
    
    // Placeholder
    let _ = (partially_available, ranges_slice);
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
    let Some(ranges_slice) = (unsafe { parse_slot_ranges(ranges) }) else {
        return false;
    };

    // SAFETY: Caller guarantees single-threaded access
    let fully_available = unsafe { get_fully_available_slots() };

    // TODO: Implement in SlotsSet:
    // 1. Return fully_available.has_overlap(ranges_slice)
    
    // Placeholder: return false for now
    let _ = (fully_available, ranges_slice);
    false
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
    let Some(ranges_slice) = (unsafe { parse_slot_ranges(ranges) }) else {
        return SLOTS_TRACKER_UNAVAILABLE;
    };

    // SAFETY: Caller guarantees single-threaded access
    let (local_slots, fully_available, partially_available) = unsafe { get_all_sets() };

    // TODO: Implement in SlotsSet:
    // Fast path: Check if sets 2 & 3 are empty
    // if fully_available.is_empty() && partially_available.is_empty() {
    //     if local_slots.equals(ranges_slice) {
    //         return VERSION.load(Ordering::Relaxed);
    //     }
    // }
    //
    // Full check:
    // 1. Check if local_slots + fully_available covers ranges_slice
    //    Use: local_slots.union_covers(fully_available, ranges_slice)
    // 2. If not covered: return SLOTS_TRACKER_UNAVAILABLE
    // 3. If covered:
    //    a. Check if local_slots + fully_available equals ranges_slice exactly
    //       AND partially_available.is_empty()
    //    b. If yes: return VERSION.load(Ordering::Relaxed)
    //    c. If no: return SLOTS_TRACKER_UNSTABLE_VERSION
    
    // Placeholder: return unavailable for now
    let _ = (local_slots, fully_available, partially_available, ranges_slice);
    SLOTS_TRACKER_UNAVAILABLE
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
    VERSION.load(Ordering::Relaxed)
}

// ============================================================================
// Placeholder Functions (to be removed or implemented)
// ============================================================================

// TODO: Implement C extern functions for:
// - Initializing/clearing a slots set instance
// - Adding slot ranges to an instance
// - Removing slot ranges from an instance
// - Checking if a slot is in an instance
// - Performing set operations between instances
// - Querying the state of an instance (e.g., get all ranges)

/*
/// Adds a slot range to the specified global slots set instance.
///
/// # Safety
///
/// This function is safe to call from C code. All parameters are passed by value.
/// The function validates inputs and returns appropriate error codes.
#[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_add_range(
    _instance_id: u8,
    _start: u16,
    _end: u16,
) -> SlotTrackerError {
    // TODO: Implement
    // - Validate instance_id (must be 0, 1, or 2)
    // - Validate range (start <= end, values in valid slot range 0-16383)
    // - Lock the appropriate global instance
    // - Add the range to the set
    // - Return appropriate error code
    SlotTrackerError::Success
}

/// Checks if a slot is contained in the specified global slots set instance.
///
/// # Safety
///
/// This function is safe to call from C code. All parameters are passed by value.
#[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_contains(_instance_id: u8, _slot: u16) -> bool {
    // TODO: Implement
    // - Validate instance_id
    // - Lock the appropriate global instance
    // - Check if the slot is in any of the ranges
    // - Return true/false (returns false on error)
    false
}

/// Clears all slot ranges from the specified global slots set instance.
///
/// # Safety
///
/// This function is safe to call from C code. All parameters are passed by value.
#[unsafe(no_mangle)]
pub extern "C" fn slots_tracker_clear(_instance_id: u8) -> SlotTrackerError {
    // TODO: Implement
    // - Validate instance_id
    // - Lock the appropriate global instance
    // - Clear all ranges
    // - Return appropriate error code
    SlotTrackerError::Success
}
*/

// TODO: Add more FFI functions as needed:
// - slots_tracker_remove_range
// - slots_tracker_union
// - slots_tracker_intersection
// - slots_tracker_difference
// - slots_tracker_get_ranges (return array of ranges to C)
