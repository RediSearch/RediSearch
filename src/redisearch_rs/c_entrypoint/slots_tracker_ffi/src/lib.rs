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
//! and performing set operations on them. It maintains a global instance of the slots tracker
//! that can be modified through the C API.
//!
//! **Thread Safety**: The global instance is designed to be accessed from a single thread only.
//! It does not use synchronization primitives like `Mutex`. If you need to access it from
//! multiple threads, you must provide your own synchronization at the C level.
//!
//! **Version Tracking**: Functions that may modify the tracker return the current version number
//! as a u32. These are currently wrapped in the C ASM API header
//! for atomic management on the C side.

use slots_tracker::{SlotRange, SlotRangeArray, SlotsTracker, Version};
use std::cell::RefCell;
use std::sync::OnceLock;
use std::thread::ThreadId;

/// FFI struct representing an optional SlotsTracker version.
/// This is used to return version information from the `slots_tracker_check_availability` function.
///
/// Expected use cases:
/// - `is_some == false`: No version (unavailable) - query should be rejected.
/// - `is_some == true`: Store the version number in `version`, to be compared with `slots_tracker_get_version` to detect changes.
#[repr(C)]
pub struct OptionSlotTrackerVersion {
    is_some: bool,
    version: u32,
}

// Conversion from Option<Version> to OptionSlotTrackerVersion
impl From<Option<Version>> for OptionSlotTrackerVersion {
    fn from(version: Option<Version>) -> Self {
        match version {
            Some(Version::Stable(v)) => Self {
                is_some: true,
                version: v.get(),
            },
            Some(Version::Unstable) => Self {
                is_some: true,
                version: 0,
            },
            None => Self {
                is_some: false,
                version: 0,
            },
        }
    }
}

// ============================================================================
// Global State
// ============================================================================

/// The thread ID that owns the tracker instance.
///
/// Set once when the first FFI function is called. All subsequent calls
/// must come from the same thread.
static OWNER_THREAD: OnceLock<ThreadId> = OnceLock::new();

// Thread-local slots tracker instance.
//
// Only accessible from the thread that first calls any FFI function.
thread_local! {
    static TRACKER: RefCell<SlotsTracker> = const { RefCell::new(SlotsTracker::new()) };
}

// ============================================================================
// Private Helper Functions
// ============================================================================

/// Ensures the current thread is the owner thread.
///
/// On first call, registers the current thread as the owner.
/// On subsequent calls, panics if called from a different thread.
///
/// # Panics
///
/// Panics if called from a thread other than the owner thread.
fn assert_owner_thread() {
    let current = std::thread::current().id();
    let owner = OWNER_THREAD.get_or_init(|| current);

    assert_eq!(
        *owner, current,
        "slots_tracker FFI functions called from wrong thread (owner: {:?}, current: {:?})",
        owner, current
    );
}

/// Gets a reference to the tracker and executes a function on it.
///
/// # Panics
///
/// Panics if called from a thread other than the owner thread.
fn with_tracker<F, R>(f: F) -> R
where
    F: FnOnce(&SlotsTracker) -> R,
{
    assert_owner_thread();
    TRACKER.with_borrow(f)
}

/// Gets a mutable reference to the tracker and executes a function on it.
///
/// # Panics
///
/// Panics if called from a thread other than the owner thread.
fn with_tracker_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut SlotsTracker) -> R,
{
    assert_owner_thread();
    TRACKER.with_borrow_mut(f)
}

/// Converts a C SlotRangeArray pointer to a core library SlotRange slice.
///
/// # Panics
///
/// Panics if the pointer is null or num_ranges is less than 0.
///
/// # Safety
///
/// The caller must ensure the pointer is valid and points to a properly initialized
/// SlotRangeArray with at least `num_ranges` elements in the flexible array.
unsafe fn parse_slot_ranges<'a>(ranges: *const SlotRangeArray) -> &'a [SlotRange] {
    debug_assert!(!ranges.is_null(), "SlotRangeArray pointer is null");

    // SAFETY: Caller guarantees valid pointer
    let ranges = unsafe { &*ranges };

    assert!(
        ranges.num_ranges >= 0,
        "num_ranges must be at least 0, got {}",
        ranges.num_ranges
    );

    // SAFETY: Caller guarantees the flexible array has num_ranges elements
    unsafe { std::slice::from_raw_parts(ranges.ranges.as_ptr(), ranges.num_ranges as usize) }
}

// ============================================================================
// Main API Functions
// ============================================================================

/// Sets the local slot ranges this shard is responsible for.
///
/// This function updates the "local slots" set to match the provided ranges.
/// If the ranges differ from the current configuration:
/// - Updates "local slots" to the new ranges
/// - Removes any overlapping slots from "fully available slots" and "partially available slots"
/// - Increments the version counter
///
/// If the ranges are identical to the current configuration, no changes are made.
///
/// Returns the current version after the operation.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn slots_tracker_set_local_slots(
    ranges: *const SlotRangeArray,
) -> u32 {
    // SAFETY: Caller guarantees valid pointer
    let ranges = unsafe { parse_slot_ranges(ranges) };

    with_tracker_mut(|tracker| {
        tracker.set_local_slots(ranges);

        let Version::Stable(version) = tracker.get_version() else {
            unreachable!("Tracker version should always be stable (from get_version)")
        };
        version.get()
    })
}

/// Marks the given slot ranges as partially available.
///
/// This function updates the "partially available slots" set by adding the provided ranges.
/// It also removes the given slots from "local slots" and "fully available slots", and
/// increments the version counter.
/// DO NOT call this function directly, use `ASM API` in the C header instead.
///
/// Returns the current version after the operation, used by `ASM API`
/// in the C header for atomic version management.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn slots_tracker_mark_partially_available_slots(
    ranges: *const SlotRangeArray,
) -> u32 {
    // SAFETY: Caller guarantees valid pointer
    let ranges = unsafe { parse_slot_ranges(ranges) };

    with_tracker_mut(|tracker| {
        tracker.mark_partially_available_slots(ranges);

        let Version::Stable(version) = tracker.get_version() else {
            unreachable!("Tracker version should always be stable (from get_version)")
        };
        version.get()
    })
}

/// Promotes slot ranges to local ownership.
///
/// This function adds the provided ranges to "local slots" and removes them from
/// "partially available slots". Does NOT modify "fully available slots" and does NOT
/// increment the version counter (the version was already bumped when slots became
/// partially available, and while partially available slots exist, `check_availability`
/// returns unstable/unavailable anyway).
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn slots_tracker_promote_to_local_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer
    let ranges = unsafe { parse_slot_ranges(ranges) };

    with_tracker_mut(|tracker| {
        let version_before = tracker.get_version();
        tracker.promote_to_local_slots(ranges);
        // Note: Version is NOT incremented here
        debug_assert_eq!(tracker.get_version(), version_before);
    });
}

/// Marks the given slot ranges as fully available non-owned.
///
/// This function updates the "fully available slots" set by adding the provided ranges.
/// It also removes the given slots from "local slots".
///
/// Note: This does NOT increment the version counter (slots availability is unchanged).
/// It also does NOT remove from "partially available slots".
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn slots_tracker_mark_fully_available_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer
    let ranges = unsafe { parse_slot_ranges(ranges) };

    with_tracker_mut(|tracker| {
        let version_before = tracker.get_version();
        tracker.mark_fully_available_slots(ranges);
        // Note: Version is NOT incremented here
        debug_assert_eq!(tracker.get_version(), version_before);
    });
}

/// Removes deleted slot ranges from the partially available slots.
///
/// This function removes the given slot ranges from "partially available slots" only.
/// It does NOT modify "local slots" or "fully available slots", and does NOT increment the version.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn slots_tracker_remove_deleted_slots(ranges: *const SlotRangeArray) {
    // SAFETY: Caller guarantees valid pointer
    let ranges = unsafe { parse_slot_ranges(ranges) };

    with_tracker_mut(|tracker| {
        let version_before = tracker.get_version();
        tracker.remove_deleted_slots(ranges);
        // Note: Version is NOT incremented here
        debug_assert_eq!(tracker.get_version(), version_before);
    });
}

/// Checks if there is any overlap between the given slot ranges and the fully available slots.
///
/// This function checks if any of the provided slot ranges overlap with "fully available slots".
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
    // SAFETY: Caller guarantees valid pointer
    let ranges = unsafe { parse_slot_ranges(ranges) };

    with_tracker(|tracker| tracker.has_fully_available_overlap(ranges))
}

/// Checks if all requested slots are available and returns version information.
///
/// Return values (via OptionSlotTrackerVersion):
/// - `is_some = false`: Required slots are not available. Query should be rejected.
/// - `is_some = true`: Slots available; Store the returned `version` and compare it (equality check) with the tracker's version.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// The `ranges` pointer must be valid and point to a properly initialized RedisModuleSlotRangeArray.
/// The ranges array must contain `num_ranges` valid elements.
/// All ranges must be sorted and have start <= end, with values in [0, 16383].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn slots_tracker_check_availability(
    ranges: *const SlotRangeArray,
) -> OptionSlotTrackerVersion {
    // SAFETY: Caller guarantees valid pointer
    let ranges = unsafe { parse_slot_ranges(ranges) };

    with_tracker(|tracker| tracker.check_availability(ranges).into())
}

// ============================================================================
// Testing Functions
// ============================================================================

/// Resets the tracker to its initial state.
///
/// This function is intended for testing purposes only. It resets the tracker
/// to a clean state with no slots configured and version reset to initial.
///
/// # Safety
///
/// This function must be called from the main thread only.
/// This function is intended for testing use only and should not be called
/// in production code.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn slots_tracker_reset_for_testing() {
    with_tracker_mut(|tracker| {
        *tracker = SlotsTracker::new();
    });
}
