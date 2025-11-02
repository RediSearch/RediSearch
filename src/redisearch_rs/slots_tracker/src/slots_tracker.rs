/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe implementation of the slots tracker state.
//!
//! This module provides a safe, non-thread-safe implementation of the slots tracker.
//! All methods take `&mut self`, making the borrowing rules enforce single-threaded access.

use crate::SlotRange;
use crate::slot_set::{CoverageRelation, SlotSet};

/// Reserved version value indicating slots are not available.
///
/// This value indicates that the query cannot proceed because required slots are not available.
pub const SLOTS_TRACKER_UNAVAILABLE: u32 = u32::MAX;

/// Reserved version value indicating unstable/partial availability.
///
/// This value will never equal a real version counter, so comparisons will always fail.
const SLOTS_TRACKER_UNSTABLE_VERSION: u32 = u32::MAX - 1;

/// Maximum valid version value before wrapping to 0.
const MAX_VALID_VERSION: u32 = u32::MAX - 2;

/// Safe slots tracker implementation.
///
/// This structure encapsulates all slot tracking state and provides safe methods
/// for manipulating the three slot sets and version counter.
///
/// All methods take `&mut self`, ensuring compile-time enforcement of exclusive access.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SlotsTracker {
    /// Local responsibility slots - owned by this Redis instance in the cluster topology.
    local: SlotSet,
    /// Fully available non-owned slots - locally available but not owned by this instance.
    fully_available: SlotSet,
    /// Partially available non-owned slots - partially available and not owned by this instance.
    partially_available: SlotSet,
    /// Version counter for tracking changes to the slots configuration.
    /// Incremented whenever the slot configuration changes. Wraps around safely.
    version: u32,
}

impl SlotsTracker {
    /// Creates a new SlotsTracker with empty slot sets and version 0.
    pub const fn new() -> Self {
        Self {
            local: SlotSet::new(),
            fully_available: SlotSet::new(),
            partially_available: SlotSet::new(),
            version: 0,
        }
    }

    /// Gets the current version counter value.
    pub const fn get_version(&self) -> u32 {
        self.version
    }

    /// Increments the version counter, skipping reserved values.
    ///
    /// If the current version is `MAX_VALID_VERSION` (u32::MAX - 2), wraps to 0
    /// instead of incrementing to reserved values (u32::MAX - 1 or u32::MAX).
    fn increment_version(&mut self) {
        debug_assert!(self.version <= MAX_VALID_VERSION);
        self.version = if self.version < MAX_VALID_VERSION {
            self.version + 1
        } else {
            0 // Wrap around to 0
        };
    }

    /// Sets the local responsibility slot ranges.
    ///
    /// This function replaces the `local` set with the provided ranges.
    /// It also removes the given slots from `fully_available` and `partially_available`.
    /// If the new ranges are identical to the current local slots, no change occurs and version
    /// is not incremented.
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges. Must be sorted and normalized (no overlaps, no adjacent ranges).
    pub fn set_local_slots(&mut self, ranges: &[SlotRange]) {
        // Check if the ranges are already equal (no change needed)
        if self.local == ranges {
            return;
        }

        // Update local slots and remove from other sets
        self.local.set_from_ranges(ranges);
        self.fully_available.remove_ranges(ranges);
        self.partially_available.remove_ranges(ranges);
        self.increment_version();
    }

    /// Sets the partially available slot ranges.
    ///
    /// This function updates the `partially_available` set by adding the provided ranges.
    /// It also removes the given slots from `local` and `fully_available`, and
    /// increments the `version` counter (fully available or not available slots are now partially available).
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges. Must be sorted and normalized (no overlaps, no adjacent ranges).
    pub fn set_partially_available_slots(&mut self, ranges: &[SlotRange]) {
        // Update partially available slots and remove from other sets
        self.partially_available.add_ranges(ranges);
        self.local.remove_ranges(ranges);
        self.fully_available.remove_ranges(ranges);
        self.increment_version();
    }

    /// Promotes slot ranges to local ownership.
    ///
    /// This function adds the provided ranges to `local` and removes them from `partially_available`.
    /// Does NOT modify `fully_available` and does NOT increment the version counter
    /// (the version was already bumped when slots became partially available, and while partially
    /// available slots exist, `check_availability` returns unstable/unavailable anyway).
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges. Must be sorted and normalized (no overlaps, no adjacent ranges).
    pub fn promote_to_local_slots(&mut self, ranges: &[SlotRange]) {
        self.local.add_ranges(ranges);
        self.partially_available.remove_ranges(ranges);
    }

    /// Sets the fully available non-owned slot ranges.
    ///
    /// This function updates the `fully_available` set by adding the provided ranges.
    /// It removes the given slots from `local`.
    /// Version is NOT incremented by this operation (slots availability is unchanged).
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges. Must be sorted and normalized (no overlaps, no adjacent ranges).
    pub fn set_fully_available_slots(&mut self, ranges: &[SlotRange]) {
        self.fully_available.add_ranges(ranges);
        self.local.remove_ranges(ranges);
    }

    /// Removes deleted slots from the partially available set.
    ///
    /// This function removes the given slot ranges from `partially_available` only.
    /// It does NOT modify `local` or `fully_available`, and does NOT increment the version.
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges to remove. Must be sorted and normalized.
    pub fn remove_deleted_slots(&mut self, ranges: &[SlotRange]) {
        // Remove deleted slots from partially available set only
        // Note: Do NOT increment `version`, only remove from set 3
        self.partially_available.remove_ranges(ranges);
    }

    /// Checks if there is any overlap between the given slot ranges and the fully available slots.
    ///
    /// This function checks if any of the provided slot ranges overlap with `fully_available` (set 2).
    /// Returns true if there is at least one overlapping slot, false otherwise.
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges to check. Must be sorted and normalized.
    ///
    /// # Returns
    ///
    /// `true` if any overlap exists, `false` otherwise.
    pub fn has_fully_available_overlap(&self, ranges: &[SlotRange]) -> bool {
        self.fully_available.has_overlap(ranges)
    }

    /// Checks if all requested slots are available and returns version information.
    ///
    /// This function performs an optimized availability check:
    /// - If sets 2 & 3 are empty and the input exactly matches set 1: returns current version (stable)
    /// - Uses `union_relation` to check if ``local` ∪ `fully_available`` covers the query
    /// - Returns special version values for partial or no coverage
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges to check. Must be sorted and normalized.
    ///
    /// # Returns
    ///
    /// - Current version (0..u32::MAX-2): Slots match exactly and are stable
    /// - `SLOTS_TRACKER_UNAVAILABLE` (u32::MAX): Required slots are not available
    /// - `SLOTS_TRACKER_UNSTABLE_VERSION` (u32::MAX-1): Slots available but partial/inexact match
    pub fn check_availability(&self, ranges: &[SlotRange]) -> u32 {
        // Fast path: If sets 2 & 3 are empty and input exactly matches set 1
        if self.fully_available.is_empty()
            && self.partially_available.is_empty()
            && self.local == ranges
        {
            return self.version;
        }

        // Full check: Use union_relation to check coverage
        match self.local.union_relation(&self.fully_available, ranges) {
            CoverageRelation::Equals if self.partially_available.is_empty() => {
                // Exact match and no partial slots
                self.version
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot_ranges_slice(ranges: &[(u16, u16)]) -> &[SlotRange] {
        unsafe { std::slice::from_raw_parts(ranges.as_ptr() as *const SlotRange, ranges.len()) }
    }

    impl<const N1: usize, const N2: usize, const N3: usize>
        PartialEq<(
            [(u16, u16); N1],
            [(u16, u16); N2],
            [(u16, u16); N3],
            Option<u32>,
        )> for SlotsTracker
    {
        fn eq(
            &self,
            other: &(
                [(u16, u16); N1],
                [(u16, u16); N2],
                [(u16, u16); N3],
                Option<u32>,
            ),
        ) -> bool {
            let other = (
                slot_ranges_slice(other.0.as_slice()),
                slot_ranges_slice(other.1.as_slice()),
                slot_ranges_slice(other.2.as_slice()),
                other.3,
            );
            self.local == other.0
                && self.fully_available == other.1
                && self.partially_available == other.2
                && match other.3 {
                    Some(v) => self.version == v,
                    None => true,
                }
        }
    }

    #[test]
    fn test_new_tracker_has_version_zero() {
        let tracker = SlotsTracker::new();
        assert_eq!(tracker.get_version(), 0);
    }

    #[test]
    fn test_set_local_slots_increments_version() {
        let mut tracker = SlotsTracker::new();
        let initial_version = tracker.get_version();

        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(tracker, ([(0, 100)], [], [], Some(initial_version + 1)));
    }

    #[test]
    fn test_set_same_local_slots_does_not_increment_version() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v1)));

        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]); // no change
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v1)));
    }

    #[test]
    fn test_remove_deleted_slots_does_not_increment_version() {
        let mut tracker = SlotsTracker::new();
        let v1 = tracker.get_version();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v1 + 1)));

        tracker.set_partially_available_slots(&[SlotRange {
            start: 200,
            end: 300,
        }]);
        assert_eq!(tracker, ([(0, 100)], [], [(200, 300)], Some(v1 + 2)));

        tracker.remove_deleted_slots(&[SlotRange {
            start: 250,
            end: 280,
        }]);
        assert_eq!(
            tracker,
            ([(0, 100)], [], [(200, 249), (281, 300)], Some(v1 + 2))
        );
    }

    #[test]
    fn test_check_availability_returns_version_for_exact_match() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v1)));

        let result = tracker.check_availability(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(result, v1);

        tracker.set_fully_available_slots(&[SlotRange { start: 0, end: 50 }]);
        assert_eq!(tracker, ([(51, 100)], [(0, 50)], [], Some(v1)));

        let result2 = tracker.check_availability(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(result2, v1);
    }

    #[test]
    fn test_check_availability_returns_unavailable_for_missing_slots() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v1)));

        let result = tracker.check_availability(&[SlotRange {
            start: 200,
            end: 300,
        }]);
        assert_eq!(result, SLOTS_TRACKER_UNAVAILABLE);
    }

    #[test]
    fn test_has_fully_available_overlap() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 200 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 200)], [], [], Some(v1)));

        tracker.set_fully_available_slots(&[SlotRange {
            start: 50,
            end: 150,
        }]);
        assert_eq!(tracker, ([(0, 49), (151, 200)], [(50, 150)], [], Some(v1)));

        assert!(tracker.has_fully_available_overlap(&[SlotRange {
            start: 100,
            end: 200
        }]));
        assert!(!tracker.has_fully_available_overlap(&[SlotRange {
            start: 300,
            end: 400
        }]));
    }

    #[test]
    fn test_version_wraps_around() {
        let mut tracker = SlotsTracker::new();
        tracker.version = MAX_VALID_VERSION;

        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(tracker, ([(0, 100)], [], [], Some(0)));
    }

    #[test]
    fn test_partially_available_increments_version() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 50)], [], [], Some(v1)));
        tracker.set_partially_available_slots(&[SlotRange { start: 60, end: 70 }]);
        assert_eq!(tracker, ([(0, 50)], [], [(60, 70)], Some(v1 + 1)));
    }

    #[test]
    fn test_fully_available_does_not_increment_version() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v1)));
        tracker.set_fully_available_slots(&[SlotRange { start: 10, end: 20 }]);
        assert_eq!(tracker, ([(0, 9), (21, 100)], [(10, 20)], [], Some(v1)));
    }

    #[test]
    fn test_check_availability_unstable_due_to_fully_available_extra() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 50)], [], [], Some(v1)));
        tracker.set_fully_available_slots(&[SlotRange {
            start: 100,
            end: 120,
        }]);
        assert_eq!(tracker, ([(0, 50)], [(100, 120)], [], Some(v1)));

        let res = tracker.check_availability(&[SlotRange { start: 0, end: 50 }]);
        // Implementation marks as UNSTABLE since union covers exact local but extra disjoint slots exist.
        assert_eq!(
            res, SLOTS_TRACKER_UNSTABLE_VERSION,
            "Extra disjoint fully-available slots make local-only query unstable"
        );
        let res2 = tracker.check_availability(&[SlotRange { start: 0, end: 120 }]);
        assert_eq!(
            res2, SLOTS_TRACKER_UNAVAILABLE,
            "Query spanning gap with uncovered range should be unavailable"
        );
    }

    #[test]
    fn test_check_availability_unavailable_when_not_covered() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 10 }]);
        assert_eq!(tracker, ([(0, 10)], [], [], None));
        let res = tracker.check_availability(&[SlotRange { start: 0, end: 20 }]);
        assert_eq!(res, SLOTS_TRACKER_UNAVAILABLE);
    }

    #[test]
    fn test_partially_available_makes_unstable() {
        let mut tracker = SlotsTracker::new();
        let v0 = tracker.get_version();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v0 + 1)));
        tracker.set_partially_available_slots(&[SlotRange {
            start: 150,
            end: 160,
        }]);
        assert_eq!(tracker, ([(0, 100)], [], [(150, 160)], Some(v0 + 2)));

        let res = tracker.check_availability(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(
            res, SLOTS_TRACKER_UNSTABLE_VERSION,
            "Exact local query becomes unstable when partial slots exist"
        );
        let res2 = tracker.check_availability(&[SlotRange { start: 0, end: 160 }]);
        assert_eq!(
            res2, SLOTS_TRACKER_UNAVAILABLE,
            "Query including partial-only slots should be unavailable"
        );
    }

    #[test]
    fn test_remove_deleted_slots_only_affects_partially_available() {
        let mut tracker = SlotsTracker::new();
        tracker.set_partially_available_slots(&[SlotRange {
            start: 200,
            end: 210,
        }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([], [], [(200, 210)], Some(v1)));
        tracker.remove_deleted_slots(&[SlotRange {
            start: 205,
            end: 207,
        }]);
        assert_eq!(tracker, ([], [], [(200, 204), (208, 210)], Some(v1)));

        // Querying partially-available slots (not covered by local/fully) should be UNAVAILABLE
        let res = tracker.check_availability(&[SlotRange {
            start: 200,
            end: 210,
        }]);
        assert_eq!(res, SLOTS_TRACKER_UNAVAILABLE);
    }

    #[test]
    fn test_sequential_version_changes() {
        let mut tracker = SlotsTracker::new();
        assert_eq!(tracker.get_version(), 0);
        assert!(tracker.local.is_empty());
        assert!(tracker.fully_available.is_empty());
        assert!(tracker.partially_available.is_empty());

        tracker.set_local_slots(&[SlotRange { start: 0, end: 10 }]); // v1
        assert_eq!(tracker, ([(0, 10)], [], [], Some(1)));

        tracker.set_partially_available_slots(&[SlotRange { start: 20, end: 30 }]); // v2
        assert_eq!(tracker, ([(0, 10)], [], [(20, 30)], Some(2)));

        tracker.set_fully_available_slots(&[SlotRange { start: 5, end: 6 }]); // still v2
        assert_eq!(tracker, ([(0, 4), (7, 10)], [(5, 6)], [(20, 30)], Some(2)));

        tracker.set_local_slots(&[SlotRange { start: 0, end: 5 }]); // v3 (changed local)
        assert_eq!(tracker, ([(0, 5)], [(6, 6)], [(20, 30)], Some(3)));
    }

    #[test]
    fn test_sets_content_after_operations() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 10 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 10)], [], [], Some(v1)));
        tracker.set_fully_available_slots(&[SlotRange { start: 0, end: 5 }]);
        assert_eq!(tracker, ([(6, 10)], [(0, 5)], [], Some(v1)));
        tracker.set_partially_available_slots(&[SlotRange { start: 50, end: 55 }]);
        assert_eq!(tracker, ([(6, 10)], [(0, 5)], [(50, 55)], Some(v1 + 1)));
    }

    #[test]
    fn test_mixed_local_and_partial_query_unavailable() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 10 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 10)], [], [], Some(v1)));
        tracker.set_partially_available_slots(&[SlotRange { start: 20, end: 25 }]);
        assert_eq!(tracker, ([(0, 10)], [], [(20, 25)], Some(v1 + 1)));

        let res = tracker.check_availability(&[SlotRange { start: 0, end: 25 }]);
        assert_eq!(
            res, SLOTS_TRACKER_UNAVAILABLE,
            "Query including partial-only slots should be unavailable"
        );
    }

    #[test]
    fn test_promote_to_local_slots_basic() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 50)], [], [], Some(v1)));

        tracker.set_partially_available_slots(&[SlotRange {
            start: 100,
            end: 150,
        }]);
        assert_eq!(tracker, ([(0, 50)], [], [(100, 150)], Some(v1 + 1)));
        assert_eq!(
            tracker.check_availability(&[SlotRange {
                start: 0,
                end: 50
            }, SlotRange {
                start: 100,
                end: 150
            }]),
            SLOTS_TRACKER_UNAVAILABLE,
            "Query including partial-only slots should be unavailable"
        );

        tracker.promote_to_local_slots(&[SlotRange {
            start: 100,
            end: 150,
        }]);
        assert_eq!(
            tracker,
            ([(0, 50), (100, 150)], [], [], Some(v1 + 1)),
            "Slots promoted to local, removed from partial, version unchanged"
        );
        assert_eq!(
            tracker.check_availability(&[SlotRange {
                start: 0,
                end: 50
            }, SlotRange {
                start: 100,
                end: 150
            }]),
            v1 + 1,
            "Query now fully covered by local slots"
        );
    }

    #[test]
    fn test_promote_to_local_slots_does_not_increment_version() {
        let mut tracker = SlotsTracker::new();
        tracker.set_partially_available_slots(&[SlotRange {
            start: 100,
            end: 200,
        }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([], [], [(100, 200)], Some(v1)));

        tracker.promote_to_local_slots(&[SlotRange {
            start: 150,
            end: 175,
        }]);
        assert_eq!(
            tracker,
            ([(150, 175)], [], [(100, 149), (176, 200)], Some(v1)),
            "Version should not change during promotion"
        );
    }

    #[test]
    fn test_promote_to_local_slots_merges_with_existing_local() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 50)], [], [], Some(v1)));

        tracker.set_partially_available_slots(&[SlotRange { start: 51, end: 100 }]);
        assert_eq!(tracker, ([(0, 50)], [], [(51, 100)], Some(v1 + 1)));

        tracker.promote_to_local_slots(&[SlotRange { start: 51, end: 100 }]);
        assert_eq!(
            tracker,
            ([(0, 100)], [], [], Some(v1 + 1)),
            "Adjacent ranges should merge"
        );
    }

    #[test]
    fn test_promote_to_local_slots_partial_overlap() {
        let mut tracker = SlotsTracker::new();
        tracker.set_partially_available_slots(&[SlotRange {
            start: 100,
            end: 200,
        }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([], [], [(100, 200)], Some(v1)));

        tracker.promote_to_local_slots(&[SlotRange {
            start: 150,
            end: 250,
        }]);
        assert_eq!(
            tracker,
            ([(150, 250)], [], [(100, 149)], Some(v1)),
            "Should promote overlapping portion and keep non-overlapping partial"
        );
    }

    #[test]
    fn test_promote_to_local_slots_does_not_affect_fully_available() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        tracker.set_fully_available_slots(&[SlotRange {
            start: 200,
            end: 250,
        }]);
        assert_eq!(tracker, ([(0, 50)], [(200, 250)], [], Some(v1)));

        tracker.set_partially_available_slots(&[SlotRange {
            start: 100,
            end: 150,
        }]);
        assert_eq!(tracker, ([(0, 50)], [(200, 250)], [(100, 150)], Some(v1 + 1)));

        tracker.promote_to_local_slots(&[SlotRange {
            start: 100,
            end: 150,
        }]);
        assert_eq!(
            tracker,
            ([(0, 50), (100, 150)], [(200, 250)], [], Some(v1 + 1)),
            "Fully available slots should remain unchanged"
        );
    }

    #[test]
    fn test_promote_to_local_slots_empty_ranges() {
        let mut tracker = SlotsTracker::new();
        tracker.set_partially_available_slots(&[SlotRange {
            start: 100,
            end: 200,
        }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([], [], [(100, 200)], Some(v1)));

        tracker.promote_to_local_slots(&[]);
        assert_eq!(
            tracker,
            ([], [], [(100, 200)], Some(v1)),
            "Empty promotion should not change state"
        );
    }

    #[test]
    fn test_promote_to_local_slots_non_overlapping() {
        let mut tracker = SlotsTracker::new();
        tracker.set_partially_available_slots(&[SlotRange {
            start: 100,
            end: 200,
        }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([], [], [(100, 200)], Some(v1)));

        tracker.promote_to_local_slots(&[SlotRange {
            start: 300,
            end: 400,
        }]);
        assert_eq!(
            tracker,
            ([(300, 400)], [], [(100, 200)], Some(v1)),
            "Non-overlapping promotion should add to local without affecting partial"
        );
    }

    #[test]
    fn test_promote_to_local_slots_multiple_ranges() {
        let mut tracker = SlotsTracker::new();
        tracker.set_partially_available_slots(&[SlotRange {
            start: 100,
            end: 200,
        }]);
        tracker.set_partially_available_slots(&[SlotRange {
            start: 300,
            end: 400,
        }]);
        let v2 = tracker.get_version();
        assert_eq!(tracker, ([], [], [(100, 200), (300, 400)], Some(v2)));

        tracker.promote_to_local_slots(&[
            SlotRange {
                start: 100,
                end: 150,
            },
            SlotRange {
                start: 350,
                end: 400,
            },
        ]);
        assert_eq!(
            tracker,
            ([(100, 150), (350, 400)], [], [(151, 200), (300, 349)], Some(v2)),
            "Multiple ranges should be promoted correctly"
        );
    }
}
