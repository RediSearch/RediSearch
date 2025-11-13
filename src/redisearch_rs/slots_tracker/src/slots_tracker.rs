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
//! This module provides a single-thread-safe implementation of the slots tracker.
//! All methods take `&mut self`, making the borrowing rules enforce single-threaded access.

use std::num::NonZeroU32;

use crate::SlotRange;
use crate::slot_set::{CoverageRelation, SlotSet};

/// Represents the version state of slot configuration.
///
/// This enum encapsulates the version logic and avoids exposing magic numbers.
/// The discriminant itself IS the version value, making this exactly 32 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    /// Stable version values: 1..=u32::MAX
    /// Each value represents a specific version counter.
    /// Using `NonZeroU32` to keep the enum size exactly 32 bits, allowing the compiler to use 0
    /// as the discriminant for the `Unstable` variant.
    Stable(NonZeroU32),
    /// Unstable state marker
    /// Slots are available but configuration is changing.
    Unstable,
}
// Ensure that Version can be stored as AtomicU32 for fast atomic access.
const _: () = assert!(std::mem::size_of::<Version>() == std::mem::size_of::<u32>());

impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}

impl Version {
    /// Creates a new Version initialized to version 1 (stable).
    const fn new() -> Self {
        // SAFETY: value is not zero
        Self::Stable(unsafe { NonZeroU32::new_unchecked(1) })
    }

    /// Increments a stable version using wrapping arithmetic.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if called on an Unstable version.
    fn increment(self) -> Self {
        match self {
            Self::Stable(v) => {
                if v.get() < u32::MAX {
                    // SAFETY: 1 < v + 1 <= u32::MAX, so not zero
                    Self::Stable(unsafe { NonZeroU32::new_unchecked(v.get() + 1) })
                } else {
                    // Wrap around to 1 from u32::MAX
                    // SAFETY: value is not zero
                    Self::Stable(unsafe { NonZeroU32::new_unchecked(1) })
                }
            }
            Self::Unstable => {
                debug_assert!(false, "Cannot increment Unstable version");
                Self::Unstable
            }
        }
    }
}

/// Safe slots tracker implementation.
///
/// This structure encapsulates all slot tracking state and provides safe methods
/// for manipulating the three slot sets and version counter.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SlotsTracker {
    /// Local responsibility slots - owned by this Redis instance in the cluster topology.
    local: SlotSet,
    /// Fully available non-owned slots - locally available but not owned by this instance.
    fully_available: SlotSet,
    /// Partially available non-owned slots - partially available and not owned by this instance.
    /// These slots cannot be used for searching, and must always be filtered out.
    partially_available: SlotSet,
    /// Version counter for tracking changes to the slots configuration.
    /// Incremented whenever the slot configuration changes. Wraps around safely using wrapping arithmetic.
    /// This is always a Stable variant internally; Unstable is only returned by check_availability
    /// when the configuration state is unstable.
    version: Version,
}

impl SlotsTracker {
    /// Creates a new SlotsTracker with empty slot sets and version 1.
    pub const fn new() -> Self {
        Self {
            local: SlotSet::new(),
            fully_available: SlotSet::new(),
            partially_available: SlotSet::new(),
            version: Version::new(),
        }
    }

    /// Gets the current state version.
    ///
    /// The internal version is always Stable.
    pub const fn get_version(&self) -> Version {
        // Internal invariant: self.version is always Stable
        self.version
    }

    /// Increments the version counter using wrapping arithmetic.
    ///
    /// The version should be incremented whenever slots *become* partially available.
    /// On import, it happens when the import event starts (non-existent slots become partially available).
    /// On migration, it happens when slots start trimming, after the migration has completed successfully.
    /// (local slots becoming fully available, then partially available as they are trimmed away, and finally removed).
    ///
    /// Internal invariant: This is always called on Stable versions.
    fn increment_version(&mut self) {
        // version is always Stable internally
        self.version = self.version.increment();
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
        self.local = SlotSet::from_ranges(ranges);
        self.fully_available.remove_ranges(ranges);
        self.partially_available.remove_ranges(ranges);
        self.increment_version();
    }

    /// Marks the given slot ranges as partially available.
    ///
    /// This function updates the `partially_available` set by adding the provided ranges.
    /// It also removes the given slots from `local` and `fully_available`, and
    /// increments the `version` counter (fully available or not available slots are now partially available).
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges. Must be sorted and normalized (no overlaps, no adjacent ranges).
    pub fn mark_partially_available_slots(&mut self, ranges: &[SlotRange]) {
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
        debug_assert!(!self.fully_available.has_overlap(ranges));
        debug_assert!(matches!(
            self.partially_available.coverage_relation(ranges),
            CoverageRelation::Equals | CoverageRelation::Covers
        ));
        self.local.add_ranges(ranges);
        self.partially_available.remove_ranges(ranges);
    }

    /// Marks the given slot ranges as fully available non-owned.
    ///
    /// This function updates the `fully_available` set by adding the provided ranges.
    /// It removes the given slots from `local`.
    /// Version is NOT incremented by this operation (slots availability is unchanged).
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges. Must be sorted and normalized (no overlaps, no adjacent ranges).
    pub fn mark_fully_available_slots(&mut self, ranges: &[SlotRange]) {
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
    /// - Returns `None` if slots are not available
    ///
    /// # Arguments
    ///
    /// * `ranges` - Slice of slot ranges to check. Must be sorted and normalized.
    ///
    /// # Returns
    ///
    /// - `Some(Stable(version))`: Slots match exactly and are stable
    /// - `Some(Unstable)`: Slots available but partial/inexact match (unstable)
    /// - `None`: Required slots are not available
    pub fn check_availability(&self, ranges: &[SlotRange]) -> Option<Version> {
        // Fast path: If sets 2 & 3 are empty and input exactly matches set 1
        println!("Checking availability");
        println!("query: {:?}", ranges);
        println!("local: {:?}", self.local);
        println!("fully_available: {:?}", self.fully_available);
        println!("partially_available: {:?}", self.partially_available);
        println!("");
        if self.fully_available.is_empty()
            && self.partially_available.is_empty()
            && self.local == ranges
        {
            // Internal version is always Stable, return it directly
            println!("Stable with version LOCAL MATCHES QUERY {:?}", self.version);
            return Some(self.version);
        }

        // Full check: Use union_relation to check coverage
        match self.local.union_relation(&self.fully_available, ranges) {
            CoverageRelation::Equals if self.partially_available.is_empty() => {
                // Exact match and no partial slots - return stable version
                println!("Stable with version {:?}", self.version);
                Some(self.version)
            }
            CoverageRelation::Equals | CoverageRelation::Covers => {
                // Covered but not exact, or has partial slots - return unstable marker
                println!("Unstable");
                Some(Version::Unstable)
            }
            CoverageRelation::NoMatch => {
                // Not all slots are available
                println!("No match");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl From<(u16, u16)> for SlotRange {
        fn from(range: (u16, u16)) -> Self {
            SlotRange {
                start: range.0,
                end: range.1,
            }
        }
    }

    impl<const N1: usize, const N2: usize, const N3: usize>
        PartialEq<(
            [(u16, u16); N1],
            [(u16, u16); N2],
            [(u16, u16); N3],
            Option<Version>,
        )> for SlotsTracker
    {
        fn eq(
            &self,
            other: &(
                [(u16, u16); N1],
                [(u16, u16); N2],
                [(u16, u16); N3],
                Option<Version>,
            ),
        ) -> bool {
            let local: Vec<SlotRange> = other.0.iter().map(|&r| r.into()).collect();
            let fully_available: Vec<SlotRange> = other.1.iter().map(|&r| r.into()).collect();
            let partially_available: Vec<SlotRange> = other.2.iter().map(|&r| r.into()).collect();
            let version = other.3.unwrap_or(self.version);

            self.local == local.as_slice()
                && self.fully_available == fully_available.as_slice()
                && self.partially_available == partially_available.as_slice()
                && self.version == version
        }
    }

    #[test]
    fn test_new_tracker_has_version_zero() {
        let tracker = SlotsTracker::new();
        assert_eq!(tracker.get_version(), Version::new());
    }

    #[test]
    fn test_set_local_slots_increments_version() {
        let mut tracker = SlotsTracker::new();
        let initial_version = tracker.get_version();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);

        assert_eq!(
            tracker,
            ([(0, 100)], [], [], Some(initial_version.increment()))
        );
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
        let initial_version = tracker.get_version();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        let next_version = initial_version.increment();
        assert_eq!(tracker, ([(0, 100)], [], [], Some(next_version)));

        tracker.mark_partially_available_slots(&[SlotRange {
            start: 200,
            end: 300,
        }]);
        let next_next_version = next_version.increment();

        assert_eq!(
            tracker,
            ([(0, 100)], [], [(200, 300)], Some(next_next_version))
        );

        tracker.remove_deleted_slots(&[SlotRange {
            start: 250,
            end: 280,
        }]);
        assert_eq!(
            tracker,
            (
                [(0, 100)],
                [],
                [(200, 249), (281, 300)],
                Some(next_next_version)
            )
        );
    }

    #[test]
    fn test_check_availability_returns_version_for_exact_match() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v1)));

        let result = tracker.check_availability(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(result, Some(v1));

        tracker.mark_fully_available_slots(&[SlotRange { start: 0, end: 50 }]);
        assert_eq!(tracker, ([(51, 100)], [(0, 50)], [], Some(v1)));

        let result2 = tracker.check_availability(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(result2, Some(v1));
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
        assert_eq!(result, None);
    }

    #[test]
    fn test_has_fully_available_overlap() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 200 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 200)], [], [], Some(v1)));

        tracker.mark_fully_available_slots(&[SlotRange {
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
        // Set to u32::MAX - 1, so next increment goes to MAX, then wraps to 1
        tracker.version = Version::Stable(NonZeroU32::new(u32::MAX - 1).unwrap());

        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        // After increment from MAX-1, we get MAX
        assert_eq!(
            tracker,
            (
                [(0, 100)],
                [],
                [],
                Some(Version::Stable(NonZeroU32::new(u32::MAX).unwrap()))
            )
        );

        // Increment again to wrap to 1
        tracker.set_local_slots(&[SlotRange { start: 0, end: 101 }]);
        assert_eq!(tracker, ([(0, 101)], [], [], Some(Version::new())));
    }

    #[test]
    fn test_partially_available_increments_version() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 50)], [], [], Some(v1)));
        tracker.mark_partially_available_slots(&[SlotRange { start: 60, end: 70 }]);
        assert_eq!(tracker, ([(0, 50)], [], [(60, 70)], Some(v1.increment())));
    }

    #[test]
    fn test_fully_available_does_not_increment_version() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v1)));
        tracker.mark_fully_available_slots(&[SlotRange { start: 10, end: 20 }]);
        assert_eq!(tracker, ([(0, 9), (21, 100)], [(10, 20)], [], Some(v1)));
    }

    #[test]
    fn test_check_availability_unstable_due_to_fully_available_extra() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 50)], [], [], Some(v1)));
        tracker.mark_fully_available_slots(&[SlotRange {
            start: 100,
            end: 120,
        }]);
        assert_eq!(tracker, ([(0, 50)], [(100, 120)], [], Some(v1)));

        let res = tracker.check_availability(&[SlotRange { start: 0, end: 50 }]);
        // Implementation marks as UNSTABLE since union covers exact local but extra disjoint slots exist.
        assert_eq!(res, Some(Version::Unstable));
        let res2 = tracker.check_availability(&[SlotRange { start: 0, end: 120 }]);
        assert_eq!(res2, None);
    }

    #[test]
    fn test_check_availability_unavailable_when_not_covered() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 10 }]);
        assert_eq!(tracker, ([(0, 10)], [], [], None));
        let res = tracker.check_availability(&[SlotRange { start: 0, end: 20 }]);
        assert_eq!(res, None);
    }

    #[test]
    fn test_partially_available_makes_unstable() {
        let mut tracker = SlotsTracker::new();
        let v0 = tracker.get_version();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 100 }]);
        let Version::Stable(v0_num) = v0 else {
            panic!()
        };
        assert_eq!(
            tracker,
            (
                [(0, 100)],
                [],
                [],
                Some(Version::Stable(
                    NonZeroU32::new(v0_num.get().saturating_add(1)).unwrap()
                ))
            )
        );
        tracker.mark_partially_available_slots(&[SlotRange {
            start: 150,
            end: 160,
        }]);
        assert_eq!(
            tracker,
            (
                [(0, 100)],
                [],
                [(150, 160)],
                Some(Version::Stable(
                    NonZeroU32::new(v0_num.get().saturating_add(2)).unwrap()
                ))
            )
        );

        let res = tracker.check_availability(&[SlotRange { start: 0, end: 100 }]);
        assert_eq!(res, Some(Version::Unstable));
        let res2 = tracker.check_availability(&[SlotRange { start: 0, end: 160 }]);
        assert_eq!(res2, None);
    }

    #[test]
    fn test_remove_deleted_slots_only_affects_partially_available() {
        let mut tracker = SlotsTracker::new();
        tracker.mark_partially_available_slots(&[SlotRange {
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
        assert_eq!(res, None);
    }

    #[test]
    fn test_sequential_version_changes() {
        let mut tracker = SlotsTracker::new();
        let initial_version = tracker.get_version();
        assert_eq!(tracker.get_version(), Version::new());
        assert!(tracker.local.is_empty());
        assert!(tracker.fully_available.is_empty());
        assert!(tracker.partially_available.is_empty());

        tracker.set_local_slots(&[SlotRange { start: 0, end: 10 }]);
        assert_eq!(
            tracker,
            ([(0, 10)], [], [], Some(initial_version.increment()))
        );

        tracker.mark_partially_available_slots(&[SlotRange { start: 20, end: 30 }]);
        assert_eq!(
            tracker,
            (
                [(0, 10)],
                [],
                [(20, 30)],
                Some(initial_version.increment().increment())
            )
        );

        tracker.mark_fully_available_slots(&[SlotRange { start: 5, end: 6 }]);
        assert_eq!(
            tracker,
            (
                [(0, 4), (7, 10)],
                [(5, 6)],
                [(20, 30)],
                Some(initial_version.increment().increment())
            )
        );

        tracker.set_local_slots(&[SlotRange { start: 0, end: 5 }]);
        assert_eq!(
            tracker,
            (
                [(0, 5)],
                [(6, 6)],
                [(20, 30)],
                Some(initial_version.increment().increment().increment())
            )
        );
    }

    #[test]
    fn test_sets_content_after_operations() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 10 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 10)], [], [], Some(v1)));
        tracker.mark_fully_available_slots(&[SlotRange { start: 0, end: 5 }]);
        assert_eq!(tracker, ([(6, 10)], [(0, 5)], [], Some(v1)));
        tracker.mark_partially_available_slots(&[SlotRange { start: 50, end: 55 }]);
        assert_eq!(
            tracker,
            ([(6, 10)], [(0, 5)], [(50, 55)], Some(v1.increment()))
        );
    }

    #[test]
    fn test_mixed_local_and_partial_query_unavailable() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 10 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 10)], [], [], Some(v1)));
        tracker.mark_partially_available_slots(&[SlotRange { start: 20, end: 25 }]);
        assert_eq!(tracker, ([(0, 10)], [], [(20, 25)], Some(v1.increment())));

        let res = tracker.check_availability(&[SlotRange { start: 0, end: 25 }]);
        assert_eq!(res, None);
    }

    #[test]
    fn test_promote_to_local_slots_basic() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 50)], [], [], Some(v1)));

        tracker.mark_partially_available_slots(&[SlotRange {
            start: 100,
            end: 150,
        }]);
        assert_eq!(tracker, ([(0, 50)], [], [(100, 150)], Some(v1.increment())));
        assert_eq!(
            tracker.check_availability(&[
                SlotRange { start: 0, end: 50 },
                SlotRange {
                    start: 100,
                    end: 150
                }
            ]),
            None
        );

        tracker.promote_to_local_slots(&[SlotRange {
            start: 100,
            end: 150,
        }]);
        assert_eq!(
            tracker,
            ([(0, 50), (100, 150)], [], [], Some(v1.increment()))
        );
        assert_eq!(
            tracker.check_availability(&[
                SlotRange { start: 0, end: 50 },
                SlotRange {
                    start: 100,
                    end: 150
                }
            ]),
            Some(v1.increment())
        );
    }

    #[test]
    fn test_promote_to_local_slots_does_not_increment_version() {
        let mut tracker = SlotsTracker::new();
        tracker.mark_partially_available_slots(&[SlotRange {
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
            ([(150, 175)], [], [(100, 149), (176, 200)], Some(v1))
        );
    }

    #[test]
    fn test_promote_to_local_slots_merges_with_existing_local() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([(0, 50)], [], [], Some(v1)));

        tracker.mark_partially_available_slots(&[SlotRange {
            start: 51,
            end: 100,
        }]);
        assert_eq!(tracker, ([(0, 50)], [], [(51, 100)], Some(v1.increment())));

        tracker.promote_to_local_slots(&[SlotRange {
            start: 51,
            end: 100,
        }]);
        assert_eq!(tracker, ([(0, 100)], [], [], Some(v1.increment())));
    }

    #[test]
    fn test_promote_to_local_slots_does_not_affect_fully_available() {
        let mut tracker = SlotsTracker::new();
        tracker.set_local_slots(&[SlotRange { start: 0, end: 50 }]);
        let v1 = tracker.get_version();
        tracker.mark_fully_available_slots(&[SlotRange {
            start: 200,
            end: 250,
        }]);
        assert_eq!(tracker, ([(0, 50)], [(200, 250)], [], Some(v1)));

        tracker.mark_partially_available_slots(&[SlotRange {
            start: 100,
            end: 150,
        }]);
        assert_eq!(
            tracker,
            ([(0, 50)], [(200, 250)], [(100, 150)], Some(v1.increment()))
        );

        tracker.promote_to_local_slots(&[SlotRange {
            start: 100,
            end: 150,
        }]);
        assert_eq!(
            tracker,
            (
                [(0, 50), (100, 150)],
                [(200, 250)],
                [],
                Some(v1.increment())
            )
        );
    }

    #[test]
    fn test_promote_to_local_slots_empty_ranges() {
        let mut tracker = SlotsTracker::new();
        tracker.mark_partially_available_slots(&[SlotRange {
            start: 100,
            end: 200,
        }]);
        let v1 = tracker.get_version();
        assert_eq!(tracker, ([], [], [(100, 200)], Some(v1)));

        tracker.promote_to_local_slots(&[]);
        assert_eq!(tracker, ([], [], [(100, 200)], Some(v1)));
    }

    #[test]
    fn test_promote_to_local_slots_multiple_ranges() {
        let mut tracker = SlotsTracker::new();
        tracker.mark_partially_available_slots(&[SlotRange {
            start: 100,
            end: 200,
        }]);
        tracker.mark_partially_available_slots(&[SlotRange {
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
            (
                [(100, 150), (350, 400)],
                [],
                [(151, 200), (300, 349)],
                Some(v2)
            )
        );
    }
}
