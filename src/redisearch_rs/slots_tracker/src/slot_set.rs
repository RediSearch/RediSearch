/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Internal implementation of the SlotSet data structure.
//!
//! This module contains the private implementation of slot range tracking.
//! It is not exposed outside of the slots_tracker crate.

use crate::SlotRange;

/// Enum describing the relationship between a set and a query.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum CoverageRelation {
    Equals,  // Set == query (exact match)
    Covers,  // Set ⊇ query (covers but has extra)
    NoMatch, // Set ⊉ query (doesn't cover all)
}

// ============================================================================
// Debug assertion helpers
// ============================================================================

/// Validates that all ranges are valid.
#[inline]
fn debug_assert_valid_ranges(ranges: &[SlotRange]) {
    debug_assert!(ranges.iter().all(|r| r.start <= r.end && r.end <= 16383));
}

/// Validates that ranges are sorted and normalized (no overlaps or adjacent ranges).
#[inline]
fn debug_assert_normalized_ranges(ranges: &[SlotRange]) {
    debug_assert!(ranges.windows(2).all(|w| w[0].end + 1 < w[1].start));
}

/// Validates that ranges are valid, sorted, and normalized.
#[inline]
fn debug_assert_valid_normalized_input(ranges: &[SlotRange]) {
    debug_assert_valid_ranges(ranges);
    debug_assert_normalized_ranges(ranges);
}

/// A collection of slot ranges with set operation capabilities.
///
/// This is an internal type used only within this crate to manage slot sets.
/// Each range is inclusive [start, end].
///
/// **Invariant**: The ranges vector is always kept sorted and normalized:
/// - Sorted by start slot in ascending order
/// - No overlapping ranges
/// - No adjacent ranges (they are merged)
/// - All ranges are valid (start <= end, values in [0, 16383])
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SlotSet {
    /// Vector of slot ranges (start, end), kept sorted and normalized.
    ranges: Vec<SlotRange>,
}

impl SlotSet {
    /// Creates a new empty `SlotSet`.
    pub(crate) const fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Creates a new SlotSet from the given ranges.
    ///
    /// The input is required to be sorted and normalized.
    pub(crate) fn from_ranges(ranges: &[SlotRange]) -> Self {
        debug_assert_valid_normalized_input(ranges);

        // Input is already normalized, just replace our vector
        Self {
            ranges: ranges.to_vec(),
        }
    }

    /// Adds/merges ranges into the set (union operation).
    ///
    /// The input is required to be sorted and normalized.
    pub(crate) fn add_ranges(&mut self, ranges: &[SlotRange]) {
        debug_assert_valid_normalized_input(ranges);

        // Simply add all ranges and normalize once at the end
        self.ranges.extend_from_slice(ranges);
        self.normalize();
    }

    /// Removes any slots that overlap with the given ranges.
    ///
    /// The input is required to be sorted and normalized.
    ///
    /// **Optimized**: Takes advantage of sorted input to avoid re-scanning from the beginning.
    pub(crate) fn remove_ranges(&mut self, ranges: &[SlotRange]) {
        debug_assert_valid_normalized_input(ranges);

        let old_ranges = std::mem::take(&mut self.ranges);
        let mut remove_iter = ranges.iter().peekable();

        'outer: for mut current in old_ranges {
            // Skip remove ranges that end before current starts
            while remove_iter.next_if(|&&r| r.end < current.start).is_some() {}

            // Apply all overlapping remove ranges to current
            while let Some(&&remove) = remove_iter.peek() {
                if remove.start > current.end {
                    break; // No more overlaps for current
                }

                // Handle overlap cases
                match (remove.start <= current.start, remove.end >= current.end) {
                    (true, true) => {
                        // Complete overlap - discard current and move to next
                        // Don't advance remove iterator - next range might also start within this remove range
                        continue 'outer;
                    }
                    (true, false) => {
                        // Remove overlaps left side - trim left
                        current.start = remove.end + 1;
                        remove_iter.next();
                    }
                    (false, true) => {
                        // Remove overlaps right side - trim right and done
                        // Don't advance remove iterator - next range might also start within this remove range
                        current.end = remove.start - 1;
                        break;
                    }
                    (false, false) => {
                        // Remove is in the middle - split current
                        self.ranges.push(SlotRange {
                            start: current.start,
                            end: remove.start - 1,
                        });
                        current.start = remove.end + 1;
                        remove_iter.next();
                    }
                }
            }

            // Keep what remains of current
            self.ranges.push(current);
        }
    }

    /// Checks if any of the given ranges overlap with any ranges in this set.
    ///
    /// The input is required to be sorted and normalized.
    ///
    /// **Optimized**: Uses iterators with two-pointer technique since both inputs are sorted.
    pub(crate) fn has_overlap(&self, ranges: &[SlotRange]) -> bool {
        debug_assert_valid_normalized_input(ranges);

        let mut their_iter = ranges.iter().peekable();

        for &our_range in &self.ranges {
            // Skip their ranges that end before our current range starts
            while their_iter.next_if(|&&r| r.end < our_range.start).is_some() {}

            let Some(&&their_range) = their_iter.peek() else {
                break;
            };

            // Check if next their range overlaps with our current range
            if their_range.start <= our_range.end {
                return true; // Overlap found
            }
        }

        false
    }

    /// Returns true if this set contains no ranges.
    pub(crate) const fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Checks the relationship between this set and input ranges in a single pass.
    ///
    /// Returns `CoverageRelation` indicating: `Equals`, `Covers`, or `NoMatch`.
    ///
    /// # Single-Pass Algorithm
    ///
    /// Walks through both sorted range lists simultaneously, tracking coverage and extras:
    /// - If we don't cover all input slots: returns `NoMatch`
    /// - If we cover exactly the input slots: returns `Equals`
    /// - If we cover all input slots plus extras: returns `Covers`
    pub(crate) fn coverage_relation(&self, ranges: &[SlotRange]) -> CoverageRelation {
        debug_assert_valid_normalized_input(ranges);

        let mut our_iter = self.ranges.iter().peekable();
        let mut has_extra = false;

        for &their_range in ranges {
            // Find the first of our ranges that does not end before their range starts
            while our_iter.next_if(|&&r| r.end < their_range.start).is_some() {
                has_extra = true; // We skipped a range, so we have extra slots
            }

            let Some(&&current_our) = our_iter.peek() else {
                return CoverageRelation::NoMatch;
            };

            // Check if our range starts before their range
            if current_our.start > their_range.start {
                // Gap found - doesn't cover
                return CoverageRelation::NoMatch;
            }
            // Check if our range ends before their range.
            // If so, we don't cover `current_our.end + 1` (not even in the next range, as our ranges are normalized), while their range does.
            if current_our.end < their_range.end {
                // Gap found - doesn't cover
                return CoverageRelation::NoMatch;
            }

            // Our range completely covers their range:
            // current_our.start <= their_range.start, current_our.end >= their_range.end
            // If it's not an exact match, we have extra slots
            // We also don't want to advance our iterator if it ends past their range, as it may cover their next range as well.
            has_extra |= current_our != their_range;
            if current_our.end == their_range.end {
                our_iter.next();
            }
        }

        // Check if we have leftover ranges
        if our_iter.peek().is_some() {
            has_extra = true;
        }

        if has_extra {
            CoverageRelation::Covers
        } else {
            CoverageRelation::Equals
        }
    }

    /// Checks the relationship between the union of this set and another set vs input ranges.
    ///
    /// Returns `CoverageRelation` indicating: `Equals`, `Covers`, or `NoMatch`.
    pub(crate) fn union_relation(&self, other: &SlotSet, ranges: &[SlotRange]) -> CoverageRelation {
        if self.is_empty() {
            other.coverage_relation(ranges)
        } else if other.is_empty() {
            self.coverage_relation(ranges)
        } else {
            let mut combined = self.clone();
            combined.add_ranges(&other.ranges);
            combined.coverage_relation(ranges)
        }
    }

    // ========================================================================
    // Private helper methods:
    // ========================================================================

    /// Normalizes the internal ranges: sorts and merges overlapping/adjacent ranges.
    fn normalize(&mut self) {
        if self.ranges.len() <= 1 {
            return;
        }

        // Sort by start position
        self.ranges.sort_unstable_by_key(|r| r.start);

        // Merge overlapping and adjacent ranges in-place
        let mut write_pos = 0;

        for read_pos in 1..self.ranges.len() {
            let current = self.ranges[read_pos];

            if self.ranges[write_pos].end + 1 >= current.start {
                // Overlapping or adjacent - merge into write_pos
                self.ranges[write_pos].end = self.ranges[write_pos].end.max(current.end);
            } else {
                // Not adjacent - advance write position and copy
                write_pos += 1;
                self.ranges[write_pos] = current;
            }
        }

        // Truncate to the merged length
        self.ranges.truncate(write_pos + 1);
    }
}

// Implement PartialEq with slices for convenient comparisons
impl PartialEq<[SlotRange]> for SlotSet {
    fn eq(&self, other: &[SlotRange]) -> bool {
        self.ranges == other
    }
}

impl PartialEq<&[SlotRange]> for SlotSet {
    fn eq(&self, other: &&[SlotRange]) -> bool {
        self.ranges == *other
    }
}

impl PartialEq<SlotSet> for [SlotRange] {
    fn eq(&self, other: &SlotSet) -> bool {
        self == other.ranges
    }
}

impl PartialEq<SlotSet> for &[SlotRange] {
    fn eq(&self, other: &SlotSet) -> bool {
        *self == other.ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a SlotRange
    fn range(start: u16, end: u16) -> SlotRange {
        SlotRange { start, end }
    }

    // ========================================================================
    // Basic construction and equality tests
    // ========================================================================

    #[test]
    fn test_new_is_empty() {
        let set = SlotSet::new();
        assert!(set.is_empty());
    }

    #[test]
    fn test_default_is_empty() {
        let set = SlotSet::default();
        assert!(set.is_empty());
    }

    #[test]
    fn test_equality_with_self() {
        let set = SlotSet::from_ranges(&[range(0, 10)]);
        assert_eq!(set, set.clone());
    }

    #[test]
    fn test_equality_with_slice() {
        let ranges = [range(0, 10), range(20, 30)];
        let set = SlotSet::from_ranges(&ranges);
        assert_eq!(set, ranges[..]);
        assert_eq!(set, ranges.as_slice());
        assert_eq!(ranges[..], set);
        assert_eq!(ranges.as_slice(), set);
    }

    // ========================================================================
    // from_ranges tests
    // ========================================================================

    #[test]
    fn test_from_ranges_empty() {
        let set = SlotSet::from_ranges(&[]);
        assert!(set.is_empty());
    }

    #[test]
    fn test_from_ranges_single() {
        let set = SlotSet::from_ranges(&[range(5, 10)]);
        assert_eq!(set, &[range(5, 10)][..]);
    }

    #[test]
    fn test_from_ranges_multiple() {
        let ranges = [range(0, 5), range(10, 15), range(20, 25)];
        let set = SlotSet::from_ranges(&ranges);
        assert_eq!(set, &ranges[..]);
    }

    // ========================================================================
    // add_ranges tests (union operation)
    // ========================================================================

    #[test]
    fn test_add_ranges_to_empty() {
        let mut set = SlotSet::new();
        set.add_ranges(&[range(10, 20)]);
        assert_eq!(set, &[range(10, 20)][..]);
    }

    #[test]
    fn test_add_ranges_non_overlapping() {
        let mut set = SlotSet::from_ranges(&[range(0, 10)]);
        set.add_ranges(&[range(20, 30)]);
        assert_eq!(set, &[range(0, 10), range(20, 30)][..]);
    }

    #[test]
    fn test_add_ranges_overlapping() {
        let mut set = SlotSet::from_ranges(&[range(0, 10)]);
        set.add_ranges(&[range(5, 15)]);
        assert_eq!(set, &[range(0, 15)][..]);
    }

    #[test]
    fn test_add_ranges_adjacent_merges() {
        let mut set = SlotSet::from_ranges(&[range(0, 10)]);
        set.add_ranges(&[range(11, 20)]);
        assert_eq!(set, &[range(0, 20)][..]);
    }

    #[test]
    fn test_add_ranges_subset() {
        let mut set = SlotSet::from_ranges(&[range(0, 100)]);
        set.add_ranges(&[range(10, 20)]);
        assert_eq!(set, &[range(0, 100)][..]);
    }

    #[test]
    fn test_add_ranges_superset() {
        let mut set = SlotSet::from_ranges(&[range(10, 20)]);
        set.add_ranges(&[range(0, 100)]);
        assert_eq!(set, &[range(0, 100)][..]);
    }

    #[test]
    fn test_add_ranges_multiple_merges() {
        let mut set = SlotSet::from_ranges(&[range(0, 5), range(10, 15), range(20, 25)]);
        set.add_ranges(&[range(6, 19)]);
        assert_eq!(set, &[range(0, 25)][..]);
    }

    #[test]
    fn test_add_ranges_empty() {
        let mut set = SlotSet::from_ranges(&[range(0, 10)]);
        set.add_ranges(&[]);
        assert_eq!(set, &[range(0, 10)][..]);
    }

    #[test]
    fn test_add_multiple_ranges() {
        let mut set = SlotSet::from_ranges(&[range(10, 15)]);
        set.add_ranges(&[range(0, 5), range(20, 30)]);
        assert_eq!(set, &[range(0, 5), range(10, 15), range(20, 30)][..]);
    }

    #[test]
    fn test_add_multiple_ranges_adjacent() {
        let mut set = SlotSet::from_ranges(&[range(5, 10), range(15, 20)]);
        set.add_ranges(&[range(0, 5), range(10, 15), range(20, 30)]);
        assert_eq!(set, &[range(0, 30)][..]);
    }

    // ========================================================================
    // remove_ranges tests
    // ========================================================================

    #[test]
    fn test_remove_ranges_from_empty() {
        let mut set = SlotSet::new();
        set.remove_ranges(&[range(10, 20)]);
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove_ranges_no_overlap() {
        let mut set = SlotSet::from_ranges(&[range(0, 10)]);
        set.remove_ranges(&[range(20, 30)]);
        assert_eq!(set, &[range(0, 10)][..]);
    }

    #[test]
    fn test_remove_ranges_exact_match() {
        let mut set = SlotSet::from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(10, 20)]);
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove_ranges_complete_overlap() {
        let mut set = SlotSet::from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(0, 30)]);
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove_ranges_trim_left() {
        let mut set = SlotSet::from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(5, 15)]);
        assert_eq!(set, &[range(16, 20)][..]);
    }

    #[test]
    fn test_remove_ranges_trim_right() {
        let mut set = SlotSet::from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(15, 25)]);
        assert_eq!(set, &[range(10, 14)][..]);
    }

    #[test]
    fn test_remove_ranges_split_middle() {
        let mut set = SlotSet::from_ranges(&[range(10, 30)]);
        set.remove_ranges(&[range(15, 20)]);
        assert_eq!(set, &[range(10, 14), range(21, 30)][..]);
    }

    #[test]
    fn test_remove_ranges_multiple_overlaps() {
        let mut set = SlotSet::from_ranges(&[range(0, 10), range(20, 30), range(40, 50)]);
        set.remove_ranges(&[range(5, 25)]);
        assert_eq!(set, &[range(0, 4), range(26, 30), range(40, 50)][..]);
    }

    #[test]
    fn test_remove_ranges_multiple_splits() {
        let mut set = SlotSet::from_ranges(&[range(0, 100)]);
        set.remove_ranges(&[range(10, 20), range(30, 40), range(50, 60)]);
        assert_eq!(
            set,
            &[range(0, 9), range(21, 29), range(41, 49), range(61, 100)][..]
        );
    }

    #[test]
    fn test_remove_ranges_consecutive_removes_same_range() {
        let mut set = SlotSet::from_ranges(&[range(0, 10), range(20, 30), range(40, 50)]);
        // Remove range that covers multiple of our ranges
        set.remove_ranges(&[range(5, 45)]);
        assert_eq!(set, &[range(0, 4), range(46, 50)][..]);
    }

    #[test]
    fn test_remove_ranges_empty() {
        let mut set = SlotSet::from_ranges(&[range(0, 10)]);
        set.remove_ranges(&[]);
        assert_eq!(set, &[range(0, 10)][..]);
    }

    #[test]
    fn test_remove_redundant_ranges() {
        let mut set = SlotSet::from_ranges(&[range(5, 10), range(12, 12), range(20, 25)]);
        set.remove_ranges(&[range(0, 3), range(13, 15), range(18, 19)]);
        assert_eq!(set, &[range(5, 10), range(12, 12), range(20, 25)][..]);
    }

    // ========================================================================
    // has_overlap tests
    // ========================================================================

    #[test]
    fn test_has_overlap_empty_sets() {
        let set = SlotSet::new();
        assert!(!set.has_overlap(&[]));
    }

    #[test]
    fn test_has_overlap_empty_self() {
        let set = SlotSet::new();
        assert!(!set.has_overlap(&[range(0, 10)]));
    }

    #[test]
    fn test_has_overlap_empty_input() {
        let set = SlotSet::from_ranges(&[range(0, 10)]);
        assert!(!set.has_overlap(&[]));
    }

    #[test]
    fn test_has_overlap_no_overlap() {
        let set = SlotSet::from_ranges(&[range(0, 10)]);
        assert!(!set.has_overlap(&[range(20, 30)]));
    }

    #[test]
    fn test_has_overlap_exact_match() {
        let set = SlotSet::from_ranges(&[range(10, 20)]);
        assert!(set.has_overlap(&[range(10, 20)]));
    }

    #[test]
    fn test_has_overlap_partial_left() {
        let set = SlotSet::from_ranges(&[range(10, 20)]);
        assert!(set.has_overlap(&[range(5, 15)]));
    }

    #[test]
    fn test_has_overlap_partial_right() {
        let set = SlotSet::from_ranges(&[range(10, 20)]);
        assert!(set.has_overlap(&[range(15, 25)]));
    }

    #[test]
    fn test_has_overlap_contained() {
        let set = SlotSet::from_ranges(&[range(10, 30)]);
        assert!(set.has_overlap(&[range(15, 20)]));
    }

    #[test]
    fn test_has_overlap_contains() {
        let set = SlotSet::from_ranges(&[range(15, 20)]);
        assert!(set.has_overlap(&[range(10, 30)]));
    }

    #[test]
    fn test_has_overlap_adjacent_no_overlap() {
        let set = SlotSet::from_ranges(&[range(0, 10)]);
        assert!(!set.has_overlap(&[range(11, 20)]));
    }

    #[test]
    fn test_has_overlap_multiple_ranges_with_overlap() {
        let set = SlotSet::from_ranges(&[range(0, 10), range(20, 30), range(40, 50)]);
        assert!(set.has_overlap(&[range(25, 35)]));
    }

    #[test]
    fn test_has_overlap_multiple_ranges_no_overlap() {
        let set = SlotSet::from_ranges(&[range(0, 10), range(20, 30), range(40, 50)]);
        assert!(!set.has_overlap(&[range(11, 19), range(31, 39)]));
    }

    // ========================================================================
    // coverage_relation tests
    // ========================================================================

    #[test]
    fn test_coverage_relation_empty_both() {
        let set = SlotSet::new();
        assert_eq!(set.coverage_relation(&[]), CoverageRelation::Equals);
    }

    #[test]
    fn test_coverage_relation_empty_self() {
        let set = SlotSet::new();
        assert_eq!(
            set.coverage_relation(&[range(10, 20)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_coverage_relation_empty_input() {
        let set = SlotSet::from_ranges(&[range(10, 20)]);
        assert_eq!(set.coverage_relation(&[]), CoverageRelation::Covers);
    }

    #[test]
    fn test_coverage_relation_equals_single() {
        let set = SlotSet::from_ranges(&[range(10, 20)]);
        assert_eq!(
            set.coverage_relation(&[range(10, 20)]),
            CoverageRelation::Equals
        );
    }

    #[test]
    fn test_coverage_relation_equals_multiple() {
        let set = SlotSet::from_ranges(&[range(0, 10), range(20, 30), range(40, 50)]);
        assert_eq!(
            set.coverage_relation(&[range(0, 10), range(20, 30), range(40, 50)]),
            CoverageRelation::Equals
        );
    }

    #[test]
    fn test_coverage_relation_covers_superset() {
        let set = SlotSet::from_ranges(&[range(0, 100)]);
        assert_eq!(
            set.coverage_relation(&[range(10, 20)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_coverage_relation_covers_extra_range() {
        let set = SlotSet::from_ranges(&[range(0, 10), range(20, 30)]);
        assert_eq!(
            set.coverage_relation(&[range(0, 10)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_coverage_relation_covers_multiple_inputs() {
        let set = SlotSet::from_ranges(&[range(0, 100)]);
        assert_eq!(
            set.coverage_relation(&[range(10, 20), range(30, 40), range(50, 60)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_coverage_relation_covers_one_range_spans_multiple_inputs() {
        let set = SlotSet::from_ranges(&[range(0, 50)]);
        assert_eq!(
            set.coverage_relation(&[range(10, 20), range(25, 35)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_coverage_relation_no_match_gap_at_start() {
        let set = SlotSet::from_ranges(&[range(10, 20)]);
        assert_eq!(
            set.coverage_relation(&[range(5, 15)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_coverage_relation_no_match_gap_at_end() {
        let set = SlotSet::from_ranges(&[range(10, 20)]);
        assert_eq!(
            set.coverage_relation(&[range(15, 25)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_coverage_relation_no_match_gap_in_middle() {
        let set = SlotSet::from_ranges(&[range(0, 10), range(30, 40)]);
        assert_eq!(
            set.coverage_relation(&[range(0, 40)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_coverage_relation_no_match_disjoint() {
        let set = SlotSet::from_ranges(&[range(0, 10)]);
        assert_eq!(
            set.coverage_relation(&[range(20, 30)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_coverage_relation_no_match_partial_coverage() {
        let set = SlotSet::from_ranges(&[range(0, 15), range(25, 40)]);
        assert_eq!(
            set.coverage_relation(&[range(10, 30)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_coverage_relation_covers_exact_with_extras() {
        let set = SlotSet::from_ranges(&[range(0, 5), range(10, 15), range(20, 30)]);
        assert_eq!(
            set.coverage_relation(&[range(0, 5), range(10, 15)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_coverage_relation_covers_with_leftover_prefix() {
        let set = SlotSet::from_ranges(&[range(0, 30)]);
        assert_eq!(
            set.coverage_relation(&[range(10, 30)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_coverage_relation_covers_with_leftover_suffix() {
        let set = SlotSet::from_ranges(&[range(0, 30)]);
        assert_eq!(
            set.coverage_relation(&[range(0, 20)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_coverage_relation_single_slot() {
        let set = SlotSet::from_ranges(&[range(15, 15)]);
        assert_eq!(
            set.coverage_relation(&[range(15, 15)]),
            CoverageRelation::Equals
        );
    }

    #[test]
    fn test_coverage_relation_covers_single_slot_in_range() {
        let set = SlotSet::from_ranges(&[range(10, 20)]);
        assert_eq!(
            set.coverage_relation(&[range(15, 15)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_coverage_relation_complex_equals() {
        let set = SlotSet::from_ranges(&[range(0, 5), range(10, 15), range(20, 25), range(30, 35)]);
        assert_eq!(
            set.coverage_relation(&[range(0, 5), range(10, 15), range(20, 25), range(30, 35)]),
            CoverageRelation::Equals
        );
    }

    #[test]
    fn test_coverage_relation_multiple_ranges_cover_one_input() {
        let set = SlotSet::from_ranges(&[range(0, 10), range(12, 20), range(22, 30)]);
        assert_eq!(
            set.coverage_relation(&[range(5, 25)]),
            CoverageRelation::NoMatch // Gap at 11 and 21
        );
    }

    #[test]
    fn test_coverage_relation_one_range_covers_multiple_with_gaps() {
        let set = SlotSet::from_ranges(&[range(0, 50)]);
        assert_eq!(
            set.coverage_relation(&[range(5, 10), range(15, 20), range(25, 30)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_coverage_relation_adjacent_ranges() {
        let set = SlotSet::from_ranges(&[range(0, 10), range(20, 30)]);
        assert_eq!(
            set.coverage_relation(&[range(0, 10), range(20, 30)]),
            CoverageRelation::Equals
        );
    }

    #[test]
    fn test_coverage_relation_no_match_insufficient_ranges() {
        let set = SlotSet::from_ranges(&[range(0, 5)]);
        assert_eq!(
            set.coverage_relation(&[range(0, 5), range(10, 15)]),
            CoverageRelation::NoMatch
        );
    }

    // ========================================================================
    // union_relation tests
    // ========================================================================

    #[test]
    fn test_union_relation_equals_empty() {
        let set1 = SlotSet::new();
        let set2 = SlotSet::new();
        assert_eq!(set1.union_relation(&set2, &[]), CoverageRelation::Equals);
    }

    #[test]
    fn test_union_relation_equals() {
        let set1 = SlotSet::from_ranges(&[range(0, 10)]);
        let set2 = SlotSet::from_ranges(&[range(20, 30)]);
        assert_eq!(
            set1.union_relation(&set2, &[range(0, 10), range(20, 30)]),
            CoverageRelation::Equals
        );
    }

    #[test]
    fn test_union_relation_covers() {
        let set1 = SlotSet::from_ranges(&[range(0, 100)]);
        let set2 = SlotSet::new();
        assert_eq!(
            set1.union_relation(&set2, &[range(10, 20)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_union_relation_no_match() {
        let set1 = SlotSet::from_ranges(&[range(0, 10)]);
        let set2 = SlotSet::new();
        assert_eq!(
            set1.union_relation(&set2, &[range(20, 30)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_union_relation_partial_coverage() {
        let set1 = SlotSet::from_ranges(&[range(0, 10)]);
        let set2 = SlotSet::from_ranges(&[range(20, 30)]);
        assert_eq!(
            set1.union_relation(&set2, &[range(0, 30)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_union_relation_with_gaps() {
        let set1 = SlotSet::from_ranges(&[range(0, 10), range(30, 40)]);
        let set2 = SlotSet::from_ranges(&[range(11, 29)]);
        assert_eq!(
            set1.union_relation(&set2, &[range(0, 40)]),
            CoverageRelation::Equals
        );
    }

    // ========================================================================
    // Edge cases and boundary tests
    // ========================================================================

    #[test]
    fn test_max_slot_value() {
        let set = SlotSet::from_ranges(&[range(16380, 16383)]);
        assert_eq!(set, &[range(16380, 16383)][..]);
    }

    #[test]
    fn test_single_slot_range() {
        let set = SlotSet::from_ranges(&[range(100, 100)]);
        assert_eq!(set, &[range(100, 100)][..]);
    }

    #[test]
    fn test_remove_single_slot() {
        let mut set = SlotSet::from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(15, 15)]);
        assert_eq!(set, &[range(10, 14), range(16, 20)][..]);
    }

    #[test]
    fn test_full_range() {
        let set = SlotSet::from_ranges(&[range(0, 16383)]);
        assert_eq!(set, &[range(0, 16383)][..]);
    }

    #[test]
    fn test_complex_operations_sequence() {
        // Start with some ranges
        let mut set = SlotSet::from_ranges(&[range(0, 100), range(200, 300)]);

        // Add overlapping ranges
        set.add_ranges(&[range(50, 150), range(250, 350)]);
        assert_eq!(set, &[range(0, 150), range(200, 350)][..]);

        // Remove from middle
        set.remove_ranges(&[range(75, 275)]);
        assert_eq!(set, &[range(0, 74), range(276, 350)][..]);

        // Check overlap
        assert!(set.has_overlap(&[range(70, 80)]));
        assert!(!set.has_overlap(&[range(100, 200)]));
    }
}
