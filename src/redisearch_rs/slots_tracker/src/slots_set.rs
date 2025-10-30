/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Internal implementation of the SlotsSet data structure.
//!
//! This module contains the private implementation of slot range tracking.
//! It is not exposed outside of the slots_tracker crate.

use crate::SlotRange;

/// Enum describing the relationship between a set and a query.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum CoverageRelation {
    Equals,   // Set == query (exact match)
    Covers,   // Set ⊇ query (covers but has extra)
    NoMatch,  // Set ⊉ query (doesn't cover all)
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
#[derive(Debug, Clone, Default)]
pub(crate) struct SlotsSet {
    /// Vector of slot ranges (start, end), kept sorted and normalized.
    ranges: Vec<SlotRange>,
}

impl SlotsSet {
    /// Creates a new empty `SlotsSet`.
    pub(crate) const fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    // ========================================================================
    // Public API methods required for the C FFI:
    // ========================================================================

    /// Checks if this SlotsSet contains exactly the same ranges as the input.
    ///
    /// Assumes both the internal ranges and input are sorted and normalized.
    pub(crate) fn equals(&self, ranges: &[SlotRange]) -> bool {
        if self.ranges.len() != ranges.len() {
            return false;
        }
        self.ranges.iter().zip(ranges.iter()).all(|(a, b)| a == b)
    }

    /// Replaces the entire contents of this SlotsSet with the given ranges (hard reset).
    ///
    /// This method validates ranges and normalizes them (merges overlapping/adjacent).
    /// Used only by `slots_tracker_set_local_slots`.
    pub(crate) fn set_from_ranges(&mut self, ranges: &[SlotRange]) {
        self.ranges.clear();
        for &range in ranges {
            if range.start > range.end || range.end > 16383 {
                continue; // Skip invalid ranges
            }
            self.ranges.push(range);
        }
        self.normalize();
    }

    /// Adds/merges ranges into the set (union operation).
    ///
    /// This method validates ranges and normalizes them.
    /// Used by `slots_tracker_set_partially_available_slots` and `slots_tracker_set_fully_available_slots`.
    pub(crate) fn add_ranges(&mut self, ranges: &[SlotRange]) {
        for &range in ranges {
            if range.start > range.end || range.end > 16383 {
                continue; // Skip invalid ranges
            }
            self.ranges.push(range);
        }
        self.normalize();
    }

    /// Removes any slots that overlap with the given ranges.
    ///
    /// May split existing ranges or remove them entirely.
    pub(crate) fn remove_ranges(&mut self, ranges: &[SlotRange]) {
        for &range in ranges {
            if range.start > range.end || range.end > 16383 {
                continue; // Skip invalid ranges
            }
            self.remove_range(range.start, range.end);
        }
    }

    /// Checks if any of the given ranges overlap with any ranges in this set.
    ///
    /// Returns true if there is at least one overlapping slot.
    pub(crate) fn has_overlap(&self, ranges: &[SlotRange]) -> bool {
        for &range in ranges {
            if range.start > range.end || range.end > 16383 {
                continue; // Skip invalid ranges
            }
            if self.has_overlap_with_range(range.start, range.end) {
                return true;
            }
        }
        false
    }

    /// Returns true if this set contains no ranges.
    pub(crate) const fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Checks the relationship between the union of this set and another set vs input ranges.
    ///
    /// Returns `CoverageRelation` indicating: `Equals`, `Covers`, or `NoMatch`.
    pub(crate) fn union_relation(&self, other: &SlotsSet, ranges: &[SlotRange]) -> CoverageRelation {
        // Build the union set
        let mut union = self.clone();
        union.add_ranges(&other.ranges);

        // Build a normalized copy of input
        let mut input = SlotsSet::new();
        input.set_from_ranges(ranges);

        // Check for exact match
        if union.equals(&input.ranges) {
            return CoverageRelation::Equals;
        }

        // Check if union covers all input slots
        for &range in &input.ranges {
            if !union.range_is_covered(range.start, range.end) {
                return CoverageRelation::NoMatch;
            }
        }

        // Union covers input but has extra slots
        CoverageRelation::Covers
    }

    // ========================================================================
    // Private helper methods:
    // ========================================================================

    /// Normalizes the internal ranges: sorts and merges overlapping/adjacent ranges.
    fn normalize(&mut self) {
        if self.ranges.is_empty() {
            return;
        }

        // Sort by start position
        self.ranges.sort_by_key(|r| r.start);

        // Merge overlapping and adjacent ranges
        let mut merged = Vec::with_capacity(self.ranges.len());
        let mut current = self.ranges[0];

        for &range in &self.ranges[1..] {
            if current.end + 1 >= range.start {
                // Overlapping or adjacent - merge
                current.end = current.end.max(range.end);
            } else {
                // Not adjacent - push current and start new
                merged.push(current);
                current = range;
            }
        }
        merged.push(current);

        self.ranges = merged;
    }

    /// Removes a single range from the set, potentially splitting existing ranges.
    fn remove_range(&mut self, remove_start: u16, remove_end: u16) {
        let mut new_ranges = Vec::new();

        for &range in &self.ranges {
            // No overlap - keep range as is
            if range.end < remove_start || range.start > remove_end {
                new_ranges.push(range);
                continue;
            }

            // Partial overlap - may need to split
            if range.start < remove_start {
                // Keep left part
                new_ranges.push(SlotRange {
                    start: range.start,
                    end: remove_start - 1,
                });
            }
            if range.end > remove_end {
                // Keep right part
                new_ranges.push(SlotRange {
                    start: remove_end + 1,
                    end: range.end,
                });
            }
            // If completely covered (start >= remove_start && end <= remove_end), skip it
        }

        self.ranges = new_ranges;
    }

    /// Checks if a single range overlaps with any range in this set.
    fn has_overlap_with_range(&self, start: u16, end: u16) -> bool {
        for &range in &self.ranges {
            if start <= range.end && range.start <= end {
                return true;
            }
        }
        false
    }

    /// Checks if a single range is completely covered by this set.
    fn range_is_covered(&self, start: u16, end: u16) -> bool {
        let mut current = start;

        for &range in &self.ranges {
            // Range starts after current position - gap found
            if range.start > current {
                return false;
            }
            // Range covers current position
            // No need to check `range.start <= current` due to previous condition
            if range.end >= current {
                current = range.end + 1;
                if current > end {
                    // Covered everything
                    return true;
                }
            }
        }

        // Didn't cover everything
        false
    }
}
