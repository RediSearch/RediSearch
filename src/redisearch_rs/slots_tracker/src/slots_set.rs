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

// ============================================================================
// Debug assertion helpers
// ============================================================================

/// Validates that all ranges are valid.
#[inline]
fn debug_assert_valid_ranges(ranges: &[SlotRange]) {
    debug_assert!(
        ranges.iter().all(|r| r.start <= r.end && r.end <= 16383),
        "All ranges must be valid (start <= end, end <= 16383)"
    );
}

/// Validates that ranges are sorted and normalized (no overlaps or adjacent ranges).
#[inline]
fn debug_assert_normalized_ranges(ranges: &[SlotRange]) {
    debug_assert!(
        ranges.windows(2).all(|w| w[0].end + 1 < w[1].start),
        "Ranges must be sorted and normalized (no overlaps or adjacent ranges)"
    );
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
    /// **Assumes input is already sorted and normalized** (no overlaps, no adjacent ranges).
    /// In debug builds, validates this assumption.
    /// Used only by `slots_tracker_set_local_slots`.
    pub(crate) fn set_from_ranges(&mut self, ranges: &[SlotRange]) {
        debug_assert_valid_normalized_input(ranges);
        
        // Input is already normalized, just copy it
        self.ranges.clear();
        self.ranges.extend_from_slice(ranges);
    }

    /// Adds/merges ranges into the set (union operation).
    ///
    /// **Assumes input is already sorted and normalized**.
    /// In debug builds, validates this assumption.
    /// Used by `slots_tracker_set_partially_available_slots` and `slots_tracker_set_fully_available_slots`.
    pub(crate) fn add_ranges(&mut self, ranges: &[SlotRange]) {
        debug_assert_valid_normalized_input(ranges);
        
        // Simply add all ranges and normalize once at the end
        self.ranges.extend_from_slice(ranges);
        self.normalize();
    }

    /// Removes any slots that overlap with the given ranges.
    ///
    /// **Assumes input is already sorted and normalized**.
    /// In debug builds, validates this assumption.
    /// May split existing ranges or remove them entirely.
    /// 
    /// **Optimized**: Takes advantage of sorted input to avoid re-scanning from the beginning.
    pub(crate) fn remove_ranges(&mut self, ranges: &[SlotRange]) {
        debug_assert_valid_normalized_input(ranges);
        
        if ranges.is_empty() {
            return;
        }
        
        let old_ranges = std::mem::take(&mut self.ranges);
        let mut remove_iter = ranges.iter().peekable();
        
        for mut current in old_ranges {
            // Skip remove ranges that end before current starts
            while remove_iter.peek().is_some_and(|&&r| r.end < current.start) {
                remove_iter.next();
            }
            
            // Apply all overlapping remove ranges to current
            while let Some(&&remove) = remove_iter.peek() {
                if remove.start > current.end {
                    break; // No more overlaps for current
                }
                
                // Handle overlap cases
                match (remove.start <= current.start, remove.end >= current.end) {
                    (true, true) => {
                        // Complete overlap - discard current
                        remove_iter.next();
                        continue; // Skip to next our_range
                    }
                    (true, false) => {
                        // Remove overlaps left side - trim left
                        current.start = remove.end + 1;
                        remove_iter.next();
                    }
                    (false, true) => {
                        // Remove overlaps right side - trim right and done
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
    /// **Assumes input is already sorted and normalized**.
    /// In debug builds, validates this assumption.
    /// Returns true if there is at least one overlapping slot.
    /// 
    /// **Optimized**: Uses iterators with two-pointer technique since both inputs are sorted.
    pub(crate) fn has_overlap(&self, ranges: &[SlotRange]) -> bool {
        debug_assert_valid_normalized_input(ranges);
        
        let mut our_iter = self.ranges.iter();
        let mut their_iter = ranges.iter().peekable();
        
        for &our_range in our_iter.by_ref() {
            // Skip their ranges that end before our current range starts
            while their_iter.peek().is_some_and(|&&r| r.end < our_range.start) {
                their_iter.next();
            }
            
            // Check if next their range overlaps with our current range
            if let Some(&&their_range) = their_iter.peek() {
                if their_range.start <= our_range.end {
                    return true; // Overlap found
                }
            } else {
                break; // No more their ranges to check
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
