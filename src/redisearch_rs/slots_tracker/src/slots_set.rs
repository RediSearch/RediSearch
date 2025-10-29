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

/// Represents a range of slots with start and end values (inclusive).
pub(crate) type SlotRange = (u16, u16);

/// A collection of slot ranges with set operation capabilities.
///
/// This is an internal type used only within this crate to manage slot sets.
/// Each range is inclusive [start, end].
#[derive(Debug, Clone, Default)]
pub(crate) struct SlotsSet {
    /// Vector of slot ranges, where each range is (start, end) inclusive.
    ranges: Vec<SlotRange>,
}

impl SlotsSet {
    /// Creates a new empty `SlotsSet`.
    pub(crate) const fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    // ========================================================================
    // Required methods for slots_tracker_set_local_slots API:
    // ========================================================================
    
    // TODO: pub(crate) fn equals(&self, ranges: &[super::SlotRange]) -> bool
    //   - Compare if this SlotsSet contains the exact same ranges as the input
    //   - Assumes both are sorted
    //   - Return true if identical, false otherwise
    
    // TODO: pub(crate) fn set_from_ranges(&mut self, ranges: &[super::SlotRange])
    //   - Replace the entire contents of this SlotsSet with the given ranges
    //   - Should validate ranges (start <= end, values in [0, 16383])
    //   - Should normalize/merge overlapping and adjacent ranges
    //   - Assumes input is sorted
    
    // TODO: pub(crate) fn remove_ranges(&mut self, ranges: &[super::SlotRange])
    //   - Remove any slots that overlap with the given ranges
    //   - For each range [start, end] in input:
    //     - Remove all slots in [start, end] from this set
    //     - This may split existing ranges or remove them entirely
    //   - Example: if self has [50-150] and input has [100-200],
    //     result should be [50-99]
    
    // ========================================================================
    // Additional helper methods:
    // ========================================================================
    
    // TODO: Implement internal methods:
    // - insert_range(start, end) - add a single range
    // - contains(slot) - check if a slot is in any range
    // - clear() - remove all ranges
    // - merge/normalize overlapping and adjacent ranges
    // - validate range (start <= end, within 0-16383)
    // - binary search for range lookup
    // - union with another SlotsSet
    // - intersection with another SlotsSet
    // - difference with another SlotsSet
    // - iterate over ranges
}
