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

    // TODO: Implement internal methods:
    // - insert/add range
    // - remove range
    // - union with another SlotsSet
    // - intersection with another SlotsSet
    // - difference with another SlotsSet
    // - contains check (whether a slot or range is in the set)
    // - clear all ranges
    // - merge overlapping/adjacent ranges (normalization)
    // - iterate over ranges
    // - validate ranges (start <= end, within 0-16383)
    // - binary search for range lookup
}
