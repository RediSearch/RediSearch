/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A slots tracker implementation.
//! This module provides a way to track and manage slots in a system.

mod slot_set;
mod slots_tracker;

pub use slots_tracker::{SLOTS_TRACKER_UNAVAILABLE, SlotsTracker};

// ============================================================================
// C FFI Interface - Public API
// ============================================================================

/// C-compatible slot range array structure.
///
/// This is a variable-length structure with a flexible array member.
#[repr(C)]
pub struct SlotRangeArray {
    pub num_ranges: i32,
    pub ranges: [SlotRange; 0], // Flexible array member
}

/// Represents a contiguous range of slots.
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
}
