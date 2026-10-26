/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Checked Redis slot intervals shared by storage and migration code.

use crate::SlotRange;

/// Number of Redis Cluster hash slots.
pub const CLUSTER_SLOTS: u16 = 16384;

/// A [`SlotRange`] checked against [`CLUSTER_SLOTS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedSlotRange(SlotRange);

/// Invalid bounds supplied to [`ValidatedSlotRange::new`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidSlotRange {
    pub start: u16,
    pub end: u16,
}

impl std::fmt::Display for InvalidSlotRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid cluster slot interval {}..={}",
            self.start, self.end
        )
    }
}

impl std::error::Error for InvalidSlotRange {}

impl ValidatedSlotRange {
    /// Rejects inverted intervals and bounds outside [`CLUSTER_SLOTS`].
    pub const fn new(start: u16, end: u16) -> Result<Self, InvalidSlotRange> {
        if start > end || end >= CLUSTER_SLOTS {
            return Err(InvalidSlotRange { start, end });
        }
        Ok(Self(SlotRange { start, end }))
    }

    /// First included slot.
    pub const fn start(self) -> u16 {
        self.0.start
    }

    /// Last included slot.
    pub const fn end(self) -> u16 {
        self.0.end
    }

    /// Included slots in ascending order.
    pub const fn slots(self) -> std::ops::RangeInclusive<u16> {
        self.0.start..=self.0.end
    }

    /// Borrows the checked C-compatible [`SlotRange`].
    pub const fn as_raw(&self) -> &SlotRange {
        &self.0
    }

    /// Sorts intervals and merges overlaps and adjacent intervals.
    pub fn normalize(ranges: &[Self]) -> Vec<Self> {
        let mut sorted = ranges.to_vec();
        sorted.sort_unstable_by_key(|range| (range.start(), range.end()));
        let mut result: Vec<Self> = Vec::with_capacity(sorted.len());
        for range in sorted {
            if let Some(last) = result.last_mut()
                && range.start() <= last.end() + 1
            {
                last.0.end = last.end().max(range.end());
            } else {
                result.push(range);
            }
        }
        result
    }
}

impl TryFrom<SlotRange> for ValidatedSlotRange {
    type Error = InvalidSlotRange;

    fn try_from(range: SlotRange) -> Result<Self, Self::Error> {
        Self::new(range.start, range.end)
    }
}
