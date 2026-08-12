/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Statistics accumulated by a fork-GC scanner while applying collected deltas.

use index_spec::IndexSpecWriteGuard;

use crate::ForkGC;

/// Book-keeping produced by applying garbage-collection deltas to an inverted
/// index.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GcApplyStats {
    /// Records removed from the inverted index. Decrements the spec's `numRecords`.
    pub records_removed: usize,
    /// Bytes freed by this GC pass. Lowers the spec's `invertedSize` and raises
    /// the GC's `totalCollected`.
    pub bytes_collected: usize,
    /// Bytes allocated during compaction (new block overhead). Raises the spec's
    /// `invertedSize` and lowers the GC's `totalCollected`.
    pub bytes_allocated: usize,
    /// Net change to the inverted-index block count. Applied to the spec's
    /// `totalInvertedIndexBlocks`.
    pub block_count_delta: i64,
    /// Number of last blocks skipped to avoid data races. Increments the GC's
    /// `gcBlocksDenied`.
    pub blocks_denied: u64,
    /// Numeric tree nodes that had vanished by the time the parent tried to
    /// apply their delta. Increments the GC's `gcNumericNodesMissed`.
    pub numeric_nodes_missed: u64,
}

impl GcApplyStats {
    /// Fold `other` into this tally.
    ///
    /// Scanners that apply many deltas under separate write locks accumulate
    /// the per-delta results here, then flush the total once.
    pub const fn record(&mut self, other: GcApplyStats) {
        self.records_removed += other.records_removed;
        self.bytes_collected += other.bytes_collected;
        self.bytes_allocated += other.bytes_allocated;
        self.block_count_delta += other.block_count_delta;
        self.blocks_denied += other.blocks_denied;
        self.numeric_nodes_missed += other.numeric_nodes_missed;
    }

    /// Apply this delta to both the spec-level and GC-level statistics.
    ///
    /// Combines the spec stats update (done under the write lock) and the GC
    /// stats update that always go together after applying a GC delta.
    pub fn apply(&self, fgc: &mut ForkGC, guard: &mut IndexSpecWriteGuard<'_>) {
        guard.update_gc_stats(
            self.records_removed,
            self.bytes_collected,
            self.bytes_allocated,
        );
        guard.add_block_count(self.block_count_delta);
        fgc.update_gc_stats(
            self.bytes_collected,
            self.bytes_allocated,
            self.blocks_denied,
            self.numeric_nodes_missed,
        );
    }
}
