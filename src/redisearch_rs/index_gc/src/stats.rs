/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Statistics accumulated by a fork-GC scanner while applying collected deltas.

use std::{
    ptr::NonNull,
    sync::atomic::{AtomicIsize, AtomicU64, Ordering},
};

use index_spec::IndexSpecWriteGuard;

use crate::ForkPipe;

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
    /// Bytes occupied by removed term keys to subtract from the spec statistics.
    pub terms_size_removed: usize,
    /// Number of removed terms to subtract from the spec statistics.
    pub terms_removed: usize,
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
        self.terms_size_removed += other.terms_size_removed;
        self.terms_removed += other.terms_removed;
    }

    /// Apply this delta to both the spec-level and GC-level statistics.
    ///
    /// Combines the spec stats update (done under the write lock) and the GC
    /// stats update that always go together after applying a GC delta.
    pub fn apply(&self, fgc: &mut ForkPipe, guard: &mut IndexSpecWriteGuard<'_>) {
        self.apply_to_spec(guard);
        fgc.record(self);
    }

    /// Apply the spec-level half of this delta, under the spec's write lock.
    pub fn apply_to_spec(&self, guard: &mut IndexSpecWriteGuard<'_>) {
        guard.update_gc_stats(
            self.records_removed,
            self.bytes_collected,
            self.bytes_allocated,
            self.terms_size_removed,
            self.terms_removed,
        );
        guard.add_block_count(self.block_count_delta);
    }
}

/// Receives the GC-level half of each [`GcApplyStats`] a cycle produces.
///
/// The spec-level half goes through [`GcApplyStats::apply_to_spec`] under the
/// write lock; this half only touches the GC's own counters.
pub trait GcStatsSink {
    /// Fold `stats` into the GC's counters.
    fn record(&mut self, stats: &GcApplyStats);
}

impl GcStatsSink for ForkPipe {
    fn record(&mut self, stats: &GcApplyStats) {
        // SAFETY: the pipe's stats are valid for as long as it is borrowed here.
        unsafe { GcCounters::new(self.stats_ptr()) }.record(stats);
    }
}

/// A GC context's counters, updated atomically and through a raw pointer: the main
/// thread reads them for `FT.INFO` while an in-process cycle updates them, so no
/// reference to them may be held across a cycle.
pub struct GcCounters(NonNull<ffi::IndexGCStats>);

impl GcCounters {
    /// # Safety
    ///
    /// `stats` points to a GC context's counters, valid for as long as the result
    /// lives, which C reads and writes only atomically.
    pub const unsafe fn new(stats: *mut ffi::IndexGCStats) -> Self {
        // SAFETY: the caller guarantees `stats` is valid, so not null.
        Self(unsafe { NonNull::new_unchecked(stats) })
    }
}

impl GcStatsSink for GcCounters {
    /// `numeric_nodes_missed` is a count rather than a single increment because the
    /// numeric scanner tallies misses across a whole node stream and flushes them
    /// together with the rest of the field's counters.
    fn record(&mut self, stats: &GcApplyStats) {
        let counters = self.0.as_ptr();
        let collected = stats.bytes_collected as isize - stats.bytes_allocated as isize;
        // SAFETY: (`new`) `counters` is valid; this only takes a field's address.
        let total_collected = unsafe { &raw mut (*counters).totalCollected };
        // SAFETY: as above.
        let blocks_denied = unsafe { &raw mut (*counters).gcBlocksDenied };
        // SAFETY: as above.
        let nodes_missed = unsafe { &raw mut (*counters).gcNumericNodesMissed };
        // SAFETY: (`new`) the fields are valid and only accessed atomically.
        unsafe { AtomicIsize::from_ptr(total_collected) }.fetch_add(collected, Ordering::Relaxed);
        // SAFETY: as above.
        unsafe { AtomicU64::from_ptr(blocks_denied) }
            .fetch_add(stats.blocks_denied, Ordering::Relaxed);
        // SAFETY: as above.
        unsafe { AtomicU64::from_ptr(nodes_missed) }
            .fetch_add(stats.numeric_nodes_missed, Ordering::Relaxed);
    }
}
