/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bounded storage shared by the COLLECT reducer variants, split by the
//! `SORTBY` axis into two family types: [`ArrayStorage`] for the unranked path
//! and [`HeapStorage`] for the ranked `COLLECT … SORTBY [LIMIT]` path.
//!
//! [`Storage`] is a thin enum that selects one family; the reducer dispatches
//! array-vs-heap once, in its `add` method, so only the heap path ever builds a
//! sort-key snapshot.

use itertools::Either;
use min_max_heap::MinMaxHeap;
use rlookup::RLookupRow;
use value::SharedValue;

use super::heap::{HeapEntry, RankingKey};

/// Default count for `SORTBY` results when no explicit `LIMIT` is provided,
/// matching the C implementation's `DEFAULT_LIMIT`.
pub const DEFAULT_LIMIT: u64 = 10;

/// Cap on the *initial* buffer allocation, to keep the up-front cost
/// bounded when `offset + count` is very large. The buffer/heap is still
/// allowed to grow past this — it only governs `with_capacity`, not the
/// number of rows we will retain.
const INITIAL_CAPACITY_CAP: usize = 16_384;

/// The *value* held by the storage: the projected (collected) fields, kept in a
/// type distinct from [`RankingKey`] so that order (the ranking key) and content
/// (this row) stay explicit.
pub struct ProjectedRow(RLookupRow<'static>);

impl ProjectedRow {
    /// Wrap a freshly projected row.
    pub const fn new(row: RLookupRow<'static>) -> Self {
        Self(row)
    }

    /// Borrow the fields, e.g. to serialize the output map.
    pub const fn row(&self) -> &RLookupRow<'static> {
        &self.0
    }

    /// Consume the wrapper, returning the inner row.
    pub fn into_row(self) -> RLookupRow<'static> {
        self.0
    }
}

/// Bounded COLLECT storage, selecting one family type by the `SORTBY` axis.
///
/// `D` is the doc-id tie-breaker carried by [`HeapStorage`]; the array path
/// never ranks, so [`ArrayStorage`] is free of it.
pub enum Storage<D: Ord> {
    Array(ArrayStorage),
    Heap(HeapStorage<D>),
}

impl<D: Ord> Storage<D> {
    /// Select the family by `sortby` and resolve `(offset, count)`.
    ///
    /// Without an explicit `LIMIT`, the array path falls back to the global
    /// `maxAggregateResults` and the heap path to [`DEFAULT_LIMIT`].
    pub fn new(sortby: bool, limit: Option<(u64, u64)>, sort_asc_map: u64) -> Self {
        let (offset, count) = match (sortby, limit) {
            (_, Some((o, c))) => (o as usize, c as usize),
            // SAFETY: `ffi::RSGlobalConfig` is the module-global config
            // instance initialised once during module load; we only read
            // a single `usize` field here.
            (false, None) => (0, unsafe { ffi::RSGlobalConfig.maxAggregateResults }),
            (true, None) => (0, DEFAULT_LIMIT as usize),
        };
        if sortby {
            Self::Heap(HeapStorage::new(sort_asc_map, offset, count))
        } else {
            Self::Array(ArrayStorage::new(offset, count))
        }
    }

    /// Drain the retained values, sliced as `skip(offset).take(count)`: the array
    /// arm in insertion order, the heap arm best→worst.
    ///
    /// The heap arm discards each entry's [`RankingKey`], which suits the
    /// client-facing local reducer. The remote reducer, which re-emits the sort
    /// columns on the shard path, drains [`HeapStorage`] directly instead.
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = ProjectedRow> {
        match self {
            Self::Array(a) => Either::Left(a.drain()),
            Self::Heap(h) => Either::Right(h.drain().map(HeapEntry::into_projected)),
        }
    }
}

/// Arrival-ordered storage for the non-`SORTBY` path.
///
/// Holds at most `offset + count` [`ProjectedRow`]s and drops the rest without
/// projecting them. Has no sort context at all.
pub struct ArrayStorage {
    buf: Vec<ProjectedRow>,
    offset: usize,
    count: usize,
}

impl ArrayStorage {
    fn new(offset: usize, count: usize) -> Self {
        let initial_capacity = offset.saturating_add(count).min(INITIAL_CAPACITY_CAP);
        Self {
            buf: Vec::with_capacity(initial_capacity),
            offset,
            count,
        }
    }

    /// Buffer one more row, dropping it once the `offset + count` cap is
    /// reached. `project` runs only for a retained row, so a dropped row pays
    /// no projection cost.
    pub fn push(&mut self, project: impl FnOnce() -> ProjectedRow) {
        if self.buf.len() < self.offset.saturating_add(self.count) {
            self.buf.push(project());
        }
    }

    /// Drain the buffered rows in insertion order, sliced as
    /// `skip(offset).take(count)`.
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = ProjectedRow> {
        std::mem::take(&mut self.buf)
            .into_iter()
            .skip(self.offset)
            .take(self.count)
    }
}

/// Top-K storage for the `SORTBY` path.
///
/// Keeps the best `offset + count` candidates under the [`RankingKey`]
/// comparator, draining best→worst. Unlike [`ArrayStorage`], it owns the sort
/// context: each candidate's sort-key snapshot lives in its [`RankingKey`] (which
/// doubles as the cap-check key), so the paired [`ProjectedRow`] never has to
/// carry it.
pub struct HeapStorage<D: Ord> {
    heap: MinMaxHeap<HeapEntry<D, ProjectedRow>>,
    sort_asc_map: u64,
    offset: usize,
    count: usize,
}

impl<D: Ord> HeapStorage<D> {
    fn new(sort_asc_map: u64, offset: usize, count: usize) -> Self {
        let initial_capacity = offset.saturating_add(count).min(INITIAL_CAPACITY_CAP);
        Self {
            heap: MinMaxHeap::with_capacity(initial_capacity),
            sort_asc_map,
            offset,
            count,
        }
    }

    /// Offer a candidate to the bounded top-K.
    ///
    /// `sort_vals` is the candidate's sort-key snapshot; it forms the ranking
    /// [`RankingKey`] and is compared against the current worst survivor *before*
    /// `project` runs, so `project` is invoked only for a candidate that is
    /// actually retained. `doc_id` breaks ties when the sort keys compare equal
    /// (see [`RankingKey`]).
    pub fn consider(
        &mut self,
        sort_vals: Box<[Option<SharedValue>]>,
        doc_id: D,
        project: impl FnOnce() -> ProjectedRow,
    ) {
        let max_size = self.offset.saturating_add(self.count);
        // `LIMIT count` is parse-validated `>= 1`, so a zero cap never reaches here.
        debug_assert!(max_size > 0, "heap storage built with a zero cap");
        let cand_key = RankingKey::new(sort_vals, self.sort_asc_map, doc_id);
        if self.heap.len() < max_size {
            self.heap.push(HeapEntry::new(cand_key, project()));
        } else {
            // `peek_min` is the worst survivor under the "best = max"
            // convention (see [`super::heap`]); a full heap is non-empty per the
            // `max_size > 0` assertion above, so the unwrap holds.
            let worst = self.heap.peek_min().expect("heap at cap is non-empty");
            if cand_key > *worst.key() {
                self.heap.push_pop_min(HeapEntry::new(cand_key, project()));
            }
        }
    }

    /// Drain the retained entries best→worst, sliced as `skip(offset).take(count)`.
    ///
    /// Yields whole [`HeapEntry`]s, not just the [`ProjectedRow`], so the remote
    /// reducer can read each [`RankingKey`] to rebuild the wire row.
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = HeapEntry<D, ProjectedRow>> {
        std::mem::take(&mut self.heap)
            .into_vec_desc()
            .into_iter()
            .skip(self.offset)
            .take(self.count)
    }
}
