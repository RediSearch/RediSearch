/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bounded storage shared by the COLLECT reducer variants, split by the
//! `SORTBY` axis into two family types:
//!
//! - [`ArrayStorage`] — preserves arrival order under an `offset + count` cap
//!   and drops excess inserts in O(1) without paying any projection cost. Used
//!   when ranking is not needed (no `SORTBY`).
//! - [`HeapStorage`] — retains the top-`(offset + count)` survivors under a
//!   comparator driven by `sort_asc_map`, wrapping the [`MinMaxHeap`] primitive
//!   from [`super::heap`] and draining best→worst. Used for the ranked
//!   `COLLECT … SORTBY [LIMIT]` path.
//!
//! [`Storage`] is a thin enum that selects one family; the reducer dispatches
//! array-vs-heap once, in its `add` method, so only the heap path ever builds a
//! sort-key snapshot.

use itertools::Either;
use min_max_heap::MinMaxHeap;
use rlookup::RLookupRow;
use value::SharedValue;

use super::heap::{EntryKey, HeapEntry};

/// Default count for `SORTBY` results when no explicit `LIMIT` is provided,
/// matching the C implementation's `DEFAULT_LIMIT`.
pub const DEFAULT_LIMIT: u64 = 10;

/// Cap on the *initial* buffer allocation, to keep the up-front cost
/// bounded when `offset + count` is very large. The buffer/heap is still
/// allowed to grow past this — it only governs `with_capacity`, not the
/// number of rows we will retain.
const INITIAL_CAPACITY_CAP: usize = 16_384;

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

    /// Drain the retained rows, sliced as `skip(offset).take(count)`.
    ///
    /// - **Array arm** yields rows in insertion order.
    /// - **Heap arm** yields rows best→worst (matching the SORTBY result order).
    ///
    /// The heap arm discards each entry's sort-key snapshot
    /// ([`HeapEntry::into_projected`]) because the client-facing local reducer
    /// never re-emits the sort columns. The remote reducer, which *does* re-emit
    /// them on the shard path, drains [`HeapStorage`] directly instead — see
    /// [`RemoteCollectCtx::finalize`][super::remote::RemoteCollectCtx::finalize].
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = RLookupRow<'static>> {
        match self {
            Self::Array(a) => Either::Left(a.drain()),
            Self::Heap(h) => Either::Right(h.drain().map(HeapEntry::into_projected)),
        }
    }
}

/// Arrival-ordered storage for the non-`SORTBY` path.
///
/// Holds at most `offset + count` rows and drops the rest without projecting
/// them. Has no sort context at all.
pub struct ArrayStorage {
    buf: Vec<RLookupRow<'static>>,
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
    pub fn push(&mut self, project: impl FnOnce() -> RLookupRow<'static>) {
        if self.buf.len() < self.offset.saturating_add(self.count) {
            self.buf.push(project());
        }
    }

    /// Drain the buffered rows in insertion order, sliced as
    /// `skip(offset).take(count)`.
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = RLookupRow<'static>> {
        std::mem::take(&mut self.buf)
            .into_iter()
            .skip(self.offset)
            .take(self.count)
    }
}

/// Top-K storage for the `SORTBY` path.
///
/// Keeps the best `offset + count` candidates under the [`EntryKey`]
/// comparator, draining best→worst. Unlike [`ArrayStorage`], it owns the sort
/// context: each candidate's sort-key snapshot lives in its [`EntryKey`] (which
/// doubles as the cap-check key), so the projected row never has to carry it.
pub struct HeapStorage<D: Ord> {
    heap: MinMaxHeap<HeapEntry<D, RLookupRow<'static>>>,
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
    /// [`EntryKey`] and is compared against the current worst survivor *before*
    /// `project` runs, so `project` is invoked only for a candidate that is
    /// actually retained. `doc_id` breaks ties when the sort keys compare equal
    /// (see [`EntryKey`]).
    pub fn consider(
        &mut self,
        sort_vals: Box<[Option<SharedValue>]>,
        doc_id: D,
        project: impl FnOnce() -> RLookupRow<'static>,
    ) {
        let max_size = self.offset.saturating_add(self.count);
        if max_size == 0 {
            return;
        }
        let cand_key = EntryKey::new(sort_vals, self.sort_asc_map, doc_id);
        if self.heap.len() < max_size {
            self.heap.push(HeapEntry::new(cand_key, project()));
        } else {
            // `peek_min` is the worst survivor under the "best = max"
            // convention (see [`super::heap`]); the unwrap is sound because a
            // full heap with `max_size > 0` is non-empty.
            let worst = self.heap.peek_min().expect("heap at cap is non-empty");
            if cand_key > *worst.key() {
                self.heap.push_pop_min(HeapEntry::new(cand_key, project()));
            }
        }
    }

    /// Drain the retained entries best→worst, sliced as
    /// `skip(offset).take(count)`.
    ///
    /// Each [`HeapEntry`] still pairs the row with its ranking [`EntryKey`]:
    /// callers that only need rows map with [`HeapEntry::into_projected`], while
    /// the remote reducer reads [`EntryKey::sort_vals`] (via
    /// [`HeapEntry::into_parts`]) to rebuild the wire row.
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = HeapEntry<D, RLookupRow<'static>>> {
        std::mem::take(&mut self.heap)
            .into_vec_desc()
            .into_iter()
            .skip(self.offset)
            .take(self.count)
    }
}
