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
//!
//! **DISTINCT spike:** [`HeapStorage`] is backed by a [`PriorityQueue`] whose
//! *key is the [`ProjectedRow`] itself* and whose *priority is the
//! [`RankingKey`]*. The row is therefore literally the dedup identity — the
//! queue dedups via [`ProjectedRow`]'s (naive) [`Hash`]/[`Eq`], and
//! `push_increase` keeps the best-ranked representative per identity. This is a
//! prototype to feel out `COLLECT … DISTINCT`; it makes the heap path *always*
//! deduplicate and drops the deferred-projection optimization. The one deferred
//! design decision is how to hash/compare a [`ProjectedRow`] — see its
//! [`Hash`]/[`Eq`] impls.

use std::hash::{Hash, Hasher};

use itertools::Either;
use priority_queue::PriorityQueue;
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

/// The *value* held by the storage: the projected (collected) fields, in a type
/// distinct from the [`RankingKey`] so the two storage axes stay explicit. The
/// ranking key decides order; the `ProjectedRow` is the content — hence the unit
/// any content-based identity (such as the DISTINCT dedup) keys on.
///
/// Naming the role does not prove the row holds *only* projected fields; that is
/// upheld by the `project` closure each reducer passes to [`ArrayStorage::push`]
/// / [`HeapStorage::consider`].
///
/// **DISTINCT spike:** its [`Hash`]/[`Eq`] impls *are* the dedup identity used as
/// the [`HeapStorage`] queue key. See [`Self::identity_repr`] for the naive
/// (placeholder) identity and the one deferred design decision.
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

    /// The naive content identity for the DISTINCT spike: the row's `Debug`
    /// rendering, used by both [`Hash`] and [`Eq`] so they stay consistent.
    ///
    /// TODO(distinct): replace with a field-aware identity. This is the single
    /// deferred decision of the spike; the `Debug` string is good enough to feel
    /// out the mechanism but is neither stable nor a real content comparison.
    fn identity_repr(&self) -> String {
        format!("{:?}", self.0)
    }
}

impl PartialEq for ProjectedRow {
    fn eq(&self, other: &Self) -> bool {
        self.identity_repr() == other.identity_repr()
    }
}

impl Eq for ProjectedRow {}

impl Hash for ProjectedRow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity_repr().hash(state);
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

    /// Drain the retained values, sliced as `skip(offset).take(count)`.
    ///
    /// - **Array arm** yields values in insertion order.
    /// - **Heap arm** yields values best→worst (matching the SORTBY result order).
    ///
    /// The heap arm discards each entry's [`RankingKey`]
    /// ([`HeapEntry::into_projected`]) because the client-facing local reducer
    /// never re-emits the sort columns. The remote reducer, which *does* re-emit
    /// them on the shard path, drains [`HeapStorage`] directly instead — see
    /// [`RemoteCollectCtx::finalize`][super::remote::RemoteCollectCtx::finalize].
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
/// **DISTINCT spike:** backed by a [`PriorityQueue`] whose key is the
/// [`ProjectedRow`] (so identical rows collapse via its [`Hash`]/[`Eq`]) and
/// whose priority is the [`RankingKey`]. [`PriorityQueue::push_increase`] keeps
/// the best-ranked representative per identity. The `offset + count` cap is
/// applied at [`Self::drain`], and projection is eager (the spike trades away the
/// deferred-projection optimization to materialise each row's identity).
pub struct HeapStorage<D: Ord> {
    pq: PriorityQueue<ProjectedRow, RankingKey<D>>,
    sort_asc_map: u64,
    offset: usize,
    count: usize,
}

impl<D: Ord> HeapStorage<D> {
    fn new(sort_asc_map: u64, offset: usize, count: usize) -> Self {
        let initial_capacity = offset.saturating_add(count).min(INITIAL_CAPACITY_CAP);
        Self {
            pq: PriorityQueue::with_capacity(initial_capacity),
            sort_asc_map,
            offset,
            count,
        }
    }

    /// Offer a candidate, deduplicating by the [`ProjectedRow`] identity and
    /// keeping the best-ranked representative per identity.
    ///
    /// Projects eagerly (the spike needs the row to key the queue), builds the
    /// ranking [`RankingKey`] from `sort_vals` / `doc_id`, and pushes
    /// `(row, ranking_key)`: [`PriorityQueue::push_increase`] retains the higher
    /// [`RankingKey`] on a collision (the queue dedups by the row's
    /// [`Hash`]/[`Eq`]).
    pub fn consider(
        &mut self,
        sort_vals: Box<[Option<SharedValue>]>,
        doc_id: D,
        project: impl FnOnce() -> ProjectedRow,
    ) {
        let ranking_key = RankingKey::new(sort_vals, self.sort_asc_map, doc_id);
        self.pq.push_increase(project(), ranking_key);
    }

    /// Drain the retained entries best→worst, sliced as
    /// `skip(offset).take(count)` (the cap is applied here, not on insert).
    ///
    /// Re-pairs each `(row, ranking_key)` into a [`HeapEntry`] so callers are
    /// unchanged: the local reducer maps [`HeapEntry::into_projected`], while the
    /// remote reducer reads [`RankingKey::sort_vals`] (via
    /// [`HeapEntry::into_parts`]) to rebuild the wire row.
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = HeapEntry<D, ProjectedRow>> {
        // `into_sorted_iter` yields highest-priority (best) first; collect into a
        // `Vec` so the result is an `ExactSizeIterator`, like the array path.
        let best_first: Vec<HeapEntry<D, ProjectedRow>> = std::mem::take(&mut self.pq)
            .into_sorted_iter()
            .map(|(row, ranking_key)| HeapEntry::new(ranking_key, row))
            .collect();
        best_first.into_iter().skip(self.offset).take(self.count)
    }
}
