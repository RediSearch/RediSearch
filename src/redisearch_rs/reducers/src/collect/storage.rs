/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bounded storage shared by the COLLECT reducer variants. Four modes:
//!
//! - [`Storage::Array`] — preserves arrival order under an `offset + count`
//!   cap and drops excess inserts in O(1) without paying any projection
//!   cost. Suitable when ranking is not needed.
//! - [`Storage::Heap`] — retains the top-`(offset + count)` survivors under
//!   a comparator driven by `sort_asc_map`, wrapping the [`MinMaxHeap`]
//!   primitive defined in [`super::heap`] and draining best→worst.
//!   Suitable when a ranked top-K is needed.
//! - [`Storage::DistinctHeap`] — like [`Storage::Heap`] but deduplicates by
//!   projected fields, keeping the best representative per sort key (the
//!   `COLLECT … SORTBY DISTINCT` path).
//! - [`Storage::DistinctArray`] — like [`Storage::Array`] but deduplicates by
//!   projected fields, keeping the first arrival per identity (the
//!   `COLLECT … DISTINCT` path without `SORTBY`).

use std::cmp::Reverse;

use std::collections::HashSet;
use std::marker::PhantomData;

use itertools::Either;
use min_max_heap::MinMaxHeap;
use priority_queue::PriorityQueue;
use rlookup::RLookupRow;
use value::SharedValue;

use super::distinct::DistinctKey;
use super::heap::{EntryKey, HeapEntry};

/// Default count for `SORTBY` results when no explicit `LIMIT` is provided,
/// matching the C implementation's `DEFAULT_LIMIT`.
pub const DEFAULT_LIMIT: u64 = 10;

/// Cap on the *initial* buffer allocation, to keep the up-front cost
/// bounded when `offset + count` is very large. The buffer/heap is still
/// allowed to grow past this — it only governs `with_capacity`, not the
/// number of rows we will retain.
const INITIAL_CAPACITY_CAP: usize = 16_384;

pub enum StorageMode {
    Array,
    Heap,
    DistinctHeap,
    DistinctArray,
}

impl StorageMode {
    pub const fn from_flags(sortby: bool, uses_distinct_storage: bool) -> Self {
        match (uses_distinct_storage, sortby) {
            // DISTINCT keeps the best representative per sort key.
            (true, true) => Self::DistinctHeap,
            // DISTINCT without SORTBY: keep the first arrival per projected
            // identity, in arrival order, under the same cap as `Array`.
            (true, false) => Self::DistinctArray,
            (false, true) => Self::Heap,
            (false, false) => Self::Array,
        }
    }
}

pub enum Storage<D: Ord> {
    Array {
        buf: Vec<RLookupRow<'static>>,
        offset: usize,
        count: usize,
        _marker: PhantomData<D>,
    },
    Heap {
        heap: MinMaxHeap<HeapEntry<D, RLookupRow<'static>>>,
        sort_asc_map: u64,
        offset: usize,
        count: usize,
    },
    DistinctHeap {
        /// Item = [`DistinctKey`] (FNV digest of the projected fields);
        /// priority = `Reverse<HeapEntry<…>>`.
        /// The [`Reverse`] makes the queue *worst-first*: the single end
        /// exposed by [`PriorityQueue::peek`]/[`PriorityQueue::pop`]
        /// is the worst survivor, the one cap eviction removes.
        pq: PriorityQueue<DistinctKey, Reverse<HeapEntry<D, RLookupRow<'static>>>>,
        sort_asc_map: u64,
        offset: usize,
        count: usize,
    },
    DistinctArray {
        seen: HashSet<u64>,
        buf: Vec<RLookupRow<'static>>,
        offset: usize,
        count: usize,
        _marker: PhantomData<D>,
    },
}

impl<D: Ord> Storage<D> {
    /// Resolve `(offset, count)` and pre-size the buffer/heap.
    pub fn new(mode: StorageMode, limit: Option<(u64, u64)>, sort_asc_map: u64) -> Self {
        let sortby = matches!(mode, StorageMode::Heap | StorageMode::DistinctHeap);
        let (offset, count) = match (sortby, limit) {
            (_, Some((o, c))) => (o as usize, c as usize),
            // SAFETY: `ffi::RSGlobalConfig` is the module-global config
            // instance initialised once during module load; we only read
            // a single `usize` field here.
            (false, None) => (0, unsafe { ffi::RSGlobalConfig.maxAggregateResults }),
            (true, None) => (0, DEFAULT_LIMIT as usize),
        };
        let initial_capacity = offset.saturating_add(count).min(INITIAL_CAPACITY_CAP);
        match mode {
            StorageMode::Array => Self::Array {
                buf: Vec::with_capacity(initial_capacity),
                offset,
                count,
                _marker: PhantomData,
            },
            StorageMode::Heap => Self::Heap {
                heap: MinMaxHeap::with_capacity(initial_capacity),
                sort_asc_map,
                offset,
                count,
            },
            StorageMode::DistinctHeap => Self::DistinctHeap {
                pq: PriorityQueue::with_capacity(initial_capacity),
                sort_asc_map,
                offset,
                count,
            },
            StorageMode::DistinctArray => Self::DistinctArray {
                seen: HashSet::with_capacity(initial_capacity),
                buf: Vec::with_capacity(initial_capacity),
                offset,
                count,
                _marker: PhantomData,
            },
        }
    }

    /// Insert an entry under the cap. Two-phase to preserve the
    /// deferred-projection invariant:
    ///
    /// - `sort_vals` is invoked only on the heap path (the array path
    ///   ignores it).
    /// - `project` is invoked only when the entry will actually be
    ///   retained (i.e. on the heap path, only when the candidate beats
    ///   the current worst).
    ///
    /// `doc_id` is only consulted on heap-backed paths, where it acts as a
    /// deterministic tie-breaker when sort keys compare equal. Callers
    /// that don't need tie-breaking instantiate `Storage<()>` and pass
    /// `()` here — `()`'s `Ord` impl makes the fallback a no-op.
    ///
    /// This is the non-DISTINCT entry point ([`Self::Array`] / [`Self::Heap`]).
    /// The DISTINCT path ([`Self::DistinctHeap`]) must call
    /// [`Self::insert_distinct_entry`] instead.
    pub fn insert_entry<S, P>(&mut self, sort_vals: S, doc_id: D, project: P)
    where
        S: FnOnce() -> Box<[Option<SharedValue>]>,
        P: FnOnce() -> RLookupRow<'static>,
    {
        match self {
            Self::Array {
                buf, offset, count, ..
            } => {
                let max_size = offset.saturating_add(*count);
                if buf.len() < max_size {
                    buf.push(project());
                }
            }
            Self::Heap {
                heap,
                sort_asc_map,
                offset,
                count,
            } => {
                let max_size = offset.saturating_add(*count);
                let make_key = |sort_vals: S| EntryKey::new(sort_vals(), *sort_asc_map, doc_id);
                if max_size == 0 {
                    return;
                }
                if heap.len() < max_size {
                    let key = make_key(sort_vals);
                    heap.push(HeapEntry::new(key, project()));
                } else {
                    let cand_key = make_key(sort_vals);
                    // `peek_min` returns the worst surviving candidate
                    // under the "best = max" convention (see `heap`).
                    // The unwrap is sound: `cap > 0` implies the heap is
                    // non-empty once we've reached the cap.
                    let worst = heap.peek_min().expect("heap at cap is non-empty");
                    if cand_key > *worst.key() {
                        heap.push_pop_min(HeapEntry::new(cand_key, project()));
                    }
                }
            }
            Self::DistinctHeap { .. } | Self::DistinctArray { .. } => {
                unreachable!("insert_entry called on DISTINCT storage")
            }
        }
    }

    /// Insert an entry into DISTINCT storage.
    pub fn insert_distinct_entry<S, M>(&mut self, sort_vals: S, doc_id: D, make_entry: M)
    where
        S: FnOnce() -> Box<[Option<SharedValue>]>,
        M: FnOnce() -> (RLookupRow<'static>, u64),
    {
        match self {
            Self::DistinctHeap {
                pq,
                sort_asc_map,
                offset,
                count,
            } => {
                let max_size = offset.saturating_add(*count);
                if max_size == 0 {
                    return;
                }
                let cand_key = EntryKey::new(sort_vals(), *sort_asc_map, doc_id);
                // If the queue is full and the candidate is no better than the
                // current worst, drop it before projecting.
                if pq.len() >= max_size {
                    let worst = pq.peek().expect("pq at cap is non-empty").1;
                    if cand_key <= *worst.0.key() {
                        return;
                    }
                }
                // Survivor: project the row and its dedup identity, then pair
                // the ranking key with the row.
                let (row, digest) = make_entry();
                let item = DistinctKey::new(digest);
                let priority = Reverse(HeapEntry::new(cand_key, row));

                // Dedup-keep-best.
                if pq.push_decrease(item, priority).is_none() && pq.len() > max_size {
                    pq.pop();
                }
            }
            Self::DistinctArray {
                seen,
                buf,
                offset,
                count,
                ..
            } => {
                let max_size = offset.saturating_add(*count);
                // At the cap, every candidate is dropped.
                if buf.len() >= max_size {
                    return;
                }
                // Below the cap we must project to derive the dedup identity.
                // `sort_vals` and `doc_id` are unused: there is no ranking.
                let (row, digest) = make_entry();
                // First arrival wins: store only a digest not seen before.
                if seen.insert(digest) {
                    buf.push(row);
                }
            }
            Self::Array { .. } | Self::Heap { .. } => {
                unreachable!("insert_distinct_entry called on non-DISTINCT storage")
            }
        }
    }

    /// Drain buffered rows.
    ///
    /// - **Array path** yields rows in insertion order.
    /// - **Heap path** yields rows best→worst (matching the SORTBY result
    ///   order).
    pub fn drain(&mut self) -> impl ExactSizeIterator<Item = RLookupRow<'static>> {
        // Sentinel left in place of `*self`. Empty Array, no allocation.
        let taken = std::mem::replace(
            self,
            Self::Array {
                buf: Vec::new(),
                offset: 0,
                count: 0,
                _marker: PhantomData,
            },
        );
        match taken {
            Self::Array {
                buf, offset, count, ..
            }
            | Self::DistinctArray {
                buf, offset, count, ..
            } => Either::Left(buf.into_iter().skip(offset).take(count)),
            Self::Heap {
                heap,
                offset,
                count,
                ..
            } => Either::Right(
                heap.into_vec_desc()
                    .into_iter()
                    .skip(offset)
                    .take(count)
                    .map(HeapEntry::into_projected),
            ),
            Self::DistinctHeap {
                pq, offset, count, ..
            } => {
                // The queue is worst-first, so `into_sorted_iter` yields
                // worst→best; reverse into a `Vec` so the final order is
                // best→worst (matching the heap path). Shares the
                // `Vec<RLookupRow>` iterator shape with the array path.
                let mut rows: Vec<RLookupRow<'static>> = pq
                    .into_sorted_iter()
                    .map(|(_key, priority)| priority.0.into_projected())
                    .collect();
                rows.reverse();
                Either::Left(rows.into_iter().skip(offset).take(count))
            }
        }
    }
}
