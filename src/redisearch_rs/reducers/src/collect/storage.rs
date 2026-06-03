/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bounded storage shared by the COLLECT reducer variants. Two modes:
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
//!   `COLLECT … SORTBY DISTINCT` path). Backed by a `DoublePriorityQueue`
//!   keyed on [`DistinctRow`]; the hash index collapses duplicates in O(1) and,
//!   on a strictly-better duplicate, the winning row *and* its priority replace
//!   the incumbent (a plain `push_increase` would keep the loser's row — see
//!   `insert_entry_with_dedup`).

use std::cmp::Ordering;

use std::marker::PhantomData;

use itertools::Either;
use min_max_heap::MinMaxHeap;
use priority_queue::DoublePriorityQueue;
use rlookup::RLookupRow;
use value::SharedValue;

use super::distinct::DistinctRow;
use super::heap::{EntryKey, HeapEntry};

/// Default count for `SORTBY` results when no explicit `LIMIT` is provided,
/// matching the C implementation's `DEFAULT_LIMIT`.
pub const DEFAULT_LIMIT: u64 = 10;

/// Cap on the *initial* buffer allocation, to keep the up-front cost
/// bounded when `offset + count` is very large. The buffer/heap is still
/// allowed to grow past this — it only governs `with_capacity`, not the
/// number of rows we will retain.
const INITIAL_CAPACITY_CAP: usize = 16_384;

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
        /// Item = projected payload (hashed/compared by [`DistinctRow`]'s
        /// canonical encoding); priority = the sort-key comparator
        /// ([`EntryKey`], best = `Greater`).
        pq: DoublePriorityQueue<DistinctRow, EntryKey<D>>,
        sort_asc_map: u64,
        offset: usize,
        count: usize,
    },
}

impl<D: Ord> Storage<D> {
    /// Resolve `(offset, count)` and pre-size the buffer/heap.
    pub fn new(sortby: bool, limit: Option<(u64, u64)>, sort_asc_map: u64) -> Self {
        let (offset, count) = match (sortby, limit) {
            (_, Some((o, c))) => (o as usize, c as usize),
            // SAFETY: `ffi::RSGlobalConfig` is the module-global config
            // instance initialised once during module load; we only read
            // a single `usize` field here.
            (false, None) => (0, unsafe { ffi::RSGlobalConfig.maxAggregateResults }),
            (true, None) => (0, DEFAULT_LIMIT as usize),
        };
        let initial_capacity = offset.saturating_add(count).min(INITIAL_CAPACITY_CAP);
        if sortby {
            Self::Heap {
                heap: MinMaxHeap::with_capacity(initial_capacity),
                sort_asc_map,
                offset,
                count,
            }
        } else {
            Self::Array {
                buf: Vec::with_capacity(initial_capacity),
                offset,
                count,
                _marker: PhantomData,
            }
        }
    }

    /// Construct the DISTINCT variant. DISTINCT always implies `SORTBY`, so an
    /// absent `LIMIT` resolves to [`DEFAULT_LIMIT`] like the [`Self::Heap`]
    /// path. The caller is responsible for the `@__key` skip (selecting
    /// [`Self::new`] with `sortby = true` instead) when dedup is probably a
    /// no-op.
    pub fn new_distinct(limit: Option<(u64, u64)>, sort_asc_map: u64) -> Self {
        let (offset, count) = match limit {
            Some((o, c)) => (o as usize, c as usize),
            None => (0, DEFAULT_LIMIT as usize),
        };
        let initial_capacity = offset.saturating_add(count).min(INITIAL_CAPACITY_CAP);
        Self::DistinctHeap {
            pq: DoublePriorityQueue::with_capacity(initial_capacity),
            sort_asc_map,
            offset,
            count,
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
    /// Returns `true` if the entry was buffered, `false` if it was dropped.
    ///
    /// `doc_id` is only consulted on heap-backed paths, where it acts as a
    /// deterministic tie-breaker when sort keys compare equal. Callers
    /// that don't need tie-breaking instantiate `Storage<()>` and pass
    /// `()` here — `()`'s `Ord` impl makes the fallback a no-op.
    ///
    /// This is the non-DISTINCT entry point ([`Self::Array`] / [`Self::Heap`]);
    /// it delegates to [`Self::insert_entry_with_dedup`] with a no-op dedup
    /// closure. The DISTINCT path ([`Self::DistinctHeap`]) calls
    /// `insert_entry_with_dedup` directly.
    pub fn insert_entry<S, P>(&mut self, sort_vals: S, doc_id: D, project: P) -> bool
    where
        S: FnOnce() -> Box<[Option<SharedValue>]>,
        P: FnOnce() -> RLookupRow<'static>,
    {
        self.insert_entry_with_dedup(sort_vals, doc_id, project, |_row| Box::default())
    }

    /// Insert an entry under the cap, with a `dedup_from_row` closure used only
    /// by the [`Self::DistinctHeap`] path to derive each row's dedup identity
    /// (the canonical encoding of its *projected* fields — see the design doc
    /// §5.1.1). [`Self::Array`] and [`Self::Heap`] ignore it.
    ///
    /// `dedup_from_row` is invoked only for a candidate that survives the
    /// doomed-candidate short-circuit, so deferred projection is preserved.
    ///
    /// Returns `true` if the entry was buffered, `false` if it was dropped.
    pub fn insert_entry_with_dedup<S, P, K>(
        &mut self,
        sort_vals: S,
        doc_id: D,
        project: P,
        dedup_from_row: K,
    ) -> bool
    where
        S: FnOnce() -> Box<[Option<SharedValue>]>,
        P: FnOnce() -> RLookupRow<'static>,
        K: FnOnce(&RLookupRow<'static>) -> Box<[u8]>,
    {
        match self {
            Self::Array {
                buf, offset, count, ..
            } => {
                let max_size = offset.saturating_add(*count);
                if buf.len() < max_size {
                    buf.push(project());
                    true
                } else {
                    false
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
                    return false;
                }
                if heap.len() < max_size {
                    let key = make_key(sort_vals);
                    heap.push(HeapEntry::new(key, project()));
                    true
                } else {
                    let cand_key = make_key(sort_vals);
                    // `peek_min` returns the worst surviving candidate
                    // under the "best = max" convention (see `heap`).
                    // The unwrap is sound: `cap > 0` implies the heap is
                    // non-empty once we've reached the cap.
                    let worst = heap.peek_min().expect("heap at cap is non-empty");
                    if cand_key > *worst.key() {
                        heap.push_pop_min(HeapEntry::new(cand_key, project()));
                        true
                    } else {
                        false
                    }
                }
            }
            Self::DistinctHeap {
                pq,
                sort_asc_map,
                offset,
                count,
            } => {
                let max_size = offset.saturating_add(*count);
                if max_size == 0 {
                    return false;
                }
                let cand_key = EntryKey::new(sort_vals(), *sort_asc_map, doc_id);
                // Doomed short-circuit (§9.1 step 1): if the heap is full and
                // the candidate is no better than the current worst, drop it
                // without projecting or hashing. Safe even without a dedup
                // probe — any existing entry is `>= worst >= candidate`, so a
                // duplicate would be represented at least as well already.
                if pq.len() >= max_size {
                    let worst = pq.peek_min().expect("pq at cap is non-empty").1;
                    if cand_key <= *worst {
                        return false;
                    }
                }
                // Project now (dedup needs the candidate's projected fields)
                // and derive its dedup identity from the projected row.
                let row = project();
                let canon = dedup_from_row(&row);
                let item = DistinctRow::from_parts(row, canon);

                // Dedup-keep-best. `push_increase` would update only the
                // *priority* while retaining the first-seen item, leaving the
                // stored row stale — its SORTBY columns would still hold the
                // loser's values, which corrupts the shard→coordinator payload
                // (`is_internal`) where those columns are serialized. So on a
                // strictly-better duplicate we replace **both** the row and its
                // priority via remove + push.
                match pq
                    .get_priority(&item)
                    .map(|existing| cand_key.cmp(existing))
                {
                    Some(Ordering::Greater) => {
                        pq.remove(&item);
                        pq.push(item, cand_key);
                    }
                    // Worse-or-equal duplicate: keep the incumbent, drop the
                    // candidate (first/best wins on ties).
                    Some(_) => {}
                    // No duplicate: a genuinely new insert, which may exceed the
                    // cap and require evicting the current worst.
                    None => {
                        pq.push(item, cand_key);
                        if pq.len() > max_size {
                            pq.pop_min();
                        }
                    }
                }
                true
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
                mut pq,
                offset,
                count,
                ..
            } => {
                // Drain best→worst via repeated `pop_max` (best = max), the
                // DEPQ analogue of `MinMaxHeap::into_vec_desc`. Shares the
                // `Vec<RLookupRow>` iterator shape with the array path.
                let mut rows = Vec::with_capacity(pq.len());
                while let Some((entry, _key)) = pq.pop_max() {
                    rows.push(entry.into_row());
                }
                Either::Left(rows.into_iter().skip(offset).take(count))
            }
        }
    }
}
