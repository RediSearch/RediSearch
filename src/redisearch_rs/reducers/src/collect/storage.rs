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
//! - [`Storage::Array`] — used when `SORTBY` is *not* requested. Holds rows
//!   in arrival order under an `offset + count` cap. Excess inserts are
//!   dropped in O(1) without paying any projection cost.
//! - [`Storage::Heap`] — used when `SORTBY` is requested. Wraps the
//!   [`MinMaxHeap`]-backed primitive defined in [`super::heap`] and keeps
//!   the top-`(offset + count)` survivors under the comparator driven by
//!   `sort_asc_map`. Drains best→worst.
//!
//! The effective `(offset, count)` is resolved once by [`Storage::new`],
//! with defaults filling in for a missing `LIMIT`. The maximum number of
//! buffered rows is `offset + count`, enforced on each insert.
//!
//! [`MinMaxHeap`]: min_max_heap::MinMaxHeap

use std::cmp::Ordering;

use value::SharedValue;

use super::heap::{EntryHeap, EntryKey, HeapEntry};

/// Default count for `SORTBY` results when no explicit `LIMIT` is provided,
/// matching the C implementation's `DEFAULT_LIMIT`.
pub const DEFAULT_LIMIT: u64 = 10;

/// Cap on the *initial* buffer allocation.
const INITIAL_CAPACITY_CAP: usize = 16_384;

/// One drained row alongside an optional sort-key snapshot.
///
/// `sort_vals` is `Some` only on the heap path: the comparator owns the
/// snapshot anyway, so the consumer can recover SORTBY columns at finalize
/// without a second `row.get` lookup. The array path returns `None` and
/// expects the caller to read sort columns straight from `projected`.
pub struct DrainedItem<T> {
    pub projected: T,
    pub sort_vals: Option<Box<[SharedValue]>>,
}

pub enum Storage<T> {
    Array {
        buf: Vec<T>,
        offset: usize,
        count: usize,
    },
    Heap {
        heap: EntryHeap<T>,
        sort_asc_map: u64,
        offset: usize,
        count: usize,
    },
}

impl<T> Storage<T> {
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
        let cap = offset.saturating_add(count).min(INITIAL_CAPACITY_CAP);
        if sortby {
            Self::Heap {
                heap: EntryHeap::with_capacity(cap),
                sort_asc_map,
                offset,
                count,
            }
        } else {
            Self::Array {
                buf: Vec::with_capacity(cap),
                offset,
                count,
            }
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
    pub fn insert_entry<S, P>(&mut self, sort_vals: S, project: P) -> bool
    where
        S: FnOnce() -> Box<[SharedValue]>,
        P: FnOnce() -> T,
    {
        match self {
            Self::Array { buf, offset, count } => {
                if buf.len() < offset.saturating_add(*count) {
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
                let cap = offset.saturating_add(*count);
                if heap.len() < cap {
                    let key = EntryKey::new(sort_vals(), *sort_asc_map);
                    heap.push(HeapEntry::new(key, project()));
                    true
                } else {
                    let cand = EntryKey::new(sort_vals(), *sort_asc_map);
                    // `peek_min` returns the worst surviving candidate
                    // under the "best = max" convention (see `heap`).
                    // The unwrap is sound: `cap > 0` implies the heap is
                    // non-empty once we've reached the cap.
                    let worst = heap.peek_min().expect("heap at cap is non-empty");
                    if cand.cmp(worst.key()) == Ordering::Greater {
                        heap.push_pop_min(HeapEntry::new(cand, project()));
                        true
                    } else {
                        false
                    }
                }
            }
        }
    }

    /// Drain buffered rows.
    ///
    /// - **Array path** yields rows in insertion order.
    /// - **Heap path** yields rows best→worst (matching the SORTBY result
    ///   order), with each item carrying its sort-key snapshot so the
    ///   caller can re-emit SORTBY columns without a second lookup.
    ///
    /// When `apply_limit` is `true`, the yielded sequence is sliced as
    /// `skip(offset).take(count)`. When `false`, every buffered row is
    /// yielded — used by the remote reducer when `is_internal` is set,
    /// where the coordinator owns the global offset.
    pub fn drain(&mut self, apply_limit: bool) -> Drain<T> {
        // Sentinel left in place of `*self`. Empty Array, no allocation.
        let taken = std::mem::replace(
            self,
            Self::Array {
                buf: Vec::new(),
                offset: 0,
                count: 0,
            },
        );
        match taken {
            Self::Array { buf, offset, count } => {
                let (offset, count) = limit_window(apply_limit, offset, count);
                Drain::Array(buf.into_iter().skip(offset).take(count))
            }
            Self::Heap {
                heap,
                sort_asc_map: _,
                offset,
                count,
            } => {
                let (offset, count) = limit_window(apply_limit, offset, count);
                Drain::Heap(heap.into_vec_desc().into_iter().skip(offset).take(count))
            }
        }
    }
}

const fn limit_window(apply_limit: bool, offset: usize, count: usize) -> (usize, usize) {
    if apply_limit {
        (offset, count)
    } else {
        (0, usize::MAX)
    }
}

/// Drain iterator over a [`Storage<T>`]. Yields [`DrainedItem<T>`].
pub enum Drain<T> {
    Array(std::iter::Take<std::iter::Skip<std::vec::IntoIter<T>>>),
    Heap(std::iter::Take<std::iter::Skip<std::vec::IntoIter<HeapEntry<T>>>>),
}

impl<T> Iterator for Drain<T> {
    type Item = DrainedItem<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Array(it) => it.next().map(|projected| DrainedItem {
                projected,
                sort_vals: None,
            }),
            Self::Heap(it) => it.next().map(|entry| {
                let (sort_vals, projected) = entry.into_parts();
                DrainedItem {
                    projected,
                    sort_vals: Some(sort_vals),
                }
            }),
        }
    }
}

impl<T> ExactSizeIterator for Drain<T> {
    fn len(&self) -> usize {
        match self {
            Self::Array(it) => it.len(),
            Self::Heap(it) => it.len(),
        }
    }
}
