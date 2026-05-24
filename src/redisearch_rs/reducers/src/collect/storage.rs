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

use std::cmp::Ordering;

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

pub enum Storage {
    Array {
        buf: Vec<RLookupRow<'static>>,
        offset: usize,
        count: usize,
    },
    Heap {
        heap: MinMaxHeap<HeapEntry<RLookupRow<'static>>>,
        sort_asc_map: u64,
        offset: usize,
        count: usize,
    },
}

impl Storage {
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
            }
        }
    }

    /// Insert an entry, honouring the `offset + count` cap.
    ///
    /// `project` is called only when the entry will be retained.
    /// `sort_view` is [`Fn`] (not [`FnOnce`]) because it may be called twice
    /// on a winner — once to borrow-compare via [`EntryKey::cmp_candidate`],
    /// once to materialise the owned snapshot. Callers must ensure it is an
    /// idempotent projection over a stable borrowed source.
    ///
    /// Returns `true` if the entry was buffered, `false` if it was dropped.
    pub fn insert_entry<'r, F, I, P>(&mut self, sort_view: F, project: P) -> bool
    where
        F: Fn() -> I,
        I: IntoIterator<Item = Option<&'r SharedValue>>,
        P: FnOnce() -> RLookupRow<'static>,
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
                if max_size == 0 {
                    return false;
                }
                if heap.len() < max_size {
                    // Below cap: no worst to beat — materialize directly.
                    let owned: Box<[Option<SharedValue>]> =
                        sort_view().into_iter().map(|v| v.cloned()).collect();
                    let key = EntryKey::new(owned, *sort_asc_map);
                    heap.push(HeapEntry::new(key, project()));
                    true
                } else {
                    // At cap: borrow-compare first; materialize only on win.
                    let worst = heap.peek_min().expect("heap at cap is non-empty");
                    if worst.key().cmp_candidate(sort_view()) == Ordering::Greater {
                        let owned: Box<[Option<SharedValue>]> =
                            sort_view().into_iter().map(|v| v.cloned()).collect();
                        let key = EntryKey::new(owned, *sort_asc_map);
                        heap.push_pop_min(HeapEntry::new(key, project()));
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
    ///   order).
    ///
    /// When `apply_limit` is `true`, the yielded sequence is sliced as
    /// `skip(offset).take(count)`. When `false`, every buffered row is
    /// yielded — used by the remote reducer when `is_internal` is set,
    /// where the coordinator owns the global offset.
    pub fn drain(
        &mut self,
        apply_limit: bool,
    ) -> impl ExactSizeIterator<Item = RLookupRow<'static>> {
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
            Self::Array {
                buf, offset, count, ..
            } => {
                let (offset, count) = limit_window(apply_limit, offset, count);
                Either::Left(buf.into_iter().skip(offset).take(count))
            }
            Self::Heap {
                heap,
                offset,
                count,
                ..
            } => {
                let (offset, count) = limit_window(apply_limit, offset, count);
                Either::Right(
                    heap.into_vec_desc()
                        .into_iter()
                        .skip(offset)
                        .take(count)
                        .map(HeapEntry::into_projected),
                )
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
