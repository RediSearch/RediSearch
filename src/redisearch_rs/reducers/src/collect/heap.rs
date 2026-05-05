/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Min-max heap building blocks for the bounded `COLLECT … SORTBY [LIMIT]`
//! top-K path.
//!
//! Wraps an `Ord`-driven [`MinMaxHeap`] over [`HeapEntry<T>`], where the
//! comparator only reads the [`EntryKey`] half. The split lets the heap
//! defer payload projection until a candidate's survival is confirmed.
//!
//! ## "Best = max" convention
//!
//! `cmp_fields` returns [`Ordering::Greater`] for the side that should appear
//! *first* in the result (matching `SearchResult_CmpByFields` and the C
//! `RPSorter`'s `mmh_pop_max` consumer). [`EntryKey`] delegates directly,
//! so under [`MinMaxHeap`]:
//!
//! - [`peek_max`][min_max_heap::MinMaxHeap::peek_max] returns the *best*
//!   surviving candidate.
//! - [`peek_min`][min_max_heap::MinMaxHeap::peek_min] returns the *worst*
//!   surviving candidate (the one to evict next).
//! - [`push_pop_min`][min_max_heap::MinMaxHeap::push_pop_min] is the
//!   bounded skip-or-replace primitive.
//! - [`drain_desc`][min_max_heap::MinMaxHeap::drain_desc] yields entries
//!   best→worst, matching the SORTBY result order.
//! - [`iter`][min_max_heap::MinMaxHeap::iter] gives an unsorted view
//!
//! ## Deferred projection
//!
//! [`EntryKey`] owns a *snapshot* of the sort-key values, severed from the
//! reused upstream [`RLookupRow`][rlookup::RLookupRow] buffer. The payload
//! `T` (e.g. an `RLookupRow<'a>` or a `Box<[SharedValue]>`) is materialised
//! by the caller only after the candidate has beaten the current worst —
//! see [`super::storage`].
//!

use std::cmp::Ordering;

use min_max_heap::MinMaxHeap;
use value::comparison::cmp_fields;
use value::{SharedValue, Value};

/// Sort-key snapshot plus the ASC/DESC bitmap, owning everything the
/// comparator is allowed to read.
///
/// Bit `i` of `sort_asc_map` (LSB-first) selects ASC for the `i`-th key
/// (set) or DESC (clear), matching `SORTASCMAP_GETASC` / `cmp_fields`.
pub struct EntryKey {
    sort_vals: Box<[SharedValue]>,
    sort_asc_map: u64,
}

impl EntryKey {
    pub const fn new(sort_vals: Box<[SharedValue]>, sort_asc_map: u64) -> Self {
        Self {
            sort_vals,
            sort_asc_map,
        }
    }

    /// Decompose into the underlying snapshot, releasing the comparator
    /// configuration (which lives once per heap, not once per entry).
    pub fn into_sort_vals(self) -> Box<[SharedValue]> {
        self.sort_vals
    }
}

impl PartialEq for EntryKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for EntryKey {}

impl PartialOrd for EntryKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EntryKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let pairs = self
            .sort_vals
            .iter()
            .zip(other.sort_vals.iter())
            .map(|(a, b)| (strip_null(a), strip_null(b)));
        // Direct delegation: `cmp_fields` already returns
        // `Ordering::Greater` for the "better" side, matching the
        // "best = max" convention in `SearchResult_CmpByFields` and the
        // C `RPSorter`'s `mmh_pop_max` consumer.
        cmp_fields(pairs, self.sort_asc_map, None)
    }
}

/// Heap slot: the comparator key alongside the projected payload.
///
/// `T` is the per-row payload the consumer (`Storage::Heap`) yields at
/// finalize. Comparing only reads `key`, so the choice of `T` does not
/// affect ranking.
pub struct HeapEntry<T> {
    key: EntryKey,
    projected: T,
}

impl<T> HeapEntry<T> {
    pub const fn new(key: EntryKey, projected: T) -> Self {
        Self { key, projected }
    }

    pub const fn key(&self) -> &EntryKey {
        &self.key
    }

    pub const fn projected(&self) -> &T {
        &self.projected
    }

    /// Decompose into `(sort_vals, projected)` — used by `Storage::Heap`'s
    /// finalize drain to surface SORTBY columns alongside the projection.
    pub fn into_parts(self) -> (Box<[SharedValue]>, T) {
        (self.key.into_sort_vals(), self.projected)
    }
}

impl<T> PartialEq for HeapEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<T> Eq for HeapEntry<T> {}

impl<T> PartialOrd for HeapEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for HeapEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// Map the stored `Value::Null` sentinel (used for missing sort keys) back
/// to `None` so [`cmp_fields`] applies its missing-worst policy instead
/// of `Value::Null`'s natural ordering.
fn strip_null(v: &SharedValue) -> Option<&Value> {
    match &**v {
        Value::Null => None,
        other => Some(other),
    }
}

/// Convenience type alias: a [`MinMaxHeap`] of [`HeapEntry<T>`].
///
/// The wrapper is a type alias rather than a newtype so callers retain
/// the full [`MinMaxHeap`] surface (`peek_min`/`peek_max`, `push_pop_min`,
/// `iter`, `drain_desc`, `with_capacity`).
pub type EntryHeap<T> = MinMaxHeap<HeapEntry<T>>;
