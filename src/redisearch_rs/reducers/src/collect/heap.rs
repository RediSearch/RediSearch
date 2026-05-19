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
//! Wraps an `Ord`-driven [`MinMaxHeap`][min_max_heap::MinMaxHeap] over
//! [`HeapEntry<T>`], where the comparator only reads the [`EntryKey`] half. The
//! split lets the heap defer payload projection until a candidate's survival is
//! confirmed.
//!
//! ## Deferred projection
//!
//! [`EntryKey`] owns a *snapshot* of the sort-key values, severed from the
//! reused upstream [`RLookupRow`][rlookup::RLookupRow] buffer. The payload
//! `T` is materialised by the caller only after the candidate has beaten the
//! current worst — see [`super::storage`].
//!

use ffi::t_docId;
use std::cmp::Ordering;
use value::SharedValue;
use value::comparison::cmp_fields;

/// Sort-key snapshot plus the ASC/DESC bitmap, owning everything the
/// comparator is allowed to read.
///
/// Bit `i` of `sort_asc_map` (LSB-first) selects ASC for the `i`-th key
/// (set) or DESC (clear), matching `SORTASCMAP_GETASC` / `cmp_fields`.
///
/// `doc_id` is the secondary key used when all `sort_vals` compare equal,
/// mirroring `SearchResult_CmpByFields`. On shards it is the upstream
/// document id read out of the hidden `__docid` slot that the C grouper
/// plants on every row; on the coordinator it is a synthetic arrival
/// counter (see `LocalCollectCtx`).
pub struct EntryKey {
    sort_vals: Box<[Option<SharedValue>]>,
    sort_asc_map: u64,
    doc_id: t_docId,
}

impl EntryKey {
    pub const fn new(
        sort_vals: Box<[Option<SharedValue>]>,
        sort_asc_map: u64,
        doc_id: t_docId,
    ) -> Self {
        Self {
            sort_vals,
            sort_asc_map,
            doc_id,
        }
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
        debug_assert_eq!(
            self.sort_vals.len(),
            other.sort_vals.len(),
            "EntryKey sort_vals length mismatch"
        );
        let pairs = self
            .sort_vals
            .iter()
            .zip(other.sort_vals.iter())
            .map(|(a, b)| (a.as_deref(), b.as_deref()));
        // `cmp_fields` returns `Ordering::Greater` for the "better" side
        // (matching the "best = max" convention in `SearchResult_CmpByFields`
        // and the C `RPSorter`'s `mmh_pop_max` consumer).
        match cmp_fields(pairs, self.sort_asc_map, None) {
            Ordering::Equal => {
                // Tiebreak by docid — ascending uses the last key's flag,
                // matching the original C loop where `ascending` retains its
                // last value. Identical to `SearchResult_CmpByFields`
                let nkeys = self.sort_vals.len();
                let ascending = nkeys > 0 && (self.sort_asc_map & (1u64 << (nkeys - 1))) != 0;
                let raw = self.doc_id.cmp(&other.doc_id);
                if ascending { raw.reverse() } else { raw }
            }
            ord => ord,
        }
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

    /// Return the projected payload.
    pub fn into_projected(self) -> T {
        self.projected
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
