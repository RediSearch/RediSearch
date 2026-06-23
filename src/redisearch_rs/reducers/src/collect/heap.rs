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
//! [`HeapEntry<D, T>`], where the comparator only reads the [`RankingKey`] half. The
//! split lets the heap defer payload projection until a candidate's survival is
//! confirmed.
//!
//! ## Deferred projection
//!
//! [`RankingKey`] owns a *snapshot* of the sort-key values, severed from the
//! reused upstream [`RLookupRow`][rlookup::RLookupRow] buffer. The payload
//! `T` is materialised by the caller only after the candidate has beaten the
//! current worst — see [`super::storage`].
//!

use std::cmp::Ordering;
use value::SharedValue;
use value::comparison::cmp_fields;

/// Sort-key snapshot plus the ASC/DESC bitmap, owning everything the
/// comparator is allowed to read.
///
/// Bit `i` of `sort_asc_map` (LSB-first) selects ASC for the `i`-th key
/// (set) or DESC (clear), matching `SORTASCMAP_GETASC` / `cmp_fields`.
pub struct RankingKey<D: Ord> {
    sort_vals: Box<[Option<SharedValue>]>,
    sort_asc_map: u64,
    doc_id: D,
}

impl<D: Ord> RankingKey<D> {
    pub const fn new(sort_vals: Box<[Option<SharedValue>]>, sort_asc_map: u64, doc_id: D) -> Self {
        Self {
            sort_vals,
            sort_asc_map,
            doc_id,
        }
    }

    /// The sort-key snapshot, in `SORTBY` order (an absent key is `None`).
    pub fn sort_vals(&self) -> &[Option<SharedValue>] {
        &self.sort_vals
    }
}

impl<D: Ord> PartialEq for RankingKey<D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<D: Ord> Eq for RankingKey<D> {}

impl<D: Ord> PartialOrd for RankingKey<D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<D: Ord> Ord for RankingKey<D> {
    fn cmp(&self, other: &Self) -> Ordering {
        debug_assert_eq!(
            self.sort_vals.len(),
            other.sort_vals.len(),
            "RankingKey sort_vals length mismatch"
        );
        let pairs = self
            .sort_vals
            .iter()
            .zip(other.sort_vals.iter())
            .map(|(a, b)| (a.as_deref(), b.as_deref()));
        // Direct delegation: `cmp_fields` already returns
        // `Ordering::Greater` for the "better" side, matching the
        // "best = max" convention in `SearchResult_CmpByFields` and the
        // C `RPSorter`'s `mmh_pop_max` consumer.
        //
        // On a tie, break by doc id. Reversing the comparison makes a
        // smaller doc id "stronger" (greater), so the heap prefers it.
        cmp_fields(pairs, self.sort_asc_map, None)
            .then_with(|| self.doc_id.cmp(&other.doc_id).reverse())
    }
}

/// Heap slot: the comparator key alongside the projected payload.
///
/// `T` is the per-row payload the consumer
/// ([`HeapStorage`][super::storage::HeapStorage]) yields at finalize. Comparing
/// only reads `key`, so the choice of `T` does not affect ranking.
pub struct HeapEntry<D: Ord, T> {
    key: RankingKey<D>,
    projected: T,
}

impl<D: Ord, T> HeapEntry<D, T> {
    pub const fn new(key: RankingKey<D>, projected: T) -> Self {
        Self { key, projected }
    }

    pub const fn key(&self) -> &RankingKey<D> {
        &self.key
    }

    pub const fn projected(&self) -> &T {
        &self.projected
    }

    /// Return the projected payload.
    pub fn into_projected(self) -> T {
        self.projected
    }

    /// Split into the ranking key and the projected payload.
    ///
    /// Unlike [`Self::into_projected`], this keeps the [`RankingKey`] so a
    /// consumer can read its [`RankingKey::sort_vals`] — needed by the remote
    /// reducer's deferred sort-key merge.
    pub fn into_parts(self) -> (RankingKey<D>, T) {
        (self.key, self.projected)
    }
}

impl<D: Ord, T> PartialEq for HeapEntry<D, T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<D: Ord, T> Eq for HeapEntry<D, T> {}

impl<D: Ord, T> PartialOrd for HeapEntry<D, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<D: Ord, T> Ord for HeapEntry<D, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
