/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Ranking primitives for the bounded `COLLECT … SORTBY [LIMIT]` top-K path:
//! [`RankingKey`] is the comparator; [`RankedEntry`] is the ranked storage's
//! element, pairing a key with its payload.
//!
//! ## Deferred projection
//!
//! [`RankingKey`] owns a *snapshot* of the sort-key values, severed from the
//! reused upstream [`RLookupRow`][rlookup::RLookupRow] buffer. On the
//! non-DISTINCT path the payload `T` is materialised only after the candidate
//! beats the current worst — see [`super::storage`].
//!

use std::cmp::Ordering;
use value::comparison::cmp_fields;
use value::{SharedValue, Value};

/// Shared by [`RankingKey`]'s `Ord` and the borrowed [`RankingKey::ranks_below`]
/// so the two can't diverge.
///
/// `cmp_fields` returns [`Ordering::Greater`] for the better side ("best = max",
/// as in C's `SearchResult_CmpByFields` / `RPSorter`); ties break on a smaller
/// doc id ranking higher, hence the reversed doc-id compare.
fn cmp_ranking<'a, 'b, D: Ord>(
    a_vals: impl IntoIterator<Item = Option<&'a Value>>,
    a_doc: &D,
    b_vals: impl IntoIterator<Item = Option<&'b Value>>,
    b_doc: &D,
    sort_asc_map: u64,
) -> Ordering {
    let pairs = a_vals.into_iter().zip(b_vals);
    cmp_fields(pairs, sort_asc_map, None).then_with(|| a_doc.cmp(b_doc).reverse())
}

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

    /// Consumes the key, yielding its sort-key snapshot in `SORTBY` order (an
    /// absent key is `None`).
    pub fn into_sort_vals(self) -> Box<[Option<SharedValue>]> {
        self.sort_vals
    }

    /// Whether `self` (a survivor) ranks strictly below `cand_sort_vals` — i.e.
    /// the candidate beats it and should evict it. Matches the [`Ord`] impl
    /// (both via [`cmp_ranking`]) but reads the candidate by borrow, so a loser
    /// is rejected without materialising its owned key.
    pub fn ranks_below<'a>(
        &self,
        cand_sort_vals: impl IntoIterator<Item = Option<&'a SharedValue>>,
        cand_doc_id: &D,
    ) -> bool {
        cmp_ranking(
            self.sort_vals.iter().map(Option::as_deref),
            &self.doc_id,
            cand_sort_vals.into_iter().map(|v| v.map(|sv| &**sv)),
            cand_doc_id,
            self.sort_asc_map,
        ) == Ordering::Less
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
        cmp_ranking(
            self.sort_vals.iter().map(Option::as_deref),
            &self.doc_id,
            other.sort_vals.iter().map(Option::as_deref),
            &other.doc_id,
            self.sort_asc_map,
        )
    }
}

/// Ordering and equality read only `key`, so the payload `T` never affects
/// ranking.
pub struct RankedEntry<K, T> {
    key: K,
    projected: T,
}

impl<K, T> RankedEntry<K, T> {
    pub const fn new(key: K, projected: T) -> Self {
        Self { key, projected }
    }

    pub const fn key(&self) -> &K {
        &self.key
    }

    pub const fn projected(&self) -> &T {
        &self.projected
    }

    pub fn into_projected(self) -> T {
        self.projected
    }

    pub fn into_parts(self) -> (K, T) {
        (self.key, self.projected)
    }
}

impl<K: PartialEq, T> PartialEq for RankedEntry<K, T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K: Eq, T> Eq for RankedEntry<K, T> {}

impl<K: Ord, T> PartialOrd for RankedEntry<K, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord, T> Ord for RankedEntry<K, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
